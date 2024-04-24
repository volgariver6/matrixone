// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	getAllAccountInfoFormat = "select " +
		"account_id as `account_id`, " +
		"account_name as `account_name`, " +
		"created_time as `created`, " +
		"status as `status`, " +
		"suspended_time as `suspended_time`, " +
		"comments as `comment` " +
		"from " +
		"mo_catalog.mo_account " +
		"%s" +
		";"

	getAccountInfoFormat = "select " +
		"account_id as `account_id`, " +
		"account_name as `account_name`, " +
		"created_time as `created`, " +
		"status as `status`, " +
		"suspended_time as `suspended_time`, " +
		"comments as `comment` " +
		"from " +
		"mo_catalog.mo_account " +
		"where account_id = %d;"

	// column index in the result set generated by
	// the sql getAllAccountInfoFormat, getAccountInfoFormat
	idxOfAccountId     = 0
	idxOfAccountName   = 1
	idxOfCreated       = 2
	idxOfStatus        = 3
	idxOfSuspendedTime = 4
	idxOfComment       = 5

	// left the `size` as a placeholder, the value will be embedding later
	// index table(__index_xxx): do not counting it into the `table_count`, but need its size.
	// mo_increment_columns: do not counting it into the `table_count`, but need its size.
	// subscription db: do not counting it into the `db_count`, and its size
	// sys table(mo_database, mo_tables, mo_column): counting them into the `table_count`, and need their sizes
	getTableStatsFormatV2 = "select " +
		"( select " +
		"        mu2.user_name as `admin_name` " +
		"  from mo_catalog.mo_user as mu2 join " +
		"      ( select " +
		"              min(user_id) as `min_user_id` " +
		"        from mo_catalog.mo_user " +
		"      ) as mu1 on mu2.user_id = mu1.min_user_id " +
		") as `admin_name`, " +
		"count(distinct md.datname) as `db_count`, " +
		"count(distinct mt.relname) as `table_count`, " +
		"cast(0 as double) as `size` " +
		"from " +
		"mo_catalog.mo_tables as mt, mo_catalog.mo_database as md " +
		"where md.dat_type != 'subscription' " +
		"and mt.relkind in ('v','r','e','cluster') " +
		"and mt.account_id in (%d, %d);"

	// column index in the result set generated by
	// the sql getTableStatsFormatV2
	idxOfAdminName  = 0
	idxOfDBCount    = 1
	idxOfTableCount = 2
	idxOfSize       = 3

	// column index in the result set of the statement show accounts
	finalIdxOfAccountName   = 0
	finalIdxOfAdminName     = 1
	finalIdxOfCreated       = 2
	finalIdxOfStatus        = 3
	finalIdxOfSuspendedTime = 4
	finalIdxOfDBCount       = 5
	finalIdxOfTableCount    = 6
	finalIdxOfSize          = 7
	finalIdxOfComment       = 8
	finalColumnCount        = 9
)

var cnUsageCache = logtail.NewStorageUsageCache(
	logtail.WithLazyThreshold(5))

func getSqlForAllAccountInfo(like *tree.ComparisonExpr) string {
	var likePattern = ""
	if like != nil {
		likePattern = strings.TrimSpace(like.Right.String())
	}
	likeClause := ""
	if len(likePattern) != 0 {
		likeClause = fmt.Sprintf("where account_name like '%s'", likePattern)
	}
	return fmt.Sprintf(getAllAccountInfoFormat, likeClause)
}

func getSqlForAccountInfo(accountId uint64) string {
	return fmt.Sprintf(getAccountInfoFormat, accountId)
}

func getSqlForTableStats(accountId int32) string {
	//return fmt.Sprintf(getTableStatsFormatV2, catalog.SystemPartitionRel, accountId)
	return fmt.Sprintf(getTableStatsFormatV2, accountId, sysAccountID)
}

func requestStorageUsage(ses *Session, accIds [][]int32) (resp any, tried bool, err error) {
	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payload := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		req := db.StorageUsageReq{}
		for x := range accIds {
			req.AccIds = append(req.AccIds, accIds[x]...)
		}

		return req.Marshal()
	}

	responseUnmarshaler := func(payload []byte) (any, error) {
		usage := &db.StorageUsageResp{}
		if err := usage.Unmarshal(payload); err != nil {
			return nil, err
		}
		return usage, nil
	}

	var ctx context.Context
	var txnOperator client.TxnOperator
	if ctx, txnOperator, err = ses.txnHandler.GetTxn(); err != nil {
		return nil, false, err
	}

	enterFPrint(ses, 8)
	defer exitFPrint(ses, 8)

	// create a new proc for `handler`
	proc := process.New(ctx, ses.proc.GetMPool(),
		ses.proc.TxnClient, txnOperator,
		ses.proc.FileService, ses.proc.LockService,
		ses.proc.QueryService, ses.proc.Hakeeper,
		ses.proc.UdfService, ses.proc.Aicm,
	)

	handler := ctl.GetTNHandlerFunc(api.OpCode_OpStorageUsage, whichTN, payload, responseUnmarshaler)
	result, err := handler(proc, "DN", "", ctl.MoCtlTNCmdSender)
	if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
		// try the previous RPC method
		payload_V0 := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) { return nil, nil }
		responseUnmarshaler_V0 := func(payload []byte) (interface{}, error) {
			usage := &db.StorageUsageResp_V0{}
			if err := usage.Unmarshal(payload); err != nil {
				return nil, err
			}
			return usage, nil
		}

		tried = true
		CmdMethod_StorageUsage := api.OpCode(14)
		handler = ctl.GetTNHandlerFunc(CmdMethod_StorageUsage, whichTN, payload_V0, responseUnmarshaler_V0)
		result, err = handler(proc, "DN", "", ctl.MoCtlTNCmdSender)

		if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
			return nil, tried, moerr.NewNotSupportedNoCtx("current tn version not supported `show accounts`")
		}
	}

	if err != nil {
		return nil, tried, err
	}

	return result.Data.([]any)[0], tried, nil
}

func handleStorageUsageResponse_V0(ctx context.Context, fs fileservice.FileService,
	usage *db.StorageUsageResp_V0) (map[int32]uint64, error) {
	result := make(map[int32]uint64, 0)
	for idx := range usage.CkpEntries {
		version := usage.CkpEntries[idx].Version
		location := usage.CkpEntries[idx].Location

		// storage usage was introduced after `CheckpointVersion9`
		if version < logtail.CheckpointVersion9 {
			// exist old version checkpoint which hasn't storage usage data in it,
			// to avoid inaccurate info leading misunderstand, we chose to return empty result
			logutil.Info("[storage usage]: found older ckp when handle storage usage response")
			return map[int32]uint64{}, nil
		}

		ckpData, err := logtail.LoadSpecifiedCkpBatch(ctx, location, version, logtail.StorageUsageInsIDX, fs)
		if err != nil {
			return nil, err
		}

		storageUsageBat := ckpData.GetBatches()[logtail.StorageUsageInsIDX]
		accIDVec := vector.MustFixedCol[uint64](
			storageUsageBat.GetVectorByName(catalog.SystemColAttr_AccID).GetDownstreamVector(),
		)
		sizeVec := vector.MustFixedCol[uint64](
			storageUsageBat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize).GetDownstreamVector(),
		)

		size := uint64(0)
		length := len(accIDVec)
		for i := 0; i < length; i++ {
			result[int32(accIDVec[i])] += sizeVec[i]
			size += sizeVec[i]
		}

		ckpData.Close()
	}

	// [account_id, db_id, table_id, obj_id, table_total_size]
	for _, info := range usage.BlockEntries {
		result[int32(info.Info[0])] += info.Info[3]
	}

	return result, nil
}

func handleStorageUsageResponse(
	ctx context.Context,
	usage *db.StorageUsageResp,
) (map[int32]uint64, error) {
	result := make(map[int32]uint64, 0)

	for x := range usage.AccIds {
		result[usage.AccIds[x]] += usage.Sizes[x]
	}

	return result, nil
}

func checkStorageUsageCache(accIds [][]int32) (result map[int32]uint64, succeed bool) {
	cnUsageCache.Lock()
	defer cnUsageCache.Unlock()

	if cnUsageCache.IsExpired() {
		return nil, false
	}

	result = make(map[int32]uint64)
	for x := range accIds {
		for y := range accIds[x] {
			size, exist := cnUsageCache.GatherAccountSize(uint64(accIds[x][y]))
			if !exist {
				// one missed, update all
				return nil, false
			}

			result[accIds[x][y]] = size
		}
	}

	return result, true
}

func updateStorageUsageCache(accIds []int32, sizes []uint64) {

	if len(accIds) == 0 {
		return
	}

	cnUsageCache.Lock()
	defer cnUsageCache.Unlock()

	// step 1: delete stale accounts
	cnUsageCache.ClearForUpdate()

	// step 2: update
	for x := range accIds {
		usage := logtail.UsageData{AccId: uint64(accIds[x]), Size: sizes[x]}
		if old, exist := cnUsageCache.Get(usage); exist {
			usage.Size += old.Size
		}

		cnUsageCache.SetOrReplace(usage)
	}
}

// getAccountStorageUsage calculates the storage usage of all accounts
// by handling checkpoint
func getAccountsStorageUsage(ctx context.Context, ses *Session, accIds [][]int32) (map[int32]uint64, error) {
	if len(accIds) == 0 {
		return nil, nil
	}

	// step 1: check cache
	if usage, succeed := checkStorageUsageCache(accIds); succeed {
		return usage, nil
	}

	// step 2: query to tn
	response, tried, err := requestStorageUsage(ses, accIds)
	if err != nil {
		return nil, err
	}

	if tried {
		usage, ok := response.(*db.StorageUsageResp_V0)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx("storage usage response decode failed, retry later")
		}

		fs, err := fileservice.Get[fileservice.FileService](ses.GetParameterUnit().FileService, defines.SharedFileServiceName)
		if err != nil {
			return nil, err
		}

		// step 3: handling these pulled data
		return handleStorageUsageResponse_V0(ctx, fs, usage)

	} else {
		usage, ok := response.(*db.StorageUsageResp)
		if !ok || usage.Magic != logtail.StorageUsageMagic {
			return nil, moerr.NewInternalErrorNoCtx("storage usage response decode failed, retry later")
		}

		updateStorageUsageCache(usage.AccIds, usage.Sizes)

		// step 3: handling these pulled data
		return handleStorageUsageResponse(ctx, usage)
	}
}

func embeddingSizeToBatch(ori *batch.Batch, size uint64, mp *mpool.MPool) {
	vector.SetFixedAt(ori.Vecs[idxOfSize], 0, math.Round(float64(size)/1048576.0*1e6)/1e6)
}

func doShowAccounts(ctx context.Context, ses *Session, sa *tree.ShowAccounts) (err error) {
	var sql string
	var accountIds [][]int32
	var allAccountInfo []*batch.Batch
	var eachAccountInfo []*batch.Batch
	var tempBatch *batch.Batch
	var MoAccountColumns, EachAccountColumns *plan.ResultColDef
	var outputBatches []*batch.Batch

	start := time.Now()
	getTableStatsDur := time.Duration(0)
	defer func() {
		v2.TaskShowAccountsTotalDurationHistogram.Observe(time.Since(start).Seconds())
		v2.TaskShowAccountsGetTableStatsDurationHistogram.Observe(getTableStatsDur.Seconds())

	}()

	mp := ses.GetMemPool()

	defer func() {
		for _, b := range allAccountInfo {
			if b == nil {
				continue
			}
			b.Clean(mp)
		}
		for _, b := range outputBatches {
			if b == nil {
				continue
			}
			b.Clean(mp)
		}
		for _, b := range eachAccountInfo {
			if b == nil {
				continue
			}
			b.Clean(mp)
		}
		if tempBatch != nil {
			tempBatch.Clean(mp)
		}
	}()

	bh := ses.GetRawBatchBackgroundExec(ctx)
	defer bh.Close()

	account := ses.GetTenantInfo()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	if err != nil {
		return err
	}

	var rsOfMoAccount *MysqlResultSet

	// step 1:
	// 	get all accounts info according the type of the requested tenant

	// system account
	if account.IsSysTenant() {
		sql = getSqlForAllAccountInfo(sa.Like)
		if allAccountInfo, accountIds, err = getAccountInfo(ctx, bh, sql, true); err != nil {
			return err
		}

		// normal account
	} else {
		if sa.Like != nil {
			return moerr.NewInternalError(ctx, "only sys account can use LIKE clause")
		}
		// switch to the sys account to get account info
		newCtx := defines.AttachAccountId(ctx, uint32(sysAccountID))
		sql = getSqlForAccountInfo(uint64(account.GetTenantID()))
		if allAccountInfo, accountIds, err = getAccountInfo(newCtx, bh, sql, true); err != nil {
			return err
		}

		if len(allAccountInfo) != 1 {
			return moerr.NewInternalError(ctx, "no such account %v", account.TenantID)
		}
	}

	rsOfMoAccount = bh.ses.GetAllMysqlResultSet()[0]
	MoAccountColumns = bh.ses.rs
	bh.ClearExecResultSet()

	// step 2
	// calculating the storage usage size of accounts
	// the returned value is a map: account_id -> size (in bytes)
	tt := time.Now()
	usage, err := getAccountsStorageUsage(ctx, ses, accountIds)
	if err != nil {
		return err
	}
	v2.TaskShowAccountsGetUsageDurationHistogram.Observe(time.Since(tt).Seconds())

	// step 3
	outputBatches = make([]*batch.Batch, len(allAccountInfo))
	for i, ids := range accountIds {
		for _, id := range ids {
			//step 3.1: get the admin_name, db_count, table_count for each account
			newCtx := defines.AttachAccountId(ctx, uint32(id))

			tt = time.Now()
			if tempBatch, err = getTableStats(newCtx, bh, id); err != nil {
				return err
			}
			getTableStatsDur += time.Since(tt)

			// step 3.2: put size value into batch
			embeddingSizeToBatch(tempBatch, usage[id], mp)

			eachAccountInfo = append(eachAccountInfo, tempBatch)
		}

		// merge result set from mo_account and table stats from each account
		outputBatches[i] = batch.NewWithSize(finalColumnCount)
		if err = mergeOutputResult(ses, outputBatches[i], allAccountInfo[i], eachAccountInfo); err != nil {
			return err
		}

		for _, b := range eachAccountInfo {
			b.Clean(mp)
		}
		eachAccountInfo = nil
	}

	rsOfEachAccount := bh.ses.GetAllMysqlResultSet()[0]
	EachAccountColumns = bh.ses.rs
	bh.ClearExecResultSet()

	//step4: generate mysql result set
	outputRS := &MysqlResultSet{}
	if err = initOutputRs(outputRS, rsOfMoAccount, rsOfEachAccount, ctx); err != nil {
		return err
	}

	oq := newFakeOutputQueue(outputRS)
	for _, b := range outputBatches {
		if err = fillResultSet(oq, b, ses); err != nil {
			return err
		}
	}

	ses.SetMysqlResultSet(outputRS)

	ses.rs = mergeRsColumns(MoAccountColumns, EachAccountColumns)
	if openSaveQueryResult(ses) {
		err = saveResult(ses, outputBatches)
	}

	return err
}

func mergeRsColumns(rsOfMoAccountColumns *plan.ResultColDef, rsOfEachAccountColumns *plan.ResultColDef) *plan.ResultColDef {
	def := &plan.ResultColDef{
		ResultCols: make([]*plan.ColDef, finalColumnCount),
	}
	def.ResultCols[finalIdxOfAccountName] = rsOfMoAccountColumns.ResultCols[idxOfAccountName]
	def.ResultCols[finalIdxOfAdminName] = rsOfEachAccountColumns.ResultCols[idxOfAdminName]
	def.ResultCols[finalIdxOfCreated] = rsOfMoAccountColumns.ResultCols[idxOfCreated]
	def.ResultCols[finalIdxOfStatus] = rsOfMoAccountColumns.ResultCols[idxOfStatus]
	def.ResultCols[finalIdxOfSuspendedTime] = rsOfMoAccountColumns.ResultCols[idxOfSuspendedTime]
	def.ResultCols[finalIdxOfDBCount] = rsOfEachAccountColumns.ResultCols[idxOfDBCount]
	def.ResultCols[finalIdxOfTableCount] = rsOfEachAccountColumns.ResultCols[idxOfTableCount]
	def.ResultCols[finalIdxOfSize] = rsOfEachAccountColumns.ResultCols[idxOfSize]
	def.ResultCols[finalIdxOfComment] = rsOfMoAccountColumns.ResultCols[idxOfComment]
	return def
}

func saveResult(ses *Session, outputBatch []*batch.Batch) error {
	for _, b := range outputBatch {
		if err := saveQueryResult(ses, b); err != nil {
			return err
		}
	}
	if err := saveQueryResultMeta(ses); err != nil {
		return err
	}
	return nil
}

func initOutputRs(rs *MysqlResultSet, rsOfMoAccount *MysqlResultSet, rsOfEachAccount *MysqlResultSet, ctx context.Context) error {
	outputColumns := make([]Column, finalColumnCount)
	var err error
	outputColumns[finalIdxOfAccountName], err = rsOfMoAccount.GetColumn(ctx, idxOfAccountName)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfAdminName], err = rsOfEachAccount.GetColumn(ctx, idxOfAdminName)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfCreated], err = rsOfMoAccount.GetColumn(ctx, idxOfCreated)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfStatus], err = rsOfMoAccount.GetColumn(ctx, idxOfStatus)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfSuspendedTime], err = rsOfMoAccount.GetColumn(ctx, idxOfSuspendedTime)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfDBCount], err = rsOfEachAccount.GetColumn(ctx, idxOfDBCount)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfTableCount], err = rsOfEachAccount.GetColumn(ctx, idxOfTableCount)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfSize], err = rsOfEachAccount.GetColumn(ctx, idxOfSize)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfComment], err = rsOfMoAccount.GetColumn(ctx, idxOfComment)
	if err != nil {
		return err
	}
	for _, o := range outputColumns {
		rs.AddColumn(o)
	}
	return nil
}

// getAccountInfo gets account info from mo_account under sys account
func getAccountInfo(ctx context.Context,
	bh *BackgroundHandler,
	sql string,
	returnAccountIds bool) ([]*batch.Batch, [][]int32, error) {
	var err error
	var batchIndex2AccounsIds [][]int32
	var rsOfMoAccount []*batch.Batch

	bh.ClearExecResultBatches()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	rsOfMoAccount = bh.GetExecResultBatches()
	if len(rsOfMoAccount) == 0 {
		return nil, nil, moerr.NewInternalError(ctx, "no account info")
	}
	if returnAccountIds {
		batchCount := len(rsOfMoAccount)
		batchIndex2AccounsIds = make([][]int32, batchCount)
		for i := 0; i < batchCount; i++ {
			vecLen := rsOfMoAccount[i].Vecs[0].Length()
			for row := 0; row < vecLen; row++ {
				batchIndex2AccounsIds[i] = append(batchIndex2AccounsIds[i], vector.GetFixedAt[int32](rsOfMoAccount[i].Vecs[0], row))
			}
		}
	}
	return rsOfMoAccount, batchIndex2AccounsIds, err
}

// getTableStats gets the table statistics for the account
func getTableStats(ctx context.Context, bh *BackgroundHandler, accountId int32) (*batch.Batch, error) {
	var sql string
	var err error
	var rs []*batch.Batch
	sql = getSqlForTableStats(accountId)
	bh.ClearExecResultBatches()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}
	rs = bh.GetExecResultBatches()
	if len(rs) != 1 {
		return nil, moerr.NewInternalError(ctx, "get table stats failed")
	}
	return rs[0], err
}

// mergeOutputResult merges the result set from mo_account and the table status
// into the final output format
func mergeOutputResult(ses *Session, outputBatch *batch.Batch, rsOfMoAccount *batch.Batch, rsOfEachAccount []*batch.Batch) error {
	var err error
	mp := ses.GetMemPool()
	outputBatch.Vecs[finalIdxOfAccountName], err = rsOfMoAccount.Vecs[idxOfAccountName].Dup(mp)
	if err != nil {
		return err
	}
	outputBatch.Vecs[finalIdxOfAdminName] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfAdminName].GetType())
	outputBatch.Vecs[finalIdxOfCreated], err = rsOfMoAccount.Vecs[idxOfCreated].Dup(mp)
	if err != nil {
		return err
	}
	outputBatch.Vecs[finalIdxOfStatus], err = rsOfMoAccount.Vecs[idxOfStatus].Dup(mp)
	if err != nil {
		return err
	}
	outputBatch.Vecs[finalIdxOfSuspendedTime], err = rsOfMoAccount.Vecs[idxOfSuspendedTime].Dup(mp)
	if err != nil {
		return err
	}
	outputBatch.Vecs[finalIdxOfDBCount] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfDBCount].GetType())
	outputBatch.Vecs[finalIdxOfTableCount] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfTableCount].GetType())
	outputBatch.Vecs[finalIdxOfSize] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfSize].GetType())
	outputBatch.Vecs[finalIdxOfComment], err = rsOfMoAccount.Vecs[idxOfComment].Dup(mp)
	if err != nil {
		return err
	}

	for _, bat := range rsOfEachAccount {
		err = outputBatch.Vecs[finalIdxOfAdminName].UnionOne(bat.Vecs[idxOfAdminName], 0, mp)
		if err != nil {
			return err
		}
		err = outputBatch.Vecs[finalIdxOfDBCount].UnionOne(bat.Vecs[idxOfDBCount], 0, mp)
		if err != nil {
			return err
		}
		err = outputBatch.Vecs[finalIdxOfTableCount].UnionOne(bat.Vecs[idxOfTableCount], 0, mp)
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		err = outputBatch.Vecs[finalIdxOfSize].UnionOne(bat.Vecs[idxOfSize], 0, mp)
		if err != nil {
			return err
		}
	}
	outputBatch.SetRowCount(rsOfMoAccount.RowCount())
	return nil
}
