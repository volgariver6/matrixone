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

package catalog

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"go.uber.org/zap"

	// "time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+
const (
	SnapshotAttr_TID            = "table_id"
	SnapshotAttr_DBID           = "db_id"
	ObjectAttr_ID               = "id"
	ObjectAttr_CreateAt         = "create_at"
	ObjectAttr_SegNode          = "seg_node"
	SnapshotAttr_BlockMaxRow    = "block_max_row"
	SnapshotAttr_ObjectMaxBlock = "Object_max_block"
	SnapshotAttr_SchemaExtra    = "schema_extra"
	AccountIDDbNameTblName      = "account_id_db_name_tbl_name"
	AccountIDDbName             = "account_id_db_name"
	ObjectAttr_ObjectStats      = "object_stats"
	ObjectAttr_State            = "state"
	ObjectAttr_Sorted           = "sorted"
	EntryNode_CreateAt          = "create_at"
	EntryNode_DeleteAt          = "delete_at"
)

type DataFactory interface {
	MakeTableFactory() TableDataFactory
	MakeBlockFactory() BlockDataFactory
}

type Catalog struct {
	*IDAlloctor
	*sync.RWMutex

	usageMemo any
	entries   map[uint64]*common.GenericDLNode[*DBEntry]
	nameNodes map[string]*nodeList[*DBEntry]
	link      *common.GenericSortedDList[*DBEntry]
	nodesMu   sync.RWMutex
}

func genDBFullName(tenantID uint32, name string) string {
	if name == pkgcatalog.MO_CATALOG {
		tenantID = 0
	}
	return fmt.Sprintf("%d-%s", tenantID, name)
}

func MockCatalog() *Catalog {
	catalog := &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		entries:    make(map[uint64]*common.GenericDLNode[*DBEntry]),
		nameNodes:  make(map[string]*nodeList[*DBEntry]),
		link:       common.NewGenericSortedDList((*DBEntry).Less),
	}
	catalog.InitSystemDB()
	return catalog
}

func NewEmptyCatalog() *Catalog {
	return &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		entries:    make(map[uint64]*common.GenericDLNode[*DBEntry]),
		nameNodes:  make(map[string]*nodeList[*DBEntry]),
		link:       common.NewGenericSortedDList((*DBEntry).Less),
	}
}

func OpenCatalog(usageMemo any) (*Catalog, error) {
	catalog := &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		entries:    make(map[uint64]*common.GenericDLNode[*DBEntry]),
		nameNodes:  make(map[string]*nodeList[*DBEntry]),
		link:       common.NewGenericSortedDList((*DBEntry).Less),
		usageMemo:  usageMemo,
	}
	catalog.InitSystemDB()
	return catalog, nil
}

func (catalog *Catalog) SetUsageMemo(memo any) {
	catalog.usageMemo = memo
}

func (catalog *Catalog) GetUsageMemo() any {
	return catalog.usageMemo
}

func (catalog *Catalog) InitSystemDB() {
	sysDB := NewSystemDBEntry(catalog)
	dbTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_DATABASE_ID, SystemDBSchema)
	tableTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_TABLES_ID, SystemTableSchema)
	columnTables := NewSystemTableEntry(sysDB, pkgcatalog.MO_COLUMNS_ID, SystemColumnSchema)
	err := sysDB.AddEntryLocked(dbTables, nil, false)
	if err != nil {
		panic(err)
	}
	if err = sysDB.AddEntryLocked(tableTables, nil, false); err != nil {
		panic(err)
	}
	if err = sysDB.AddEntryLocked(columnTables, nil, false); err != nil {
		panic(err)
	}
	if err = catalog.AddEntryLocked(sysDB, nil, false); err != nil {
		panic(err)
	}
}
func (catalog *Catalog) GCByTS(ctx context.Context, ts types.TS) {
	logutil.Infof("GC Catalog %v", ts.ToString())
	processor := LoopProcessor{}
	processor.DatabaseFn = func(d *DBEntry) error {
		d.RLock()
		needGC := d.DeleteBefore(ts)
		d.RUnlock()
		if needGC {
			catalog.RemoveEntry(d)
		}
		return nil
	}
	processor.TableFn = func(te *TableEntry) error {
		te.RLock()
		needGC := te.DeleteBefore(ts)
		te.RUnlock()
		if needGC {
			db := te.db
			db.RemoveEntry(te)
		}
		return nil
	}
	processor.ObjectFn = func(se *ObjectEntry) error {
		se.RLock()
		needGC := se.DeleteBefore(ts)
		se.RUnlock()
		if needGC {
			tbl := se.table
			tbl.RemoveEntry(se)
		}
		return nil
	}
	processor.BlockFn = func(be *BlockEntry) error {
		be.RLock()
		needGC := be.DeleteBefore(ts)
		be.RUnlock()
		if needGC {
			obj := be.object
			obj.RemoveEntry(be)
		}
		return nil
	}
	err := catalog.RecurLoop(&processor)
	if err != nil {
		panic(err)
	}
}
func (catalog *Catalog) ReplayCmd(
	txncmd txnif.TxnCmd,
	dataFactory DataFactory,
	observer wal.ReplayObserver) {
	switch txncmd.GetType() {
	case txnbase.IOET_WALTxnCommand_Composed:
		cmds := txncmd.(*txnbase.ComposedCmd)
		for _, cmds := range cmds.Cmds {
			catalog.ReplayCmd(cmds, dataFactory, observer)
		}
	case IOET_WALTxnCommand_Database:
		cmd := txncmd.(*EntryCommand[*EmptyMVCCNode, *DBNode])
		catalog.onReplayUpdateDatabase(cmd, observer)
	case IOET_WALTxnCommand_Table:
		cmd := txncmd.(*EntryCommand[*TableMVCCNode, *TableNode])
		catalog.onReplayUpdateTable(cmd, dataFactory, observer)
	case IOET_WALTxnCommand_Object:
		cmd := txncmd.(*EntryCommand[*ObjectMVCCNode, *ObjectNode])
		catalog.onReplayUpdateObject(cmd, dataFactory, observer)
	case IOET_WALTxnCommand_Block:
		cmd := txncmd.(*EntryCommand[*MetadataMVCCNode, *BlockNode])
		catalog.onReplayUpdateBlock(cmd, dataFactory, observer)
	case IOET_WALTxnCommand_Segment:
		// segment is deprecated
		return
	default:
		panic("unsupport")
	}
}

func (catalog *Catalog) onReplayUpdateDatabase(cmd *EntryCommand[*EmptyMVCCNode, *DBNode], observer wal.ReplayObserver) {
	catalog.OnReplayDBID(cmd.ID.DbID)
	var err error
	un := cmd.mvccNode
	if un.Is1PC() {
		if err := un.ApplyCommit(); err != nil {
			panic(err)
		}
	}

	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		db = NewReplayDBEntry()
		db.ID = cmd.ID.DbID
		db.catalog = catalog
		db.DBNode = cmd.node
		db.Insert(un)
		err = catalog.AddEntryLocked(db, un.GetTxn(), false)
		if err != nil {
			panic(err)
		}
		return
	}

	dbun := db.SearchNode(un)
	if dbun == nil {
		db.Insert(un)
	} else {
		return
		// panic(fmt.Sprintf("logic err: duplicate node %v and %v", dbun.String(), un.String()))
	}
}

func (catalog *Catalog) OnReplayDatabaseBatch(ins, insTxn, del, delTxn *containers.Batch) {
	for i := 0; i < ins.Length(); i++ {
		dbid := ins.GetVectorByName(pkgcatalog.SystemDBAttr_ID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_Name).Get(i).([]byte))
		txnNode := txnbase.ReadTuple(insTxn, i)
		tenantID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_AccID).Get(i).(uint32)
		userID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_Creator).Get(i).(uint32)
		roleID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_Owner).Get(i).(uint32)
		createAt := ins.GetVectorByName(pkgcatalog.SystemDBAttr_CreateAt).Get(i).(types.Timestamp)
		createSql := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_CreateSQL).Get(i).([]byte))
		datType := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_Type).Get(i).([]byte))
		catalog.onReplayCreateDB(dbid, name, txnNode, tenantID, userID, roleID, createAt, createSql, datType)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		catalog.onReplayDeleteDB(dbid, txnNode)
	}
}

func (catalog *Catalog) onReplayCreateDB(
	dbid uint64, name string, txnNode *txnbase.TxnMVCCNode,
	tenantID, userID, roleID uint32, createAt types.Timestamp, createSql, datType string) {
	catalog.OnReplayDBID(dbid)
	db, _ := catalog.GetDatabaseByID(dbid)
	if db != nil {
		dbCreatedAt := db.GetCreatedAtLocked()
		if !dbCreatedAt.Equal(txnNode.End) {
			panic(moerr.NewInternalErrorNoCtx("logic err expect %s, get %s",
				txnNode.End.ToString(), dbCreatedAt.ToString()))
		}
		return
	}
	db = NewReplayDBEntry()
	db.catalog = catalog
	db.ID = dbid
	db.DBNode = &DBNode{
		acInfo: accessInfo{
			TenantID: tenantID,
			UserID:   userID,
			RoleID:   roleID,
			CreateAt: createAt,
		},
		createSql: createSql,
		datType:   datType,
		name:      name,
	}
	_ = catalog.AddEntryLocked(db, nil, true)
	un := &MVCCNode[*EmptyMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
	}
	db.Insert(un)
}
func (catalog *Catalog) onReplayDeleteDB(dbid uint64, txnNode *txnbase.TxnMVCCNode) {
	catalog.OnReplayDBID(dbid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info("delete %d", zap.Uint64("dbid", dbid), zap.String("catalog pp", catalog.SimplePPString(common.PPL3)))
		panic(err)
	}
	dbDeleteAt := db.GetDeleteAt()
	if !dbDeleteAt.IsEmpty() {
		if !dbDeleteAt.Equal(txnNode.End) {
			panic(moerr.NewInternalErrorNoCtx("logic err expect %s, get %s", txnNode.End.ToString(), dbDeleteAt.ToString()))
		}
		return
	}
	prev := db.MVCCChain.GetLatestNodeLocked()
	un := &MVCCNode[*EmptyMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: db.GetCreatedAtLocked(),
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
		BaseNode:    prev.BaseNode.CloneAll(),
	}
	db.Insert(un)
}
func (catalog *Catalog) onReplayUpdateTable(cmd *EntryCommand[*TableMVCCNode, *TableNode], dataFactory DataFactory, observer wal.ReplayObserver) {
	catalog.OnReplayTableID(cmd.ID.TableID)
	// prepareTS := cmd.GetTs()
	// if prepareTS.LessEq(catalog.GetCheckpointed().MaxTS) {
	// 	if observer != nil {
	// 		observer.OnStaleIndex(idx)
	// 	}
	// 	return
	// }
	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.ID.TableID)

	un := cmd.mvccNode
	if un.Is1PC() {
		if err := un.ApplyCommit(); err != nil {
			panic(err)
		}
	}

	if err != nil {
		tbl = NewReplayTableEntry()
		tbl.ID = cmd.ID.TableID
		tbl.db = db
		tbl.tableData = dataFactory.MakeTableFactory()(tbl)
		tbl.TableNode = cmd.node
		tbl.TableNode.schema.Store(un.BaseNode.Schema)
		tbl.Insert(un)
		err = db.AddEntryLocked(tbl, un.GetTxn(), true)
		if err != nil {
			logutil.Warn(catalog.SimplePPString(common.PPL3))
			panic(err)
		}
		return
	}
	tblun := tbl.SearchNode(un)
	if tblun == nil {
		tbl.Insert(un) //TODO isvalid
		if tbl.isColumnChangedInSchema() {
			tbl.FreezeAppend()
		}
		schema := un.BaseNode.Schema
		tbl.TableNode.schema.Store(schema)
		// alter table rename
		if schema.Extra.OldName != "" && un.DeletedAt.IsEmpty() {
			err := tbl.db.RenameTableInTxn(schema.Extra.OldName, schema.Name, tbl.ID, schema.AcInfo.TenantID, un.GetTxn(), true)
			if err != nil {
				logutil.Warn(schema.String())
				panic(err)
			}
		}
	}

}

func (catalog *Catalog) OnReplayTableBatch(ins, insTxn, insCol, del, delTxn *containers.Batch, dataFactory DataFactory) {
	schemaOffset := 0
	for i := 0; i < ins.Length(); i++ {
		tid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_ID).Get(i).(uint64)
		dbid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_DBID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Name).Get(i).([]byte))
		schema := NewEmptySchema(name)
		schemaOffset = schema.ReadFromBatch(insCol, schemaOffset, tid)
		schema.Comment = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Comment).Get(i).([]byte))
		schema.Version = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Version).Get(i).(uint32)
		schema.CatalogVersion = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion).Get(i).(uint32)
		schema.Partitioned = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partitioned).Get(i).(int8)
		schema.Partition = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partition).Get(i).([]byte))
		schema.Relkind = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Kind).Get(i).([]byte))
		schema.Createsql = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateSQL).Get(i).([]byte))
		schema.View = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_ViewDef).Get(i).([]byte))
		schema.Constraint = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Constraint).Get(i).([]byte)
		schema.AcInfo = accessInfo{}
		schema.AcInfo.RoleID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Owner).Get(i).(uint32)
		schema.AcInfo.UserID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Creator).Get(i).(uint32)
		schema.AcInfo.CreateAt = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateAt).Get(i).(types.Timestamp)
		schema.AcInfo.TenantID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_AccID).Get(i).(uint32)
		schema.BlockMaxRows = insTxn.GetVectorByName(SnapshotAttr_BlockMaxRow).Get(i).(uint32)
		schema.ObjectMaxBlocks = insTxn.GetVectorByName(SnapshotAttr_ObjectMaxBlock).Get(i).(uint16)
		extra := insTxn.GetVectorByName(SnapshotAttr_SchemaExtra).Get(i).([]byte)
		schema.MustRestoreExtra(extra)
		schema.Finalize(true)
		txnNode := txnbase.ReadTuple(insTxn, i)
		catalog.onReplayCreateTable(dbid, tid, schema, txnNode, dataFactory)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		catalog.onReplayDeleteTable(dbid, tid, txnNode)
	}
}

func (catalog *Catalog) onReplayCreateTable(dbid, tid uint64, schema *Schema, txnNode *txnbase.TxnMVCCNode, dataFactory DataFactory) {
	catalog.OnReplayTableID(tid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	tbl, _ := db.GetTableEntryByID(tid)
	if tbl != nil {
		tblCreatedAt := tbl.GetCreatedAtLocked()
		if tblCreatedAt.Greater(txnNode.End) {
			panic(moerr.NewInternalErrorNoCtx("logic err expect %s, get %s", txnNode.End.ToString(), tblCreatedAt.ToString()))
		}
		// alter table
		un := &MVCCNode[*TableMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: tblCreatedAt,
			},
			TxnMVCCNode: txnNode,
			BaseNode: &TableMVCCNode{
				Schema: schema,
			},
		}
		tbl.Insert(un)
		if tbl.isColumnChangedInSchema() {
			tbl.FreezeAppend()
		}
		tbl.TableNode.schema.Store(schema)
		if schema.Extra.OldName != "" {
			logutil.Infof("replay rename %v from %v -> %v", tid, schema.Extra.OldName, schema.Name)
			err := tbl.db.RenameTableInTxn(schema.Extra.OldName, schema.Name, tbl.ID, schema.AcInfo.TenantID, un.GetTxn(), true)
			if err != nil {
				logutil.Warn(schema.String())
				panic(err)
			}
		}

		return
	}
	tbl = NewReplayTableEntry()
	tbl.TableNode = &TableNode{}
	tbl.TableNode.schema.Store(schema)
	tbl.db = db
	tbl.ID = tid
	tbl.tableData = dataFactory.MakeTableFactory()(tbl)
	_ = db.AddEntryLocked(tbl, nil, true)
	un := &MVCCNode[*TableMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
		BaseNode: &TableMVCCNode{
			Schema: schema,
		},
	}
	tbl.Insert(un)
}
func (catalog *Catalog) onReplayDeleteTable(dbid, tid uint64, txnNode *txnbase.TxnMVCCNode) {
	catalog.OnReplayTableID(tid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(tid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	tableDeleteAt := tbl.GetDeleteAt()
	if !tableDeleteAt.IsEmpty() {
		if !tableDeleteAt.Equal(txnNode.End) {
			panic(moerr.NewInternalErrorNoCtx("logic err expect %s, get %s", txnNode.End.ToString(), tableDeleteAt.ToString()))
		}
		return
	}
	prev := tbl.MVCCChain.GetLatestCommittedNode()
	un := &MVCCNode[*TableMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: prev.CreatedAt,
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
		BaseNode:    prev.BaseNode.CloneAll(),
	}
	tbl.Insert(un)

}
func (catalog *Catalog) onReplayUpdateObject(
	cmd *EntryCommand[*ObjectMVCCNode, *ObjectNode],
	dataFactory DataFactory,
	observer wal.ReplayObserver) {
	catalog.OnReplayObjectID(cmd.node.SortHint)

	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.ID.TableID)
	if err != nil {
		logutil.Debugf("tbl %d-%d", cmd.ID.DbID, cmd.ID.TableID)
		logutil.Info(catalog.SimplePPString(3))
		panic(err)
	}
	obj, err := tbl.GetObjectByID(cmd.ID.ObjectID())
	un := cmd.mvccNode
	if un.Is1PC() {
		if err := un.ApplyCommit(); err != nil {
			panic(err)
		}
	}
	if err != nil {
		obj = NewReplayObjectEntry()
		obj.ID = *cmd.ID.ObjectID()
		obj.table = tbl
		obj.Insert(un)
		obj.ObjectNode = cmd.node
		tbl.AddEntryLocked(obj)
	} else {
		node := obj.SearchNode(un)
		if node == nil {
			obj.Insert(un)
		} else {
			node.BaseNode.Update(un.BaseNode)
		}
	}
}

func (catalog *Catalog) OnReplayObjectBatch(objectInfo *containers.Batch, dataFactory DataFactory) {
	dbidVec := objectInfo.GetVectorByName(SnapshotAttr_DBID)
	for i := 0; i < dbidVec.Length(); i++ {
		dbid := objectInfo.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := objectInfo.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		objectNode := ReadObjectInfoTuple(objectInfo, i)
		sid := objectNode.ObjectName().ObjectId()
		txnNode := txnbase.ReadTuple(objectInfo, i)
		entryNode := ReadEntryNodeTuple(objectInfo, i)
		state := objectInfo.GetVectorByName(ObjectAttr_State).Get(i).(bool)
		entryState := ES_Appendable
		if !state {
			entryState = ES_NotAppendable
		}
		catalog.onReplayCheckpointObject(dbid, tid, sid, objectNode, entryNode, txnNode, entryState, dataFactory)
	}
}

func (catalog *Catalog) onReplayCheckpointObject(
	dbid, tbid uint64,
	objid *types.Objectid,
	objNode *ObjectMVCCNode,
	entryNode *EntryMVCCNode,
	txnNode *txnbase.TxnMVCCNode,
	state EntryState,
	dataFactory DataFactory,
) {
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	rel, err := db.GetTableEntryByID(tbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	obj, _ := rel.GetObjectByID(objid)
	if obj == nil {
		obj = NewReplayObjectEntry()
		obj.ID = *objid
		obj.table = rel
		obj.ObjectNode = &ObjectNode{
			state:    state,
			sorted:   state == ES_NotAppendable,
			SortHint: catalog.NextObject(),
		}
		rel.AddEntryLocked(obj)
	}
	un := &MVCCNode[*ObjectMVCCNode]{
		EntryMVCCNode: entryNode,
		BaseNode:      objNode,
		TxnMVCCNode:   txnNode,
	}
	node := obj.SearchNode(un)
	if node == nil {
		obj.Insert(un)
	} else {
		node.BaseNode.Update(un.BaseNode)
	}
	if entryNode.DeletedAt.IsEmpty() {
		stats := objNode.ObjectStats
		blkCount := stats.BlkCnt()
		// for aobj, stats is empty when create.
		// aobj always has one blk.
		if state == ES_Appendable {
			blkCount = 1
		}
		leftRows := stats.Rows()
		blkMaxRow := rel.GetLastestSchema().BlockMaxRows
		for i := 0; i < int(blkCount); i++ {
			blkID := objectio.NewBlockidWithObjectID(objid, uint16(i))
			rows := blkMaxRow
			if leftRows < blkMaxRow {
				rows = leftRows
			}
			leftRows -= rows
			var metaLoc objectio.Location
			if state != ES_Appendable {
				metaLoc = objectio.BuildLocation(stats.ObjectName(), stats.Extent(), rows, uint16(i))
			}
			catalog.onReplayCreateBlock(dbid, tbid, objid, blkID, state, metaLoc, nil, txnNode, dataFactory)
		}
	} else {
		stats := objNode.ObjectStats
		blkCount := stats.BlkCnt()
		leftRows := stats.Rows()
		blkMaxRow := rel.GetLastestSchema().BlockMaxRows
		for i := 0; i < int(blkCount); i++ {
			blkID := objectio.NewBlockidWithObjectID(objid, uint16(i))
			rows := blkMaxRow
			if leftRows < blkMaxRow {
				rows = leftRows
			}
			leftRows -= rows
			metaLoc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), rows, uint16(i))
			catalog.onReplayDeleteBlock(dbid, tbid, objid, blkID, metaLoc, nil, txnNode)
		}
	}
}

// Before ckp version 10 and IOET_WALTxnCommand_Object, object info doesn't exist.
// Replay Object by block.
// Original Size, Compressed Size and Block Number is skipped.
func (catalog *Catalog) replayObjectByBlock(
	tbl *TableEntry,
	blkID types.Blockid,
	state EntryState,
	start, end types.TS,
	metaLocation objectio.Location,
	needApplyCommit bool,
	create, delete bool,
	txn txnif.TxnReader,
	dataFactory DataFactory) {
	ObjectID := blkID.Object()
	obj, _ := tbl.GetObjectByID(ObjectID)
	// create
	if create {
		if obj == nil {
			obj = NewObjectEntryByMetaLocation(
				tbl,
				ObjectID,
				start, end, state, metaLocation)
			tbl.AddEntryLocked(obj)
		}
	}
	// delete
	if delete {
		node := obj.SearchNode(
			&MVCCNode[*ObjectMVCCNode]{
				TxnMVCCNode: &txnbase.TxnMVCCNode{
					Start: start,
				},
			},
		)
		if node == nil {
			node = obj.GetLatestNodeLocked().CloneData()
			node.Start = start
			node.Prepare = end
			node.End = end
			if node.BaseNode.IsEmpty() {
				node.BaseNode = NewObjectInfoWithMetaLocation(metaLocation, ObjectID)
			}
			obj.Insert(node)
			node.DeletedAt = end
		}
	}
}
func (catalog *Catalog) onReplayUpdateBlock(
	cmd *EntryCommand[*MetadataMVCCNode, *BlockNode],
	dataFactory DataFactory,
	observer wal.ReplayObserver) {
	// catalog.OnReplayBlockID(cmd.ID.BlockID)
	prepareTS := cmd.GetTs()
	db, err := catalog.GetDatabaseByID(cmd.ID.DbID)
	if err != nil {
		panic(err)
	}
	tbl, err := db.GetTableEntryByID(cmd.ID.TableID)
	if err != nil {
		panic(err)
	}
	catalog.replayObjectByBlock(
		tbl,
		cmd.ID.BlockID,
		cmd.node.state,
		cmd.mvccNode.Start,
		cmd.mvccNode.Prepare,
		cmd.mvccNode.BaseNode.MetaLoc,
		true,
		cmd.mvccNode.CreatedAt.Equal(txnif.UncommitTS),
		cmd.mvccNode.DeletedAt.Equal(txnif.UncommitTS),
		cmd.mvccNode.Txn,
		dataFactory)
	obj, err := tbl.GetObjectByID(cmd.ID.ObjectID())
	if err != nil {
		panic(err)
	}
	blk, err := obj.GetBlockEntryByID(&cmd.ID.BlockID)
	un := cmd.mvccNode
	if un.Is1PC() {
		if err := un.ApplyCommit(); err != nil {
			panic(err)
		}
	}
	if !un.BaseNode.DeltaLoc.IsEmpty() {
		name := un.BaseNode.DeltaLoc.Name()
		objn := name.Num()
		obj.replayNextObjectIdx(objn)
	}
	if err == nil {
		blkun := blk.SearchNode(un)
		if blkun != nil {
			blkun.Update(un)
		} else {
			blk.Insert(un)
			blk.location = un.BaseNode.MetaLoc
		}
		blk.blkData.TryUpgrade()
		blk.blkData.GCInMemeoryDeletesByTS(blk.GetDeltaPersistedTS())
		return
	}
	blk = NewReplayBlockEntry()
	blk.ID = cmd.ID.BlockID
	blk.BlockNode = *cmd.node
	blk.BaseEntryImpl.Insert(un)
	blk.location = un.BaseNode.MetaLoc
	blk.object = obj
	if blk.blkData == nil {
		blk.blkData = dataFactory.MakeBlockFactory()(blk)
	} else {
		blk.blkData.TryUpgrade()
	}
	blk.blkData.GCInMemeoryDeletesByTS(blk.GetDeltaPersistedTS())
	if observer != nil {
		observer.OnTimeStamp(prepareTS)
	}
	obj.ReplayAddEntryLocked(blk)
}

func (catalog *Catalog) OnReplayBlockBatch(ins, insTxn, del, delTxn *containers.Batch, dataFactory DataFactory) {
	for i := 0; i < ins.Length(); i++ {
		dbid := insTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := insTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		appendable := ins.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Get(i).(bool)
		state := ES_NotAppendable
		if appendable {
			state = ES_Appendable
		}
		blkID := ins.GetVectorByName(pkgcatalog.BlockMeta_ID).Get(i).(types.Blockid)
		sid := blkID.Object()
		metaLoc := ins.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte)
		deltaLoc := ins.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Get(i).([]byte)
		txnNode := txnbase.ReadTuple(insTxn, i)
		catalog.onReplayCreateBlock(dbid, tid, sid, &blkID, state, metaLoc, deltaLoc, txnNode, dataFactory)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		rid := del.GetVectorByName(AttrRowID).Get(i).(types.Rowid)
		blkID := rid.BorrowBlockID()
		sid := rid.BorrowObjectID()
		un := txnbase.ReadTuple(delTxn, i)
		metaLoc := delTxn.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte)
		deltaLoc := delTxn.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Get(i).([]byte)
		catalog.onReplayDeleteBlock(dbid, tid, sid, blkID, metaLoc, deltaLoc, un)
	}
}
func (catalog *Catalog) onReplayCreateBlock(
	dbid, tid uint64,
	objid *types.Objectid,
	blkid *types.Blockid,
	state EntryState,
	metaloc, deltaloc objectio.Location,
	txnNode *txnbase.TxnMVCCNode,
	dataFactory DataFactory) {
	// catalog.OnReplayBlockID(blkid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	rel, err := db.GetTableEntryByID(tid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	catalog.replayObjectByBlock(
		rel,
		*blkid,
		state,
		txnNode.Start,
		txnNode.End,
		metaloc,
		false,
		true,
		false,
		nil,
		dataFactory)
	obj, err := rel.GetObjectByID(objid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	if !deltaloc.IsEmpty() {
		name := deltaloc.Name()
		objn := name.Num()
		obj.replayNextObjectIdx(objn)
	}
	blk, _ := obj.GetBlockEntryByID(blkid)
	var un *MVCCNode[*MetadataMVCCNode]
	if blk == nil {
		blk = NewReplayBlockEntry()
		blk.object = obj
		blk.ID = *blkid
		blk.state = state
		obj.ReplayAddEntryLocked(blk)
		un = &MVCCNode[*MetadataMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: txnNode.End,
			},
			TxnMVCCNode: txnNode,
			BaseNode: &MetadataMVCCNode{
				MetaLoc:  metaloc,
				DeltaLoc: deltaloc,
			},
		}
		blk.Insert(un)
	} else {
		prevUn := blk.MVCCChain.GetLatestNodeLocked()
		un = &MVCCNode[*MetadataMVCCNode]{
			EntryMVCCNode: &EntryMVCCNode{
				CreatedAt: prevUn.CreatedAt,
			},
			TxnMVCCNode: txnNode,
			BaseNode: &MetadataMVCCNode{
				MetaLoc:  metaloc,
				DeltaLoc: deltaloc,
			},
		}
		node := blk.MVCCChain.SearchNode(un)
		if node != nil {
			node.IdempotentUpdate(un)
		} else {
			blk.Insert(un)
		}
	}
	blk.location = un.BaseNode.MetaLoc
	if blk.blkData == nil {
		blk.blkData = dataFactory.MakeBlockFactory()(blk)
	} else {
		blk.blkData.TryUpgrade()
	}
	blk.blkData.GCInMemeoryDeletesByTS(blk.GetDeltaPersistedTS())
}

func (catalog *Catalog) onReplayDeleteBlock(
	dbid, tid uint64,
	objid *types.Objectid,
	blkid *types.Blockid,
	metaloc,
	deltaloc objectio.Location,
	txnNode *txnbase.TxnMVCCNode,
) {
	// catalog.OnReplayBlockID(blkid)
	db, err := catalog.GetDatabaseByID(dbid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	rel, err := db.GetTableEntryByID(tid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	catalog.replayObjectByBlock(
		rel,
		*blkid,
		ES_Appendable, // state is not used when delete
		txnNode.Start,
		txnNode.End,
		metaloc,
		false,
		false,
		true,
		nil,
		nil)
	obj, err := rel.GetObjectByID(objid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}
	if !deltaloc.IsEmpty() {
		name := deltaloc.Name()
		objn := name.Num()
		obj.replayNextObjectIdx(objn)
	}
	blk, err := obj.GetBlockEntryByID(blkid)
	if err != nil {
		logutil.Info(catalog.SimplePPString(common.PPL3))
		panic(err)
	}

	prevUn := blk.MVCCChain.GetLatestNodeLocked()
	un := &MVCCNode[*MetadataMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: prevUn.CreatedAt,
			DeletedAt: txnNode.End,
		},
		TxnMVCCNode: txnNode,
		BaseNode: &MetadataMVCCNode{
			MetaLoc:  metaloc,
			DeltaLoc: deltaloc,
		},
	}
	node := blk.MVCCChain.SearchNode(un)
	if node != nil {
		node.IdempotentUpdate(un)
	} else {
		blk.MVCCChain.Insert(un)
	}
	blk.location = un.BaseNode.MetaLoc
	blk.blkData.TryUpgrade()
	blk.blkData.GCInMemeoryDeletesByTS(blk.GetDeltaPersistedTS())
}
func (catalog *Catalog) ReplayTableRows() {
	rows := uint64(0)
	tableProcessor := new(LoopProcessor)
	tableProcessor.BlockFn = func(be *BlockEntry) error {
		if !be.IsActive() {
			return nil
		}
		rows += be.GetBlockData().GetRowsOnReplay()
		return nil
	}
	processor := new(LoopProcessor)
	processor.TableFn = func(tbl *TableEntry) error {
		if tbl.db.name == pkgcatalog.MO_CATALOG {
			return nil
		}
		rows = 0
		err := tbl.RecurLoop(tableProcessor)
		if err != nil {
			panic(err)
		}
		tbl.rows.Store(rows)
		return nil
	}
	err := catalog.RecurLoop(processor)
	if err != nil {
		panic(err)
	}
}
func (catalog *Catalog) Close() error {
	return nil
}

func (catalog *Catalog) CoarseDBCnt() int {
	catalog.RLock()
	defer catalog.RUnlock()
	return len(catalog.entries)
}

func (catalog *Catalog) GetItemNodeByIDLocked(id uint64) *common.GenericDLNode[*DBEntry] {
	return catalog.entries[id]
}

func (catalog *Catalog) GetDatabaseByID(id uint64) (db *DBEntry, err error) {
	catalog.RLock()
	defer catalog.RUnlock()
	node := catalog.entries[id]
	if node == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	db = node.GetPayload()
	return
}

func (catalog *Catalog) AddEntryLocked(database *DBEntry, txn txnif.TxnReader, skipDedup bool) error {
	nn := catalog.nameNodes[database.GetFullName()]
	if nn == nil {
		n := catalog.link.Insert(database)
		catalog.entries[database.ID] = n

		nn := newNodeList(catalog.GetItemNodeByIDLocked,
			dbVisibilityFn[*DBEntry],
			&catalog.nodesMu,
			database.name)
		catalog.nameNodes[database.GetFullName()] = nn

		nn.CreateNode(database.ID)
	} else {
		node := nn.GetNode()
		if !skipDedup {
			record := node.GetPayload()
			err := record.PrepareAdd(txn)
			if err != nil {
				return err
			}
		}
		n := catalog.link.Insert(database)
		catalog.entries[database.ID] = n
		nn.CreateNode(database.ID)
	}
	return nil
}

func (catalog *Catalog) MakeDBIt(reverse bool) *common.GenericSortedDListIt[*DBEntry] {
	catalog.RLock()
	defer catalog.RUnlock()
	return common.NewGenericSortedDListIt(catalog.RWMutex, catalog.link, reverse)
}

func (catalog *Catalog) SimplePPString(level common.PPLevel) string {
	return catalog.PPString(level, 0, "")
}

func (catalog *Catalog) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	cnt := 0
	it := catalog.MakeDBIt(true)
	for it.Valid() {
		cnt++
		entry := it.Get().GetPayload()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(entry.PPString(level, depth+1, ""))
		it.Next()
	}

	var w2 bytes.Buffer
	_, _ = w2.WriteString(fmt.Sprintf("CATALOG[CNT=%d]", cnt))
	_, _ = w2.WriteString(w.String())
	return w2.String()
}

func (catalog *Catalog) RemoveEntry(database *DBEntry) error {
	if database.IsSystemDB() {
		logutil.Warnf("system db cannot be removed")
		return moerr.NewTAEErrorNoCtx("not permitted")
	}
	logutil.Info("[Catalog]", common.OperationField("remove"),
		common.OperandField(database.String()))
	catalog.Lock()
	defer catalog.Unlock()
	if n, ok := catalog.entries[database.ID]; !ok {
		return moerr.NewBadDBNoCtx(database.GetName())
	} else {
		nn := catalog.nameNodes[database.GetFullName()]
		nn.DeleteNode(database.ID)
		catalog.link.Delete(n)
		if nn.Length() == 0 {
			delete(catalog.nameNodes, database.GetFullName())
		}
		delete(catalog.entries, database.ID)
	}
	return nil
}

func (catalog *Catalog) txnGetNodeByName(
	tenantID uint32,
	name string,
	txn txnif.TxnReader) (*common.GenericDLNode[*DBEntry], error) {
	catalog.RLock()
	defer catalog.RUnlock()
	fullName := genDBFullName(tenantID, name)
	node := catalog.nameNodes[fullName]
	if node == nil {
		return nil, moerr.NewBadDBNoCtx(name)
	}
	return node.TxnGetNodeLocked(txn, "")
}

func (catalog *Catalog) GetDBEntryByName(
	tenantID uint32,
	name string,
	txn txnif.TxnReader) (db *DBEntry, err error) {
	n, err := catalog.txnGetNodeByName(tenantID, name, txn)
	if err != nil {
		return
	}
	db = n.GetPayload()
	return
}

func (catalog *Catalog) TxnGetDBEntryByName(name string, txn txnif.AsyncTxn) (*DBEntry, error) {
	n, err := catalog.txnGetNodeByName(txn.GetTenantID(), name, txn)
	if err != nil {
		return nil, err
	}
	return n.GetPayload(), nil
}

func (catalog *Catalog) TxnGetDBEntryByID(id uint64, txn txnif.AsyncTxn) (*DBEntry, error) {
	dbEntry, err := catalog.GetDatabaseByID(id)
	if err != nil {
		return nil, err
	}
	visiable, dropped := dbEntry.GetVisibility(txn)
	if !visiable || dropped {
		return nil, moerr.GetOkExpectedEOB()
	}
	return dbEntry, nil
}

func (catalog *Catalog) DropDBEntry(
	name string,
	txn txnif.AsyncTxn) (newEntry bool, deleted *DBEntry, err error) {
	if name == pkgcatalog.MO_CATALOG {
		err = moerr.NewTAEErrorNoCtx("not permitted")
		return
	}
	tn, err := catalog.txnGetNodeByName(txn.GetTenantID(), name, txn)
	if err != nil {
		return
	}
	entry := tn.GetPayload()
	entry.Lock()
	defer entry.Unlock()
	if newEntry, err = entry.DropEntryLocked(txn); err == nil {
		deleted = entry
	}
	return
}

func (catalog *Catalog) DropDBEntryByID(id uint64, txn txnif.AsyncTxn) (newEntry bool, deleted *DBEntry, err error) {
	if id == pkgcatalog.MO_CATALOG_ID {
		err = moerr.NewTAEErrorNoCtx("not permitted")
		return
	}
	entry, err := catalog.GetDatabaseByID(id)
	if err != nil {
		return
	}
	entry.Lock()
	defer entry.Unlock()
	if newEntry, err = entry.DropEntryLocked(txn); err == nil {
		deleted = entry
	}
	return
}

func (catalog *Catalog) CreateDBEntry(name, createSql, datTyp string, txn txnif.AsyncTxn) (*DBEntry, error) {
	var err error
	catalog.Lock()
	defer catalog.Unlock()
	entry := NewDBEntry(catalog, name, createSql, datTyp, txn)
	err = catalog.AddEntryLocked(entry, txn, false)

	return entry, err
}

func (catalog *Catalog) CreateDBEntryWithID(name, createSql, datTyp string, id uint64, txn txnif.AsyncTxn) (*DBEntry, error) {
	var err error
	catalog.Lock()
	defer catalog.Unlock()
	if _, exist := catalog.entries[id]; exist {
		return nil, moerr.GetOkExpectedDup()
	}
	entry := NewDBEntryWithID(catalog, name, createSql, datTyp, id, txn)
	err = catalog.AddEntryLocked(entry, txn, false)

	return entry, err
}

func (catalog *Catalog) CreateDBEntryByTS(name string, ts types.TS) (*DBEntry, error) {
	entry := NewDBEntryByTS(catalog, name, ts)
	err := catalog.AddEntryLocked(entry, nil, false)
	return entry, err
}

func (catalog *Catalog) RecurLoop(processor Processor) (err error) {
	dbIt := catalog.MakeDBIt(true)
	for dbIt.Valid() {
		dbEntry := dbIt.Get().GetPayload()
		if err = processor.OnDatabase(dbEntry); err != nil {
			// XXX: Performance problem.   Error should not be used
			// to handle normal return code.
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				dbIt.Next()
				continue
			}
			break
		}
		if err = dbEntry.RecurLoop(processor); err != nil {
			return
		}
		if err = processor.OnPostDatabase(dbEntry); err != nil {
			break
		}
		dbIt.Next()
	}
	if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
		err = nil
	}
	return err
}
