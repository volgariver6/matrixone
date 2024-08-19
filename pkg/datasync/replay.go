// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logservice"
)

type readerConfig struct {
	haKeeperCfg logservice.HAKeeperClientConfig
}

type replay struct {
	common
	// cfg is reader module configuration.
	cfg readerConfig
	// the client to HAKeeper server.
	haKeeperClient logservice.LogHAKeeperClient
	// the client to logservice server.
	client logservice.Client
}

func newReplay() Worker {
	return &replay{}
}

func (r *replay) Start(ctx context.Context) {
	/*
		lsn, err := r.getLSN(ctx)
		if err != nil {
			r.log.Error("replay cannot get lsn", zap.Error(err))
			panic(fmt.Sprintf("replay cannot get lsn: %v", err))
		}

		r.log.Info("replay get lsn", zap.Uint64("LSN", lsn))
	*/

	// read entries the log storage and put them into sync-data-queue.
	return
}

func (r *replay) Close() {

}

func (r *replay) prepareRead(ctx context.Context) {
	if r.haKeeperClient == nil {
		createHAKeeperClient(ctx, r.sid, r.haKeeperConfig)
	}
	if r.client == nil {
		r.client = createLogServiceClient(
			ctx,
			r.sid,
			r.haKeeperClient,
			r.haKeeperConfig,
		)
	}
}

func (r *replay) getLSN(ctx context.Context) (uint64, error) {
	r.prepareRead(ctx)
	lsn, err := r.client.GetTruncatedLsn(ctx)
	if err != nil {
		return 0, err
	}
	return lsn, nil
}
