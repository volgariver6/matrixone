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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

type appenderConfig struct {
	haKeeperCfg logservice.HAKeeperClientConfig
}

type appender struct {
	common
	// cfg is appender module configuration.
	cfg appenderConfig
	// the client to HAKeeper server.
	haKeeperClient logservice.LogHAKeeperClient
	// the client to logservice server.
	client logservice.Client
	// allDataQ contains all the data comes from TN shard.
	allDataQ queue
	// syncDataQueue only contains the data that need to sync to
	// the other s3 storage.
	syncDataQ queue
}

type appenderOption func(*appender)

func withAppenderHAKeeperClientConfig(v logservice.HAKeeperClientConfig) appenderOption {
	return func(t *appender) {
		t.cfg.haKeeperCfg = v
	}
}

func newAppender(syncDataQ queue, opts ...appenderOption) Appender {
	ap := &appender{
		allDataQ:  newDataQueue(10240),
		syncDataQ: syncDataQ,
	}
	for _, opt := range opts {
		opt(ap)
	}
	return ap
}

func (a *appender) Enqueue(w *wrappedData) {
	a.allDataQ.enqueue(w)
}

func (a *appender) Close() {
	if err := a.haKeeperClient.Close(); err != nil {
		a.log.Error("failed to close HAKeeper client", zap.Error(err))
	}
	if err := a.client.Close(); err != nil {
		a.log.Error("failed to close LogService client", zap.Error(err))
	}
	a.allDataQ.close()
}

func (a *appender) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			w, err := a.allDataQ.dequeue(ctx)
			if err != nil {
				a.log.Error("failed to dequeue data", zap.Error(err))
				a.pool.release(w)
				continue
			}

			if s3Related(w) {
				// First save the data to WAL.
				lsn := a.saveWAL(ctx, w.data)
				w.lsn = lsn

				// Enqueue the data to another queue to take the action
				// to sync data from one storage to another.
				a.syncDataQ.enqueue(w)
			} else {
				a.pool.release(w)
			}
		}
	}
}

func (a *appender) prepareAppend(ctx context.Context) {
	if a.haKeeperClient == nil {
		createHAKeeperClient(ctx, a.sid, a.haKeeperConfig)
	}
	if a.client == nil {
		a.client = createLogServiceClient(
			ctx,
			a.sid,
			a.haKeeperClient,
			a.haKeeperConfig,
		)
	}
}

// adjustLeaseHolderID updates the leaseholder ID of the data.
// The original leaseholder ID is replica ID of TN, set it to 0.
func adjustLeaseholderID(old []byte) {
	// headerSize is 4.
	binaryEnc.PutUint64(old[4:], 0)
}

func (a *appender) saveWAL(ctx context.Context, data []byte) uint64 {
	a.prepareAppend(ctx)
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	adjustLeaseholderID(data)
	lsn, err := a.client.Append(ctx, pb.LogRecord{Data: data})
	if err != nil {
		a.log.Error("liubo: append error", zap.Error(err))
	}
	return lsn
}
