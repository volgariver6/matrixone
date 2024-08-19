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
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"go.uber.org/zap"
)

const (
	defaultDataSize  = 1024 * 2
	truncateInterval = time.Second * 10
)

var (
	binaryEnc = binary.BigEndian
)

type syncer struct {
	common
	ctx        context.Context
	appender   Appender
	truncation Worker
	reader     *reader
	dataSync   Worker

	// syncDataQ only contains the data that need to sync to
	// the other s3 storage.
	syncDataQ queue

	syncedLSN atomic.Uint64
}

// NewDataSync creates a new syncer instance.
func NewDataSync(
	stopper *stopper.Stopper,
	log *log.MOLogger,
	sid string,
	cfg logservice.HAKeeperClientConfig,
) (logservice.DataSync, error) {
	ss := &syncer{
		ctx: context.Background(),
		common: common{
			stopper:        stopper,
			log:            log.With(zap.String("module", "datasync")),
			sid:            sid,
			haKeeperConfig: cfg,
			pool:           newDataPool(defaultDataSize),
		},
		syncDataQ: newDataQueue(1024),
	}
	ss.appender = newAppender(ss.syncDataQ, withAppenderHAKeeperClientConfig(cfg))
	ss.dataSync = newDataSync(ss.syncDataQ, &ss.syncedLSN)
	ss.truncation = newTruncation(
		ss.common,
		&ss.syncedLSN,
		withTruncateInterval(truncateInterval),
		withTruncationHAKeeperClientConfig(cfg),
	)
	if err := ss.stopper.RunNamedTask("data-syncer", ss.start); err != nil {
		return nil, err
	}
	return ss, nil
}

// TODO(liubo): what to do when the chan is full?
func (s *syncer) Append(data []byte) {
	// Acquire data intance from pool.
	w := s.pool.acquire(len(data))
	if len(w.data) != len(data) {
		panic(fmt.Sprintf("mismatch data length: %d and %d",
			len(w.data), len(data)))
	}

	// copy the data.
	b := copy(w.data, data)
	if b != len(data) {
		panic(fmt.Sprintf("mismatch data length: %d and %d",
			b, len(data)))
	}

	// send data to the queue.
	s.appender.Enqueue(w)
}

func (s *syncer) Close() error {
	s.reader.close()
	s.appender.Close()
	s.truncation.Close()
	s.dataSync.Close()
	s.syncDataQ.close()
	return nil
}

// start starts the goroutines in the syncer module:
func (s *syncer) start(ctx context.Context) {
	// start truncation worker.
	go s.truncation.Start(ctx)

	// start the sync data worker, which fetch items in the sync-data-queue
	// and do the sync job.
	go s.dataSync.Start(ctx)

	// read truncated lsn from RSM.
	var lsn uint64
	s.log.Info("syncer start, read lsn", zap.Uint64("LSN", lsn))

	// read entries the log storage and put them into sync-data-queue.

	// the filter worker should start after replay entries in the WAL.
	go s.appender.Start(ctx)

	// wait the end of the whole the process.
	<-ctx.Done()
}
