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
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
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
	ctx     context.Context
	log     *log.MOLogger
	stopper *stopper.Stopper
	uuid    string

	cfg logservice.HAKeeperClientConfig
	// haKeeperClient is used to get log shard ID from HAKeeper
	haKeeperClient logservice.LogHAKeeperClient

	// logClient is used to append data to log shard.
	appendClient logservice.Client

	// truncateClient is used to update truncate LSN of the log shard.
	truncateClient logservice.Client

	truncation *truncation

	pool dataPool

	// allDataQueue contains all the data comes from TN shard.
	allDataQueue queue

	// syncDataQueue only contains the data that need to sync to
	// the other s3 storage.
	syncDataQueue queue

	syncedLSN atomic.Uint64
}

// NewDataSync creates a new syncer instance.
func NewDataSync(
	stopper *stopper.Stopper,
	log *log.MOLogger,
	uuid string,
	cfg logservice.HAKeeperClientConfig,
) (logservice.DataSync, error) {
	ss := &syncer{
		ctx:           context.Background(),
		stopper:       stopper,
		log:           log,
		uuid:          uuid,
		cfg:           cfg,
		pool:          newDataPool(defaultDataSize),
		allDataQueue:  newDataQueue(10240),
		syncDataQueue: newDataQueue(1024),
	}
	ss.truncation = newTruncation(
		uuid,
		stopper,
		log,
		&ss.syncedLSN,
		withTruncateInterval(truncateInterval),
		withHAKeeperClientConfig(cfg),
	)
	if err := ss.stopper.RunNamedTask("data-syncer", ss.start); err != nil {
		return nil, err
	}
	return ss, nil
}

// TODO(liubo): what to do when the chan is full?
func (s *syncer) Append(data []byte) {
	// Acquire data intance from pool.
	d := s.pool.acquire(len(data))
	if len(d.data) != len(data) {
		panic(fmt.Sprintf("mismatch data length: %d and %d",
			len(d.data), len(data)))
	}

	// copy the data.
	b := copy(d.data, data)
	if b != len(data) {
		panic(fmt.Sprintf("mismatch data length: %d and %d",
			b, len(data)))
	}

	// send data to the queue.
	s.allDataQueue.enqueue(d)
}

func (s *syncer) Close() error {
	if s.haKeeperClient != nil {
		if err := s.haKeeperClient.Close(); err != nil {
			s.log.Error("failed to close HAKeeper client", zap.Error(err))
		}
	}
	// close the queues.
	s.allDataQueue.close()
	s.syncDataQueue.close()
	return nil
}

func (s *syncer) startFilterData(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			w, err := s.allDataQueue.dequeue(ctx)
			if err != nil {
				s.log.Error("failed to dequeue data", zap.Error(err))
				return
			}
			if s3Related(w) {
				s.syncDataQueue.enqueue(w)
			} else {
				s.pool.release(w)
			}
		}
	}
}

func (s *syncer) startSyncData(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			w, err := s.syncDataQueue.dequeue(ctx)
			if err != nil {
				s.log.Error("failed to dequeue data", zap.Error(err))
				return
			}

			// 1. save to WAL
			lsn := s.saveWAL(w.data)
			w.lsn = lsn

			// 2. sync data
			s.syncData(w)

			// 3. release data
			s.pool.release(w)
		}
	}
}

func (s *syncer) start(ctx context.Context) {
	go s.startFilterData(ctx)

	go s.startSyncData(ctx)
}

// adjustLeaseHolderID updates the leaseholder ID of the data.
// The original leaseholder ID is replica ID of TN, set it to 0.
func adjustLeaseholderID(old []byte) {
	// headerSize is 4.
	binaryEnc.PutUint64(old[4:], 0)
}

func (s *syncer) prepareAppend(ctx context.Context) {
	if s.haKeeperClient == nil {
		createHAKeeperClient(ctx, s.uuid, s.cfg)
	}
	if s.appendClient == nil {
		s.appendClient = createLogServiceClient(
			ctx,
			s.uuid,
			s.haKeeperClient,
			s.cfg,
		)
	}
}

func (s *syncer) saveWAL(data []byte) uint64 {
	s.prepareAppend(s.ctx)
	ctx, cancel := context.WithTimeout(s.ctx, time.Second*5)
	defer cancel()
	adjustLeaseholderID(data)
	lsn, err := s.appendClient.Append(ctx, pb.LogRecord{Data: data})
	if err != nil {
		s.log.Error("liubo: append error", zap.Error(err))
	}
	return lsn
}
