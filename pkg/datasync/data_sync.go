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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

const (
	defaultDataSize = 1024 * 2
)

var (
	binaryEnc = binary.BigEndian
)

// s3Related checks if the data is operating s3 storage.
func s3Related(data []byte) bool {
	return true
}

type dataSync struct {
	ctx     context.Context
	log     *log.MOLogger
	stopper *stopper.Stopper
	uuid    string

	cfg logservice.HAKeeperClientConfig
	// HAKeeperClient is used to get log shard ID from HAKeeper
	HAKeeperClient logservice.LogHAKeeperClient

	// logClient is used to send data to log shard.
	logClient logservice.Client

	// allDataQueue contains all the data comes from TN shard.
	allDataQueue queue

	// syncDataQueue only contains the data that need to sync to
	// the other s3 storage.
	syncDataQueue queue

	latestLSN uint64
}

// NewDataSync creates a new dataSync instance.
func NewDataSync(
	stopper *stopper.Stopper,
	log *log.MOLogger,
	uuid string,
	cfg logservice.HAKeeperClientConfig,
) (logservice.DataSync, error) {
	ss := &dataSync{
		ctx:           context.Background(),
		stopper:       stopper,
		log:           log,
		uuid:          uuid,
		cfg:           cfg,
		allDataQueue:  newDataQueue(defaultDataSize, 10240),
		syncDataQueue: newDataQueue(defaultDataSize, 1024),
	}
	if err := ss.stopper.RunNamedTask("data-syncer", ss.start); err != nil {
		return nil, err
	}
	return ss, nil
}

func (s *dataSync) createHAKeeperClient(cfg logservice.HAKeeperClientConfig) {
	createFn := func() error {
		ctx, cancel := context.WithTimeout(s.ctx, time.Second*5)
		defer cancel()
		cc, err := logservice.NewLogHAKeeperClient(ctx, s.uuid, cfg)
		if err != nil {
			s.log.Error("failed to create HAKeeper client", zap.Error(err))
			return err
		}
		s.HAKeeperClient = cc
		return nil
	}
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			panic("failed to create HAKeeper client")

		default:
			if err := createFn(); err != nil {
				time.Sleep(time.Second * 3)
				continue
			}
			return
		}
	}
}

func (s *dataSync) createLogClient() {
	createFn := func() error {
		ctx, cancel := context.WithTimeout(s.ctx, time.Second*5)
		defer cancel()
		state, err := s.HAKeeperClient.GetClusterState(ctx)
		if err != nil {
			s.log.Error("failed to get cluster details", zap.Error(err))
			return err
		}
		// There must be at least three shards, the first one is for HAKeeper,
		// the second one is for TN, and the last one is for data sync.
		if len(state.ClusterInfo.LogShards) < 3 {
			panic("not enough shards")
		}
		shard := state.ClusterInfo.LogShards[2]
		logClient, err := logservice.NewClient(ctx, s.uuid, logservice.ClientConfig{
			ReadOnly:         false,
			LogShardID:       shard.ShardID,
			ServiceAddresses: s.cfg.ServiceAddresses,
			DiscoveryAddress: s.cfg.DiscoveryAddress,
		})
		if err != nil {
			s.log.Error("failed to create logservice client", zap.Error(err))
			return err
		}
		s.logClient = logClient
		return nil
	}
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			panic("failed to create logservice client")

		default:
			if err := createFn(); err != nil {
				time.Sleep(time.Second * 3)
				continue
			}
			return
		}
	}
}

// TODO(liubo): what to do when the chan is full?
func (s *dataSync) Append(data []byte) {
	s.allDataQueue.enqueue(data)
}

func (s *dataSync) Close() error {
	if s.HAKeeperClient != nil {
		if err := s.HAKeeperClient.Close(); err != nil {
			s.log.Error("failed to close HAKeeper client", zap.Error(err))
		}
	}
	// close the queues.
	s.allDataQueue.close()
	s.syncDataQueue.close()
	return nil
}

func (s *dataSync) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			data, releaseFn, err := s.allDataQueue.dequeue(ctx)
			if err != nil {
				s.log.Error("failed to dequeue data", zap.Error(err))
				return
			}
			s.appendData(data.([]byte))
			if releaseFn != nil {
				releaseFn()
			}
		}
	}
}

// adjustLeaseHolderID updates the leaseholder ID of the data.
// The original leaseholder ID is replica ID of TN, set it to 0.
func adjustLeaseholderID(old []byte) {
	// headerSize is 4.
	binaryEnc.PutUint64(old[4:], 0)
}

func (s *dataSync) prepareAppend() {
	if s.HAKeeperClient == nil {
		s.createHAKeeperClient(s.cfg)
	}
	if s.logClient == nil {
		s.createLogClient()
	}
}

func (s *dataSync) appendData(data []byte) {
	s.prepareAppend()
	ctx, cancel := context.WithTimeout(s.ctx, time.Second*5)
	defer cancel()
	adjustLeaseholderID(data)
	_, err := s.logClient.Append(ctx, pb.LogRecord{Data: data})
	if err != nil {
		s.log.Error("liubo: append error", zap.Error(err))
	}
}
