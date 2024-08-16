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

package logservice

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

type syncShard struct {
	ctx     context.Context
	log     *log.MOLogger
	stopper *stopper.Stopper
	uuid    string

	cfg HAKeeperClientConfig
	// HAKeeperClient is used to get log shard ID from HAKeeper
	HAKeeperClient LogHAKeeperClient

	// logClient is used to send data to log shard.
	logClient Client

	dataC     chan []byte
	latestLSN uint64
}

func newSyncShard(
	stopper *stopper.Stopper,
	log *log.MOLogger, uuid string, cfg HAKeeperClientConfig,
) (*syncShard, error) {
	ss := &syncShard{
		ctx:     context.Background(),
		stopper: stopper,
		log:     log,
		uuid:    uuid,
		cfg:     cfg,
		dataC:   make(chan []byte, 10240),
	}
	ss.log.Info("liubo: hakeeper cfg", zap.Any("cfg", cfg))
	if err := ss.stopper.RunNamedTask("data-syncer", ss.start); err != nil {
		return nil, err
	}
	return ss, nil
}

func (s *syncShard) createHAKeeperClient(cfg HAKeeperClientConfig) {
	createFn := func() error {
		ctx, cancel := context.WithTimeout(s.ctx, time.Second*5)
		defer cancel()
		cc, err := NewLogHAKeeperClient(ctx, s.uuid, cfg)
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

func (s *syncShard) createLogClient() {
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
		logClient, err := NewClient(ctx, s.uuid, ClientConfig{
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

func (s *syncShard) close() error {
	if s.HAKeeperClient != nil {
		if err := s.HAKeeperClient.Close(); err != nil {
			s.log.Error("failed to close HAKeeper client", zap.Error(err))
		}
	}
	close(s.dataC)
	return nil
}

// TODO(liubo): what to do when the chan is full?
func (s *syncShard) enqueue(data []byte) {
	s.dataC <- data
}

func (s *syncShard) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case data := <-s.dataC:
			s.appendData(data)
		}
	}
}

// adjustLeaseHolderID updates the leaseholder ID of the data.
// The original leaseholder ID is replica ID of TN, set it to 0.
func adjustLeaseholderID(old []byte) {
	binaryEnc.PutUint64(old[headerSize:], 0)
}

func (s *syncShard) appendData(data []byte) {
	if s.HAKeeperClient == nil {
		s.createHAKeeperClient(s.cfg)
	}
	if s.logClient == nil {
		logutil.Infof("liubo: log client is nil")
		s.createLogClient()
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Second*5)
	defer cancel()
	adjustLeaseholderID(data)
	s.log.Info("liubo: append data",
		zap.Uint32("type", binaryEnc.Uint32(data)),
		zap.Uint64("lease", binaryEnc.Uint64(data[headerSize:])),
	)
	_, err := s.logClient.Append(ctx, pb.LogRecord{Data: data})
	if err != nil {
		s.log.Error("liubo: append error", zap.Error(err))
	}
}
