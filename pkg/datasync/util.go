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
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func createHAKeeperClient(
	ctx context.Context, sid string, cfg logservice.HAKeeperClientConfig,
) logservice.LogHAKeeperClient {
	var c logservice.LogHAKeeperClient
	createFn := func() error {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		client, err := logservice.NewLogHAKeeperClient(ctx, sid, cfg)
		if err != nil {
			logutil.Errorf("failed to create HAKeeper client: %v", err)
			return err
		}
		c = client
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
			return c
		}
	}
}

func createLogServiceClient(
	ctx context.Context,
	sid string,
	haKeeperClient logservice.LogHAKeeperClient,
	cfg logservice.HAKeeperClientConfig,
) logservice.Client {
	var c logservice.Client
	createFn := func() error {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		state, err := haKeeperClient.GetClusterState(ctx)
		if err != nil {
			logutil.Errorf("failed to get cluster details: %v", err)
			return err
		}
		// There must be at least three shards, the first one is for HAKeeper,
		// the second one is for TN, and the last one is for data sync.
		if len(state.ClusterInfo.LogShards) < 3 {
			panic("not enough shards")
		}
		shard := state.ClusterInfo.LogShards[2]
		logClient, err := logservice.NewClient(ctx, sid, logservice.ClientConfig{
			ReadOnly:         false,
			LogShardID:       shard.ShardID,
			ServiceAddresses: cfg.ServiceAddresses,
			DiscoveryAddress: cfg.DiscoveryAddress,
		})
		if err != nil {
			logutil.Errorf("failed to create logservice client: %v", err)
			return err
		}
		c = logClient
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
			return c
		}
	}
}
