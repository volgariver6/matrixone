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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"go.uber.org/zap"
)

type truncationConfig struct {
	interval    time.Duration
	haKeeperCfg logservice.HAKeeperClientConfig
}

type truncation struct {
	common
	// cfg is truncation module configuration.
	cfg truncationConfig
	// the client to HAKeeper server.
	haKeeperClient logservice.LogHAKeeperClient
	// the client to logservice server.
	client logservice.Client
	// syncedLSN the LSN that has been synced.
	syncedLSN *atomic.Uint64
}

type truncationOption func(*truncation)

func withTruncateInterval(v time.Duration) truncationOption {
	return func(t *truncation) {
		t.cfg.interval = v
	}
}

func withTruncationHAKeeperClientConfig(v logservice.HAKeeperClientConfig) truncationOption {
	return func(t *truncation) {
		t.cfg.haKeeperCfg = v
	}
}

func newTruncation(
	common common, syncedLSN *atomic.Uint64, opts ...truncationOption,
) Worker {
	if syncedLSN == nil {
		panic("syncedLSN is nil")
	}
	t := &truncation{
		common:    common,
		syncedLSN: syncedLSN,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *truncation) prepareTruncate(ctx context.Context) {
	if t.haKeeperClient == nil {
		t.haKeeperClient = createHAKeeperClient(ctx, t.sid, t.cfg.haKeeperCfg)
	}
	if t.client == nil {
		t.client = createLogServiceClient(
			ctx,
			t.sid,
			t.haKeeperClient,
			t.cfg.haKeeperCfg,
		)
	}
}

func (t *truncation) truncate(ctx context.Context) error {
	// Prepare the client instance.
	t.prepareTruncate(ctx)

	lsn := t.syncedLSN.Load()
	if lsn == 0 {
		t.log.Warn("the synced LSN is zero")
		return nil
	}

	// Update the truncate LSN.
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	if err := t.client.Truncate(ctx, lsn); err != nil {
		return err
	}
	return nil
}

func (t *truncation) Start(ctx context.Context) {
	timer := time.NewTimer(t.cfg.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return

		case <-timer.C:
			if err := t.truncate(ctx); err != nil {
				t.log.Error("failed to truncate log", zap.Error(err))
			}
			timer.Reset(t.cfg.interval)
		}
	}
}

func (t *truncation) Close() {}
