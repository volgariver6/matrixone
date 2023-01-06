// Copyright 2022 Matrix Origin
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
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice/connpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	benchmarkTestDirname = "log_benchmark_dir"
	defaultName          = defines.LocalFileServiceName
	exportSnapshotDir    = "exported-snapshot"
)

func retryWithTimeout(timeoutDuration time.Duration, fn func() (shouldReturn bool)) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return moerr.NewInternalError(ctx, "retry timeout")
		default:
			if fn() {
				return nil
			}
		}
	}
}

func createClientFactory(c ClientConfig) connpool.Factory {
	return func() (connpool.IConn, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return NewClient(ctx, ClientConfig{
			ReadOnly:         false,
			LogShardID:       c.LogShardID,
			DNReplicaID:      c.DNReplicaID,
			ServiceAddresses: c.ServiceAddresses,
			MaxMessageSize:   100 * 1024 * 1024,
		})
	}
}

func createFileService(dataDir string) (*fileservice.FileServices, error) {
	// create all services
	fileServices := [1]fileservice.Config{
		{
			Name:    "LOCAL",
			Backend: "Disk",
			DataDir: dataDir,
		},
	}
	services := make([]fileservice.FileService, 0, len(fileServices))
	for _, config := range fileServices {
		service, err := fileservice.NewFileService(config)
		if err != nil {
			return nil, err
		}
		services = append(services, service)
	}
	// create FileServices
	fs, err := fileservice.NewFileServices(
		defaultName,
		services...,
	)
	if err != nil {
		return nil, err
	}
	// validate default name
	_, err = fileservice.Get[fileservice.FileService](fs, defaultName)
	if err != nil {
		return nil, err
	}
	// ensure local exists
	_, err = fileservice.Get[fileservice.FileService](fs, defines.LocalFileServiceName)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func singleServiceTest(b *testing.B, fn func(*testing.B, context.Context, Client), parallel int) {
	moruntime.SetupProcessLevelRuntime(moruntime.DefaultRuntime())
	defer leaktest.AfterTest(b)()
	cfg := getServiceTestConfig()
	cfg.DataDir = benchmarkTestDirname
	// enlarge this parameter
	cfg.LogDBBufferSize = 1024 * 1024 * 128
	cfg.RPC.MaxMessageSize = 1024 * 1024 * 400
	cfg.FS = vfs.Default
	cfg.UseTeeLogDB = false
	fs, err := createFileService(filepath.Join(benchmarkTestDirname, "local"))
	require.NoError(b, err)
	defer func() {
		if err := cfg.FS.RemoveAll(benchmarkTestDirname); err != nil {
			b.Fatalf("failed to remove dir %v", err)
		}
		if err := cfg.FS.RemoveAll(exportSnapshotDir); err != nil {
			b.Fatalf("failed to remove dir %v", err)
		}
	}()

	service, err := NewService(cfg,
		fs,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(b, err)
	defer func() {
		require.NoError(b, service.Close())
	}()

	init := make(map[uint64]string)
	init[2] = service.ID()
	require.NoError(b, service.store.startReplica(1, 2, init, false))

	scfg := getClientConfig(false)

	cc := connpool.Config{
		MaxSize:      2000,
		InitSize:     parallel,
		AllowOverUse: true,
		InitSync:     true,
	}
	pool := connpool.NewConnPool(cc, "benchmark", createClientFactory(scfg))

	b.ResetTimer()
	b.SetParallelism(parallel / runtime.GOMAXPROCS(0))
	b.RunParallel(func(pb *testing.PB) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		c, err := pool.Acquire(ctx)
		if err != nil {
			panic(err)
		}
		for pb.Next() {
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				fn(b, ctx, c.RealConn().(Client))
			}()
		}
		pool.Release(c)
	})
	b.StopTimer()

	if err := pool.Close(); err != nil {
		panic(err)
	}
}

func multiServiceTest(b *testing.B, fn func(*testing.B, context.Context, Client), parallel int) {
	defer leaktest.AfterTest(b)()
	nodeCount := 3
	shardCount := 1
	configs := make([]Config, 0)
	services := make([]*Service, 0)
	for i := 0; i < nodeCount; i++ {
		fs, err := createFileService(filepath.Join(fmt.Sprintf("%s-%d", benchmarkTestDirname, i), "local"))
		require.NoError(b, err)
		cfg := Config{
			FS:             vfs.Default,
			UUID:           uuid.New().String(),
			DeploymentID:   1,
			RTTMillisecond: 200,
			DataDir:        fmt.Sprintf("%s-%d", benchmarkTestDirname, i),
			ServiceAddress: fmt.Sprintf("127.0.0.1:%d", 51000+10*i),
			RaftAddress:    fmt.Sprintf("127.0.0.1:%d", 51000+10*i+1),
			GossipAddress:  fmt.Sprintf("127.0.0.1:%d", 51000+10*i+2),
			GossipSeedAddresses: []string{
				"127.0.0.1:51002",
				"127.0.0.1:51012",
				"127.0.0.1:51022",
			},
			DisableWorkers:  true,
			LogDBBufferSize: 1024 * 1024 * 128,
		}
		cfg.GossipProbeInterval.Duration = 350 * time.Millisecond
		configs = append(configs, cfg)
		logger := logutil.GetGlobalLogger().Named("log-service").With(zap.String("uuid", cfg.UUID))
		moruntime.SetupProcessLevelRuntime(moruntime.NewRuntime(metadata.ServiceType_LOG, cfg.UUID, logger))
		service, err := NewService(cfg,
			fs,
			WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
				return true
			}),
			WithLogger(logger),
		)
		require.NoError(b, err)
		services = append(services, service)
	}
	defer func() {
		testLogger.Info("going to close all services")
		var wg sync.WaitGroup
		for i, s := range services {
			if s != nil {
				idx := i
				selected := s
				wg.Add(1)
				go func() {
					require.NoError(b, selected.Close())
					if err := configs[idx].FS.RemoveAll(fmt.Sprintf("%s-%d", benchmarkTestDirname, idx)); err != nil {
						panic(fmt.Sprintf("failed to remove dir %v", err))
					}
					wg.Done()
					testLogger.Info("closed a service")
				}()
			}
		}
		if err := configs[0].FS.RemoveAll(exportSnapshotDir); err != nil {
			b.Fatalf("failed to remove dir %v", err)
		}
		wg.Wait()
		time.Sleep(time.Second * 2)
	}()
	// start all replicas
	id := uint64(100)
	for i := uint64(0); i < uint64(shardCount); i++ {
		shardID := i + 1
		r1 := id
		r2 := id + 1
		r3 := id + 2
		id += 3
		replicas := make(map[uint64]dragonboat.Target)
		replicas[r1] = services[i*3].ID()
		replicas[r2] = services[i*3+1].ID()
		replicas[r3] = services[i*3+2].ID()
		require.NoError(b, services[i*3+0].store.startReplica(shardID, r1, replicas, false))
		require.NoError(b, services[i*3+1].store.startReplica(shardID, r2, replicas, false))
		require.NoError(b, services[i*3+2].store.startReplica(shardID, r3, replicas, false))
	}
	wait := func() {
		time.Sleep(50 * time.Millisecond)
	}
	// check & wait all leaders to be elected and known to all services
	cci := uint64(0)
	iterations := 1000
	for retry := 0; retry < iterations; retry++ {
		notReady := 0
		for i := 0; i < nodeCount; i++ {
			shardID := uint64(i/3 + 1)
			service := services[i]
			info, ok := service.getShardInfo(shardID)
			if !ok || info.LeaderID == 0 {
				notReady++
				wait()
				continue
			}
			if shardID == 1 && info.Epoch != 0 {
				cci = info.Epoch
			}
		}
		if notReady <= 1 {
			break
		}
		require.True(b, retry < iterations-1)
	}
	require.True(b, cci != 0)

	cf := ClientConfig{
		ReadOnly:         false,
		LogShardID:       1,
		DNReplicaID:      10,
		ServiceAddresses: []string{"127.0.0.1:51000", "127.0.0.1:51010", "127.0.0.1:51020"},
		MaxMessageSize:   defaultMaxMessageSize,
	}

	cc := connpool.Config{
		MaxSize:      2000,
		InitSize:     parallel,
		AllowOverUse: true,
		InitSync:     true,
	}
	pool := connpool.NewConnPool(cc, "benchmark", createClientFactory(cf))

	b.ResetTimer()
	b.SetParallelism(parallel / runtime.GOMAXPROCS(0))
	b.RunParallel(func(pb *testing.PB) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		c, err := pool.Acquire(ctx)
		if err != nil {
			panic(err)
		}
		for pb.Next() {
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				fn(b, ctx, c.RealConn().(Client))
			}()
		}
		pool.Release(c)
	})
	b.StopTimer()

	if err := pool.Close(); err != nil {
		panic(err)
	}
}

func benchmarkSingleAppend(b *testing.B, sz int, parallel int) {
	fn := func(b *testing.B, ctx context.Context, c Client) {
		rec := c.GetLogRecord(sz)
		e := retryWithTimeout(time.Minute, func() (shouldReturn bool) {
			ctx1, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := c.Append(ctx1, rec)
			cancel()
			return err == nil
		})
		require.NoError(b, e)
		b.SetBytes(int64(sz) + 12)
	}
	singleServiceTest(b, fn, parallel)
}

func benchmarkMultiAppend(b *testing.B, sz int, parallel int) {
	fn := func(b *testing.B, ctx context.Context, c Client) {
		rec := c.GetLogRecord(sz)
		e := retryWithTimeout(time.Minute, func() (shouldReturn bool) {
			ctx1, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := c.Append(ctx1, rec)
			cancel()
			return err == nil
		})
		require.NoError(b, e)
		b.SetBytes(int64(sz) + 12)
	}
	multiServiceTest(b, fn, parallel)
}

func BenchmarkSingle16K400P(b *testing.B) {
	benchmarkSingleAppend(b, 16*1024, 400)
}

func BenchmarkSingle32K400P(b *testing.B) {
	benchmarkSingleAppend(b, 32*1024, 400)
}

func BenchmarkSingle64K400P(b *testing.B) {
	benchmarkSingleAppend(b, 64*1024, 400)
}

func BenchmarkSingle128K400P(b *testing.B) {
	benchmarkSingleAppend(b, 128*1024, 400)
}

func BenchmarkSingle256K400P(b *testing.B) {
	benchmarkSingleAppend(b, 256*1024, 400)
}

func BenchmarkSingle16K800P(b *testing.B) {
	benchmarkSingleAppend(b, 16*1024, 800)
}

func BenchmarkSingle32K800P(b *testing.B) {
	benchmarkSingleAppend(b, 32*1024, 800)
}

func BenchmarkSingle64K800P(b *testing.B) {
	benchmarkSingleAppend(b, 64*1024, 800)
}

func BenchmarkSingle128K800P(b *testing.B) {
	benchmarkSingleAppend(b, 128*1024, 800)
}

func BenchmarkSingle256K800P(b *testing.B) {
	benchmarkSingleAppend(b, 256*1024, 800)
}

func BenchmarkMulti16K400P(b *testing.B) {
	benchmarkMultiAppend(b, 16*1024, 400)
}

func BenchmarkMulti32K400P(b *testing.B) {
	benchmarkMultiAppend(b, 32*1024, 400)
}

func BenchmarkMulti64K400P(b *testing.B) {
	benchmarkMultiAppend(b, 64*1024, 400)
}

func BenchmarkMulti128K400P(b *testing.B) {
	benchmarkMultiAppend(b, 128*1024, 400)
}

func BenchmarkMulti256K400P(b *testing.B) {
	benchmarkMultiAppend(b, 256*1024, 400)
}
