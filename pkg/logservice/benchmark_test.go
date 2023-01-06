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
	"runtime"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice/connpool"
	"github.com/stretchr/testify/require"
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

// Modify this to connection external logserivce.
func makeClientConfig(readOnly bool) ClientConfig {
	return ClientConfig{
		ReadOnly:    readOnly,
		LogShardID:  1,
		DNReplicaID: 2,
		// Change this connect address.
		ServiceAddresses: []string{"127.0.0.1:32001"},
		MaxMessageSize:   defaultMaxMessageSize,
	}
}

func singleServiceTest(b *testing.B, fn func(*testing.B, context.Context, Client), parallel int) {
	moruntime.SetupProcessLevelRuntime(moruntime.DefaultRuntime())
	scfg := makeClientConfig(false)
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
