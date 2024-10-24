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

package fileservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cacheservice/client"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/cache"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"go.uber.org/zap"
)

type CacheConfig struct {
	MemoryCapacity       *toml.ByteSize `toml:"memory-capacity" user_setting:"advanced"`
	DiskPath             *string        `toml:"disk-path"`
	DiskCapacity         *toml.ByteSize `toml:"disk-capacity"`
	DiskMinEvictInterval *toml.Duration `toml:"disk-min-evict-interval"`
	DiskEvictTarget      *float64       `toml:"disk-evict-target"`
	RemoteCacheEnabled   bool           `toml:"remote-cache-enabled"`
	RPC                  morpc.Config   `toml:"rpc"`

	CacheClient      client.CacheClient `json:"-"`
	KeyRouterFactory KeyRouterFactory   `json:"-"`
	KeyRouter        KeyRouter          `json:"-"`
	InitKeyRouter    *sync.Once         `json:"-"`
	CacheCallbacks   `json:"-"`

	enableDiskCacheForLocalFS bool // for testing only
}

type CacheCallbacks struct {
	PostGet   []CacheCallbackFunc
	PostSet   []CacheCallbackFunc
	PostEvict []CacheCallbackFunc
}

type CacheCallbackFunc = func(CacheKey, CacheData)

func (c *CacheConfig) setDefaults() {
	if c.MemoryCapacity == nil {
		size := toml.ByteSize(512 << 20)
		c.MemoryCapacity = &size
	}
	if c.DiskCapacity == nil {
		size := toml.ByteSize(8 << 30)
		c.DiskCapacity = &size
	}
	if c.DiskMinEvictInterval == nil {
		c.DiskMinEvictInterval = &toml.Duration{
			Duration: time.Minute * 7,
		}
	}
	if c.DiskEvictTarget == nil {
		target := 0.8
		c.DiskEvictTarget = &target
	}
	c.RPC.Adjust()
}

func (c *CacheConfig) SetRemoteCacheCallback() {
	if !c.RemoteCacheEnabled || c.KeyRouterFactory == nil {
		return
	}
	c.InitKeyRouter = &sync.Once{}
	c.CacheCallbacks.PostSet = append(c.CacheCallbacks.PostSet,
		func(key CacheKey, data CacheData) {
			c.InitKeyRouter.Do(func() {
				c.KeyRouter = c.KeyRouterFactory()
			})
			if c.KeyRouter == nil {
				return
			}
			c.KeyRouter.AddItem(key, gossip.Operation_Set)
		},
	)
	c.CacheCallbacks.PostEvict = append(c.CacheCallbacks.PostEvict,
		func(key CacheKey, data CacheData) {
			c.InitKeyRouter.Do(func() {
				c.KeyRouter = c.KeyRouterFactory()
			})
			if c.KeyRouter == nil {
				return
			}
			c.KeyRouter.AddItem(key, gossip.Operation_Delete)
		},
	)
}

var DisabledCacheConfig = CacheConfig{
	MemoryCapacity: ptrTo[toml.ByteSize](DisableCacheCapacity),
	DiskCapacity:   ptrTo[toml.ByteSize](DisableCacheCapacity),
}

const DisableCacheCapacity = 1

// var DefaultCacheDataAllocator = RCBytesPool
var DefaultCacheDataAllocator = new(bytesAllocator)

// VectorCache caches IOVector
type IOVectorCache interface {
	Read(
		ctx context.Context,
		vector *IOVector,
	) error
	Update(
		ctx context.Context,
		vector *IOVector,
		async bool,
	) error
	Flush()
	//TODO file contents may change, so we still need this s.
	DeletePaths(
		ctx context.Context,
		paths []string,
	) error
}

func readCache(ctx context.Context, cache IOVectorCache, vector *IOVector) error {
	ctx, cancel := context.WithTimeout(ctx, slowCacheReadThreshold)
	defer cancel()
	err := cache.Read(ctx, vector)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			logutil.Warn("cache read exceed deadline",
				zap.Any("err", err),
				zap.Any("cache type", fmt.Sprintf("%T", cache)),
				zap.Any("path", vector.FilePath),
				zap.Any("entries", vector.Entries),
			)
			// safe to ignore
			return nil
		}
		return err
	}
	return nil
}

type CacheKey = pb.CacheKey

// DataCache caches IOEntry.CachedData
type DataCache interface {
	Set(ctx context.Context, key CacheKey, value CacheData)
	Get(ctx context.Context, key CacheKey) (value CacheData, ok bool)
	//TODO file contents may change, so we still need this s.
	DeletePaths(ctx context.Context, paths []string)
	Flush()
	Capacity() int64
	Used() int64
	Available() int64
}

var slowCacheReadThreshold = time.Second * 10
