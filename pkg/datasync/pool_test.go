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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolAcquire(t *testing.T) {
	p := newDataPool(defaultDataSize)
	assert.NotNil(t, p)
	v := p.acquire(200)
	data, ok := v.([]byte)
	assert.True(t, ok)
	assert.Equal(t, 200, len(data))
	assert.Equal(t, defaultDataSize, cap(data))

	v = p.acquire(defaultDataSize + 200)
	data, ok = v.([]byte)
	assert.True(t, ok)
	assert.Equal(t, defaultDataSize+200, len(data))
	assert.Equal(t, defaultDataSize+200, cap(data))
}

func TestPoolRelease(t *testing.T) {
	t.Run("small data", func(t *testing.T) {
		p := newDataPool(defaultDataSize)
		assert.NotNil(t, p)
		v := make([]byte, 100)
		p.release(v)
		r := p.(*bytesPool).pool.Get().([]byte)
		assert.Equal(t, 100, len(r))
	})

	t.Run("large data", func(t *testing.T) {
		p := newDataPool(defaultDataSize)
		assert.NotNil(t, p)
		v := make([]byte, defaultDataSize*2)
		p.release(v)
		r := p.(*bytesPool).pool.Get().([]byte)
		assert.Equal(t, defaultDataSize, len(r))
	})
}
