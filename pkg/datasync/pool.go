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

import "sync"

type dataPool interface {
	acquire(int) any
	release(any)
}

type bytesPool struct {
	pool sync.Pool
}

func newDataPool(size int) dataPool {
	return &bytesPool{
		pool: sync.Pool{
			New: func() any {
				return make([]byte, size)
			},
		},
	}
}

func (p *bytesPool) acquire(size int) any {
	d := p.pool.Get().([]byte)
	if cap(d) < size {
		p.pool.Put(d)
		d = make([]byte, size)
	} else {
		d = d[:size]
	}
	return d
}

func (p *bytesPool) release(data any) {
	v := data.([]byte)
	if cap(v) > defaultDataSize {
		return
	}
	p.pool.Put(v)
}
