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
)

type queue interface {
	enqueue(any)
	dequeue(ctx context.Context) (any, func(), error)
	close()
}

type dataQueue struct {
	queue chan []byte
	pool  dataPool
}

func newDataQueue(dataSize int, queueSize int) *dataQueue {
	return &dataQueue{
		queue: make(chan []byte, queueSize),
		pool:  newDataPool(dataSize),
	}
}

func (q *dataQueue) enqueue(data any) {
	orig := data.([]byte)
	v := q.pool.acquire(len(orig)).([]byte)
	copy(v, orig)

	for {
		select {
		case q.queue <- v:
			return

		default:
			if !s3Related(v) {
				// if the queue is full and the data is not s3 related, ignore it.
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (q *dataQueue) dequeue(ctx context.Context) (any, func(), error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()

	case v := <-q.queue:
		return v, func() { q.pool.release(v) }, nil
	}

}

func (q *dataQueue) close() {
	close(q.queue)
}
