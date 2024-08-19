package datasync

import (
	"context"
	"sync/atomic"

	"go.uber.org/zap"
)

type dataSync struct {
	common
	syncDataQueue queue
	syncedLSN     *atomic.Uint64
}

func newDataSync(q queue, lsn *atomic.Uint64) *dataSync {
	return &dataSync{
		syncDataQueue: q,
		syncedLSN:     lsn,
	}
}

func (ds *dataSync) syncData(w *wrappedData) {
	// do the sync things...

	// update the LSN
	if w.lsn > ds.syncedLSN.Load() {
		ds.syncedLSN.Store(w.lsn)
	}
}

func (ds *dataSync) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			w, err := ds.syncDataQueue.dequeue(ctx)
			if err != nil {
				ds.log.Error("failed to dequeue data", zap.Error(err))
				ds.pool.release(w)
				continue
			}
			ds.syncData(w)
			ds.pool.release(w)
		}
	}
}

func (ds *dataSync) Close() {}
