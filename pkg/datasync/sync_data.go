package datasync

func (s *syncer) syncData(w *wrappedData) {
	// do the sync things...

	// update the LSN
	if w.lsn > s.syncedLSN.Load() {
		s.syncedLSN.Store(w.lsn)
	}
}
