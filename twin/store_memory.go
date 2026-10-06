package twin

import "sync"

// MemoryStore keeps the snapshot in RAM. It is useful for tests and for
// demonstration runs that do not need restart recovery. It is safe for
// concurrent use.
type MemoryStore struct {
	mu   sync.Mutex
	snap Snapshot
	set  bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) Save(snap *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := cloneSnapshotForStore(snap)
	if err != nil {
		return err
	}
	s.snap = *data
	s.set = true
	return nil
}

func (s *MemoryStore) Load() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.set {
		return &Snapshot{Devices: []*Device{}}, nil
	}
	return cloneSnapshotForStore(&s.snap)
}
