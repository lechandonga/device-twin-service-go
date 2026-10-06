package twin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// FileStore persists snapshots as a single JSON file, written atomically via a
// temporary file in the same directory followed by an os.Rename. A crash
// therefore exposes either the old file or the complete new file, never a
// half-written one. An fsync on the file (and best-effort on the directory)
// makes the write durable before it becomes visible.
type FileStore struct {
	path string
}

// NewFileStore creates a file-backed store at path.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Save(snap *Snapshot) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("twin: create data dir: %w", err)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("twin: encode snapshot: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("twin: create temp snapshot: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("twin: write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("twin: fsync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("twin: close snapshot: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("twin: rename snapshot: %w", err)
	}
	syncDir(dir)
	return nil
}

func (s *FileStore) Load() (*Snapshot, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &Snapshot{}, nil
		}
		return nil, fmt.Errorf("twin: read snapshot: %w", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("twin: decode snapshot: %w", err)
	}
	if snap.Devices == nil {
		snap.Devices = []*Device{}
	}
	for _, d := range snap.Devices {
		if d.Desired == nil {
			d.Desired = map[string]any{}
		} else {
			d.Desired = restoreTombstones(d.Desired).(map[string]any)
		}
		if d.Reported == nil {
			d.Reported = map[string]any{}
		} else {
			d.Reported = restoreTombstones(d.Reported).(map[string]any)
		}
		for _, c := range d.Commands {
			if c.Delta != nil {
				c.Delta = restoreTombstones(c.Delta).(map[string]any)
			}
		}
		if d.AppliedRequests == nil {
			d.AppliedRequests = map[string]string{}
		}
	}
	return &snap, nil
}

// syncDir best-effort fsyncs the directory so the rename is durable. It is
// harmless on platforms where directory sync is unsupported.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
