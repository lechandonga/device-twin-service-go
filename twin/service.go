// Package twin provides a locally runnable device-twin (shadow) service.
//
// The service keeps one versioned shadow.Document per device, computes
// desired-vs-reported deltas, queues them together with commands while a
// device is offline, and replays them in per-device order on reconnect.
// All state is persisted to a directory as atomic JSON snapshots so a
// process restart resumes with monotonically increasing versions and no
// half-applied updates are ever visible.
package twin

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/lechandonga/device-twin-service-go/shadow"
)

// EntryStatus is the lifecycle state of a queued delta or command.
type EntryStatus string

const (
	StatusPending    EntryStatus = "pending"    // queued, not yet delivered
	StatusSent       EntryStatus = "sent"       // delivered, awaiting ack
	StatusAcked      EntryStatus = "acked"      // acknowledged by the device
	StatusSuperseded EntryStatus = "superseded" // overtaken by a newer write or already reflected in reported state
	StatusExpired    EntryStatus = "expired"    // delivery attempts exhausted
)

// EntryKind distinguishes delta deliveries from command deliveries.
type EntryKind string

const (
	KindDelta   EntryKind = "delta"
	KindCommand EntryKind = "command"
)

// Entry is one unit of offline-capable delivery. Deltas carry a merge patch
// computed from the shadow diff; commands carry an opaque payload.
type Entry struct {
	Kind     EntryKind      `json:"kind"`
	Seq      int64          `json:"seq"`
	Version  int64          `json:"version"` // shadow version that produced a delta
	Patch    map[string]any `json:"patch,omitempty"`
	Command  string         `json:"command,omitempty"`
	Payload  map[string]any `json:"payload,omitempty"`
	Status   EntryStatus    `json:"status"`
	Attempts int            `json:"attempts"`
}

// AckResult explains the outcome of acknowledging a delivery.
type AckResult string

const (
	AckApplied    AckResult = "applied"    // first ack, entry completed
	AckDuplicate  AckResult = "duplicate"  // already acked, no-op
	AckSuperseded AckResult = "superseded" // entry was overtaken before ack
	AckExpired    AckResult = "expired"    // entry exhausted its attempts
	AckUnknown    AckResult = "unknown"    // no such sequence number
)

var (
	// ErrAlreadyConnected is returned when a device connects twice without
	// disconnecting; the duplicate connection receives no replay.
	ErrAlreadyConnected = errors.New("twin: device already connected")
	// ErrNotConnected is returned when acking on a closed connection.
	ErrNotConnected = errors.New("twin: device not connected")
)

// deviceState is the full recoverable state of one device.
type deviceState struct {
	Doc     shadow.Document `json:"doc"`
	NextSeq int64           `json:"nextSeq"`
	Outbox  []*Entry        `json:"outbox"`
}

func newDeviceState() *deviceState {
	return &deviceState{Doc: *shadow.NewDocument(), NextSeq: 1}
}

// Service is the device-twin service. It is safe for concurrent use.
type Service struct {
	mu          sync.Mutex
	dir         string
	maxAttempts int
	devices     map[string]*deviceState
	connected   map[string]bool
}

// Option configures a Service.
type Option func(*Service)

// WithMaxAttempts caps delivery attempts per entry before it is expired.
func WithMaxAttempts(n int) Option {
	return func(s *Service) {
		if n > 0 {
			s.maxAttempts = n
		}
	}
}

// Open loads (or creates) the service rooted at dir. Recovery is complete
// before Open returns, so no half-updated state is ever observable.
func Open(dir string, opts ...Option) (*Service, error) {
	s := &Service{
		dir:         dir,
		maxAttempts: 3,
		devices:     map[string]*deviceState{},
		connected:   map[string]bool{},
	}
	for _, opt := range opts {
		opt(s)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("twin: recover %s: %w", e.Name(), err)
		}
		var st deviceState
		if err := json.Unmarshal(raw, &st); err != nil {
			return nil, fmt.Errorf("twin: recover %s: %w", e.Name(), err)
		}
		// Entries left in "sent" by a crash were never acked; return them
		// to pending so they are redelivered on the next connection.
		for _, en := range st.Outbox {
			if en.Status == StatusSent {
				en.Status = StatusPending
			}
		}
		s.devices[strings.TrimSuffix(e.Name(), ".json")] = &st
	}
	return s, nil
}

func (s *Service) stateLocked(device string) *deviceState {
	st, ok := s.devices[device]
	if !ok {
		st = newDeviceState()
		s.devices[device] = st
	}
	return st
}

// Get returns a deep copy of the device's shadow document.
func (s *Service) Get(device string) shadow.Document {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.stateLocked(device).Doc.Clone()
}

// UpdateDesired merges patch into the desired section using
// compare-and-swap on baseVersion, then queues the resulting delta for
// delivery. Returns the new version or a *shadow.ConflictError.
func (s *Service) UpdateDesired(device string, patch map[string]any, baseVersion int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stateLocked(device)
	version, err := st.Doc.ApplyDesired(device, patch, baseVersion)
	if err != nil {
		return version, err
	}
	s.enqueueDeltaLocked(device, st, version)
	return version, s.persistLocked(device, st)
}

// UpdateReported merges patch into the reported section using
// compare-and-swap on baseVersion. Deltas already reflected by the new
// reported state are marked superseded so they are never delivered.
func (s *Service) UpdateReported(device string, patch map[string]any, baseVersion int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stateLocked(device)
	version, err := st.Doc.ApplyReported(device, patch, baseVersion)
	if err != nil {
		return version, err
	}
	s.pruneLocked(st)
	return version, s.persistLocked(device, st)
}

// SendCommand queues a command for the device. Commands persist across
// restarts and are delivered in per-device sequence order.
func (s *Service) SendCommand(device, command string, payload map[string]any) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stateLocked(device)
	entry := &Entry{
		Kind:    KindCommand,
		Seq:     st.NextSeq,
		Version: st.Doc.Version,
		Command: command,
		Payload: payload,
		Status:  StatusPending,
	}
	st.NextSeq++
	st.Outbox = append(st.Outbox, entry)
	return entry.Seq, s.persistLocked(device, st)
}

// enqueueDeltaLocked computes the current desired-vs-reported diff and
// queues it. Any still-open delta is superseded first: only the newest
// diff is worth delivering, which guarantees no stale value can overwrite
// a newer one on the device.
func (s *Service) enqueueDeltaLocked(device string, st *deviceState, version int64) {
	for _, en := range st.Outbox {
		if en.Kind == KindDelta && (en.Status == StatusPending || en.Status == StatusSent) {
			en.Status = StatusSuperseded
		}
	}
	patch := shadow.Diff(st.Doc.Desired, st.Doc.Reported)
	if patch == nil {
		return
	}
	st.Outbox = append(st.Outbox, &Entry{
		Kind:    KindDelta,
		Seq:     st.NextSeq,
		Version: version,
		Patch:   patch,
		Status:  StatusPending,
	})
	st.NextSeq++
}

// pruneLocked marks open deltas superseded when the reported state already
// satisfies them (e.g. the device applied the change before delivery).
func (s *Service) pruneLocked(st *deviceState) {
	remaining := shadow.Diff(st.Doc.Desired, st.Doc.Reported)
	if remaining != nil {
		return
	}
	for _, en := range st.Outbox {
		if en.Kind == KindDelta && (en.Status == StatusPending || en.Status == StatusSent) {
			en.Status = StatusSuperseded
		}
	}
}

// Connect attaches the device and returns its pending entries in sequence
// order, marking them sent. A duplicate connection while already connected
// fails with ErrAlreadyConnected and delivers nothing, so retries and
// double connects never cause duplicate deliveries.
func (s *Service) Connect(device string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connected[device] {
		return nil, ErrAlreadyConnected
	}
	s.connected[device] = true
	st := s.stateLocked(device)
	var out []Entry
	for _, en := range st.Outbox {
		if en.Status != StatusPending {
			continue
		}
		en.Attempts++
		if en.Attempts > s.maxAttempts {
			en.Status = StatusExpired
			continue
		}
		en.Status = StatusSent
		out = append(out, *cloneEntry(en))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, s.persistLocked(device, st)
}

// Disconnect detaches the device. Entries that were sent but never acked
// return to pending and will be redelivered (up to the attempt cap) on the
// next connection.
func (s *Service) Disconnect(device string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.connected[device] {
		return ErrNotConnected
	}
	delete(s.connected, device)
	st := s.stateLocked(device)
	for _, en := range st.Outbox {
		if en.Status == StatusSent {
			en.Status = StatusPending
		}
	}
	return s.persistLocked(device, st)
}

// Ack acknowledges a delivered entry by sequence number. The result
// distinguishes applied, duplicate, superseded, expired and unknown so
// late or repeated acks are explainable no-ops rather than errors.
func (s *Service) Ack(device string, seq int64) (AckResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.connected[device] {
		return AckUnknown, ErrNotConnected
	}
	st := s.stateLocked(device)
	for _, en := range st.Outbox {
		if en.Seq != seq {
			continue
		}
		var res AckResult
		switch en.Status {
		case StatusSent:
			en.Status = StatusAcked
			res = AckApplied
		case StatusAcked:
			res = AckDuplicate
		case StatusSuperseded:
			res = AckSuperseded
		case StatusExpired:
			res = AckExpired
		default:
			res = AckUnknown
		}
		return res, s.persistLocked(device, st)
	}
	return AckUnknown, nil
}

// Outbox returns a snapshot of the device's delivery queue, in order.
func (s *Service) Outbox(device string) []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stateLocked(device)
	out := make([]Entry, 0, len(st.Outbox))
	for _, en := range st.Outbox {
		out = append(out, *cloneEntry(en))
	}
	return out
}

func cloneEntry(en *Entry) *Entry {
	raw, _ := json.Marshal(en)
	var out Entry
	_ = json.Unmarshal(raw, &out)
	return &out
}

// persistLocked writes the device state atomically (temp file + rename), so
// a crash can leave either the old or the new snapshot, never a partial one.
func (s *Service) persistLocked(device string, st *deviceState) error {
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, device+".tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, device+".json"))
}
