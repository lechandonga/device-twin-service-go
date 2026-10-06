package twin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// MaxAttempts bounds how many times one command is handed to a connection.
// Attempts count distinct dispatches (initial send plus redeliveries caused
// by reconnect/retry). After MaxAttempts the command is marked expired: it is
// never silently dropped and never resent infinitely.
const MaxAttempts = 5

// Logger is the minimal logging hook used by the service.
type Logger interface {
	Printf(format string, args ...any)
}

// DesiredRequest is a platform-side, versioned desired-state write.
type DesiredRequest struct {
	DeviceID string
	// ExpectedDesiredVersion is the version the caller observed. Zero means
	// "expect 0" and is valid for the very first write to a device.
	ExpectedDesiredVersion int64
	// Patch is merged into desired with the documented deep-merge rules.
	Patch map[string]any
	// RequestID enables idempotent retries. Empty disables the check.
	RequestID string
}

// DesiredResult describes an accepted desired write.
type DesiredResult struct {
	DesiredVersion int64
	// CommandSeq is the generated dispatch's sequence number, or 0 when
	// desired already matched reported (nothing to deliver).
	CommandSeq int64
}

// ReportRequest is a device-side, versioned reported-state update. It may also
// explicitly acknowledge a command sequence.
type ReportRequest struct {
	DeviceID string
	// ExpectedReportedVersion is the version the device observed.
	ExpectedReportedVersion int64
	Patch                   map[string]any
	RequestID               string
	// AckSeq, when non-zero, explicitly marks the command sequence executed.
	AckSeq int64
}

// ReportResult describes an accepted reported update.
type ReportResult struct {
	ReportedVersion int64
	// ExecutedSeqs lists commands moved to executed by state convergence or
	// AckSeq during this report.
	ExecutedSeqs []int64
}

// Service is the shadow service. All methods are safe for concurrent use.
type Service struct {
	store Store
	log   Logger

	mu       sync.Mutex
	devices  map[string]*Device
	liveConn map[string]*Connection
}

// NewService loads durable state from store. Until Load returns no other
// method is served: restart recovery never exposes a half-loaded view.
func NewService(store Store, log Logger) (*Service, error) {
	snap, err := store.Load()
	if err != nil {
		return nil, err
	}
	s := &Service{
		store:    store,
		log:      log,
		devices:  map[string]*Device{},
		liveConn: map[string]*Connection{},
	}
	for _, d := range snap.Devices {
		if d.AppliedRequests == nil {
			d.AppliedRequests = map[string]string{}
		}
		// A live channel cannot survive a process restart; every device
		// starts offline and must reconnect.
		d.Online = false
		// A "delivered" flag without a live connection is meaningless after a
		// crash: the device never had a chance to acknowledge, so these
		// dispatches return to the queue and are replayed on reconnect.
		for _, c := range d.Commands {
			if c.Status == CmdDelivered {
				c.Status = CmdQueued
			}
		}
		s.devices[d.ID] = d
	}
	s.logf("service: recovered %d device shadow(s) from store", len(s.devices))
	return s, nil
}

// SnapshotView is a point-in-time, detached view of one device for callers.
type SnapshotView struct {
	ID              string
	Desired         map[string]any
	DesiredVersion  int64
	Reported        map[string]any
	ReportedVersion int64
	Online          bool
	Commands        []*Command
}

// Get returns a detached copy; callers may mutate it freely.
func (s *Service) Get(deviceID string) *SnapshotView {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.devices[deviceID]
	if !ok {
		return nil
	}
	v := &SnapshotView{
		ID:              d.ID,
		Desired:         cloneValue(d.Desired).(map[string]any),
		DesiredVersion:  d.DesiredVersion,
		Reported:        cloneValue(d.Reported).(map[string]any),
		ReportedVersion: d.ReportedVersion,
		Online:          d.Online,
		Commands:        make([]*Command, len(d.Commands)),
	}
	for i, c := range d.Commands {
		cc := *c
		cc.Delta = cloneValue(c.Delta).(map[string]any)
		v.Commands[i] = &cc
	}
	return v
}

// Register creates a device shadow with empty states at version 0. It is
// idempotent.
func (s *Service) Register(deviceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.devices[deviceID]; ok {
		return nil
	}
	s.devices[deviceID] = &Device{
		ID:              deviceID,
		Desired:         map[string]any{},
		DesiredVersion:  0,
		Reported:        map[string]any{},
		ReportedVersion: 0,
		NextSeq:         0,
		AppliedRequests: map[string]string{},
	}
	s.logf("service: registered device %q", deviceID)
	return s.persistLocked()
}

func (s *Service) logf(format string, args ...any) {
	if s.log != nil {
	s.log.Printf(format, args...)
	}
}

func (s *Service) deviceLocked(id string) (*Device, error) {
	d, ok := s.devices[id]
	if !ok {
		return nil, fmt.Errorf("twin: device %q is not registered", id)
	}
	return d, nil
}

func checkVersion(expected, current int64, tree string) error {
	if expected < current {
		return conflict(ConflictExpiredVersion, expected, current, tree)
	}
	if expected > current {
		return conflict(ConflictVersionAhead, expected, current, tree)
	}
	return nil
}

// fingerprint hashes the kind, base version and canonical patch so identical
// retries are recognised and different retries conflict.
func fingerprint(kind string, baseVersion int64, patch map[string]any) string {
	data, _ := json.Marshal(struct {
		Kind    string         `json:"kind"`
		Base    int64          `json:"base"`
		Patch   map[string]any `json:"patch"`
	}{kind, baseVersion, patch})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *Service) persistLocked() error {
	snap := &Snapshot{Devices: make([]*Device, 0, len(s.devices))}
	ids := make([]string, 0, len(s.devices))
	for id := range s.devices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		snap.Devices = append(snap.Devices, s.devices[id])
	}
	return s.store.Save(snap)
}
