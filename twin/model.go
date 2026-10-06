package twin

// CommandStatus is the lifecycle state of a generated delta command.
type CommandStatus string

const (
	// CmdQueued: generated while the device is offline, waiting for a
	// connection; never delivered yet.
	CmdQueued CommandStatus = "queued"
	// CmdDelivered: handed to a live connection, waiting for the device to
	// acknowledge via a reported update or Ack.
	CmdDelivered CommandStatus = "delivered"
	// CmdExecuted: device reported state matching the desired values (or an
	// explicit ack) — the terminal success state.
	CmdExecuted CommandStatus = "executed"
	// CmdSuperseded: a newer desired update overwrote a field this queued
	// command was carrying, so the enqueued payload is stale. The newer
	// desired state is delivered by a later command instead.
	CmdSuperseded CommandStatus = "superseded"
	// CmdExpired: the delivery budget (attempts) ran out before the device
	// acknowledged; the terminal failure state. Nothing is dropped
	// silently — the outcome is retained and queryable.
	CmdExpired CommandStatus = "expired"
)

func (s CommandStatus) terminal() bool {
	return s == CmdExecuted || s == CmdSuperseded || s == CmdExpired
}

// Command is one per-device, per-desired-version delta dispatch.
type Command struct {
	// Seq is the per-device monotonically increasing dispatch sequence.
	Seq int64 `json:"seq"`
	// BaseDesiredVersion is the desired version that produced this delta.
	BaseDesiredVersion int64 `json:"base_desired_version"`
	// Delta is the desired-vs-reported snapshot captured at generation time.
	Delta map[string]any `json:"delta"`
	Status CommandStatus `json:"status"`
	// Attempts counts actual deliveries. Retries stop at MaxAttempts.
	Attempts int `json:"attempts"`
	// AckedReportedVersion, when non-zero, records the reported version that
	// confirmed execution of this command.
	AckedReportedVersion int64 `json:"acked_reported_version,omitempty"`
}

// Device is the persisted shadow state of one device.
type Device struct {
	ID string `json:"id"`

	Desired        map[string]any `json:"desired"`
	DesiredVersion int64          `json:"desired_version"`
	Reported       map[string]any `json:"reported"`
	ReportedVersion int64         `json:"reported_version"`

	// NextSeq continues to increase across restarts and across superseded
	// or expired commands; it never reuses a sequence number.
	NextSeq int64 `json:"next_seq"`

	// Commands is the per-device ordered dispatch ledger.
	Commands []*Command `json:"commands"`

	// Online reflects the persisted connection state. Note: a live channel
	// handle is not serialisable; after restart every device starts offline
	// and the device side must reconnect.
	Online bool `json:"online"`

	// AppliedRequests stores requestId -> payload fingerprint for
	// idempotent replays.
	AppliedRequests map[string]string `json:"applied_requests"`
}

// Snapshot is the whole durable state written atomically per mutation.
type Snapshot struct {
	Devices []*Device `json:"devices"`
}

// Store persists snapshots and restores them at startup. Implementations must
// make writes durable and atomic from the reader's point of view: a crash
// either exposes the previous complete snapshot or the new complete snapshot,
// never a half-written file.
type Store interface {
	Save(snap *Snapshot) error
	Load() (*Snapshot, error)
}
