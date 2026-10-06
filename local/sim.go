// Package local provides an in-process simulation of platform and device
// sides talking to the shadow service. There is no real network: the device
// side consumes commands from the connection channel and replies through the
// service, so the whole system runs locally and deterministically.
package local

import (
	"fmt"
	"io"
	"sync"

	"github.com/lechandonga/device-twin-service-go/twin"
)

// DeviceSim is a scriptable device end. It owns a connection id per "boot",
// drains deliveries on demand (no goroutines/timers required), and tracks the
// reported version it has observed so tests stay deterministic.
type DeviceSim struct {
	Service  *twin.Service
	ID       string
	bootSeq  int
	reported int64

	mu   sync.Mutex
	conn *twin.Connection
}

// NewDevice registers the device in the service if needed.
func NewDevice(svc *twin.Service, id string) (*DeviceSim, error) {
	if err := svc.Register(id); err != nil {
		return nil, err
	}
	return &DeviceSim{Service: svc, ID: id}, nil
}

// Boot opens a new connection (a fresh boot/reconnect) and replays the
// backlog. Repeated Boot calls simulate disconnect/reconnect cycles.
func (d *DeviceSim) Boot() (*twin.Connection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bootSeq++
	connID := fmt.Sprintf("boot-%d", d.bootSeq)
	conn, err := d.Service.Connect(d.ID, connID)
	if err != nil {
		return nil, err
	}
	d.conn = conn
	return conn, nil
}

// ReplayConnect repeats the current connection id, exercising duplicate
// connection de-duplication.
func (d *DeviceSim) ReplayConnect() (*twin.Connection, error) {
	d.mu.Lock()
	connID := ""
	if d.conn != nil {
		connID = d.conn.ID()
	}
	d.mu.Unlock()
	if connID == "" {
		return d.Boot()
	}
	return d.Service.Connect(d.ID, connID)
}

// Drain returns all commands currently buffered on the connection, in order.
// It never blocks or sleeps.
func (d *DeviceSim) Drain() []*twin.Command {
	d.mu.Lock()
	conn := d.conn
	d.mu.Unlock()
	if conn == nil {
		return nil
	}
	var got []*twin.Command
	for {
		select {
		case c := <-conn.Ch:
			got = append(got, c)
		default:
			return got
		}
	}
}

// GoOffline closes the current link.
func (d *DeviceSim) GoOffline() error {
	d.mu.Lock()
	conn := d.conn
	d.conn = nil
	d.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// Report sends a versioned reported update and remembers the new version.
func (d *DeviceSim) Report(patch map[string]any, requestID string, ackSeq int64) (*twin.ReportResult, error) {
	res, err := d.Service.Report(twin.ReportRequest{
		DeviceID:               d.ID,
		ExpectedReportedVersion: d.reported,
		Patch:                  patch,
		RequestID:              requestID,
		AckSeq:                 ackSeq,
	})
	if err != nil {
		return nil, err
	}
	d.reported = res.ReportedVersion
	return res, nil
}

// ObservedReportedVersion returns the simulator's current version cursor.
func (d *DeviceSim) ObservedReportedVersion() int64 { return d.reported }

// SetReportedVersion seeds the cursor, e.g. after a process restart.
func (d *DeviceSim) SetReportedVersion(v int64) { d.reported = v }

// logWriter adapts io.Writer to twin.Logger.
type logWriter struct{ w io.Writer }

func (l logWriter) Printf(format string, args ...any) {
	fmt.Fprintf(l.w, format+"\n", args...)
}

// Logger wraps an io.Writer as a twin.Logger.
func Logger(w io.Writer) twin.Logger { return logWriter{w: w} }
