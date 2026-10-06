package twin

import "sort"

// Connect attaches a local link for deviceID. Repeated connects with the same
// connection id are deduplicated and return the existing connection without
// re-sending the ledger. A connect with a new id replaces the old link: the
// stale connection is closed, and every outstanding (queued, or
// delivered-but-unacknowledged) command is replayed in per-device sequence
// order.
func (s *Service) Connect(deviceID, connID string) (*Connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deviceLocked(deviceID)
	if err != nil {
		return nil, err
	}
	if cur := s.liveConn[deviceID]; cur != nil {
		if cur.id == connID {
			s.logf("connect[%s]: duplicate connect id=%q ignored, no redelivery", deviceID, connID)
			return cur, nil
		}
		s.logf("connect[%s]: replacing stale connection %q with %q", deviceID, cur.id, connID)
		// Drain accounting: anything still buffered on the stale link was
		// never consumed by the device. Replay into the new link below, and
		// detach the old one.
		s.detachLocked(cur)
	}
	ch := make(chan *Command, 64)
	conn := &Connection{id: connID, devID: deviceID, Ch: ch, ch: ch, service: s}
	s.liveConn[deviceID] = conn
	d.Online = true

	outstanding := s.outstandingLocked(d)
	s.logf("connect[%s]: id=%q online, %d outstanding command(s) to replay", deviceID, connID, len(outstanding))

	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	// Replay order is strictly per-device Seq order.
	for _, c := range outstanding {
		s.trySendLocked(conn, d, c)
	}
	return conn, nil
}

// RetryOutstanding re-attempts delivering any outstanding commands over the
// live connection. It lets tests (and a real scheduler) drive retries
// deterministically without real waits or timers. Commands whose attempt
// budget is exhausted are moved to expired instead of sent.
func (s *Service) RetryOutstanding(deviceID string) (resent, expired []int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deviceLocked(deviceID)
	if err != nil {
		return nil, nil, err
	}
	conn := s.liveConn[deviceID]
	for _, c := range s.outstandingLocked(d) {
		if c.Attempts >= MaxAttempts {
			c.Status = CmdExpired
			expired = append(expired, c.Seq)
			continue
		}
		if conn != nil && s.trySendLocked(conn, d, c) {
			resent = append(resent, c.Seq)
		}
	}
	s.logf("retry[%s]: resent=%v expired=%v (online=%v)", deviceID, resent, expired, conn != nil)
	if err := s.persistLocked(); err != nil {
		return nil, nil, err
	}
	return resent, expired, nil
}

func (s *Service) disconnect(deviceID, connID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.liveConn[deviceID]
	if cur == nil {
		return nil
	}
	if cur.id != connID {
		// A newer connection already replaced this one; never tear it down.
		s.logf("disconnect[%s]: stale id=%q close ignored (current=%q)", deviceID, connID, cur.id)
		return nil
	}
	s.detachLocked(cur)
	if d, ok := s.devices[deviceID]; ok {
		d.Online = false
		// Deliveries that were never acknowledged are unfinished: requeue
		// them so reconnect/retry dispatches each exactly once, instead of
		// the device seeing a stale copy on the discarded link plus a replay.
		requeued := 0
		for _, c := range d.Commands {
			if c.Status == CmdDelivered {
				c.Status = CmdQueued
				requeued++
			}
		}
		s.logf("disconnect[%s]: id=%q offline; %d command(s) remain outstanding (%d requeued)",
			deviceID, connID, len(s.outstandingLocked(d)), requeued)
		_ = requeued
		return s.persistLocked()
	}
	return nil
}

func (s *Service) detachLocked(conn *Connection) {
	if cur := s.liveConn[conn.devID]; cur == conn {
		delete(s.liveConn, conn.devID)
	}
	// Closing a replaced channel can only race with the device side that
	// owned the old handle; the service itself keeps using liveConn.
}

// outstandingLocked returns queued and delivered-but-unfinished commands in
// Seq order. Superseded/executed/expired commands never come back.
func (s *Service) outstandingLocked(d *Device) []*Command {
	var out []*Command
	for _, c := range d.Commands {
		if c.Status == CmdQueued || c.Status == CmdDelivered {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// trySendLocked delivers one command without blocking. On success the command
// becomes delivered and consumes one attempt; a full/closed channel leaves it
// pending for a later retry and cannot stall other devices.
func (s *Service) trySendLocked(conn *Connection, d *Device, c *Command) bool {
	select {
	case conn.ch <- c:
		c.Status = CmdDelivered
		c.Attempts++
		s.logf("deliver[%s]: seq=%d attempt=%d delta=%s", d.ID, c.Seq, c.Attempts, describeState(c.Delta))
		if err := s.persistLocked(); err != nil {
			s.logf("deliver[%s]: persist error: %v", d.ID, err)
		}
		return true
	default:
		s.logf("deliver[%s]: seq=%d deferred (link busy), will retry", d.ID, c.Seq)
		return false
	}
}
