package twin

import "sort"

// Report applies a device-side reported-state update and converges commands.
func (s *Service) Report(req ReportRequest) (*ReportResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deviceLocked(req.DeviceID)
	if err != nil {
		return nil, err
	}
	fp := fingerprint("reported", req.ExpectedReportedVersion, req.Patch)
	if req.RequestID != "" {
		if prev, ok := d.AppliedRequests[req.RequestID]; ok {
			if prev != fp {
				return nil, conflict(ConflictDuplicateRequest, req.ExpectedReportedVersion, d.ReportedVersion, req.RequestID)
			}
			s.logf("reported[%s]: duplicate request %q identical payload, idempotent no-op", req.DeviceID, req.RequestID)
			return &ReportResult{ReportedVersion: d.ReportedVersion}, nil
		}
	}
	if err := checkVersion(req.ExpectedReportedVersion, d.ReportedVersion, "reported"); err != nil {
		return nil, err
	}

	deepMerge(d.Reported, req.Patch)
	d.ReportedVersion++
	if req.RequestID != "" {
		d.AppliedRequests[req.RequestID] = fp
	}

	result := &ReportResult{ReportedVersion: d.ReportedVersion}

	mark := func(c *Command) {
		c.Status = CmdExecuted
		c.AckedReportedVersion = d.ReportedVersion
		result.ExecutedSeqs = append(result.ExecutedSeqs, c.Seq)
	}

	// Explicit ack wins even if the device's report does not yet contain the
	// target value.
	if req.AckSeq > 0 {
		for _, c := range d.Commands {
			if c.Seq == req.AckSeq && c.Status != CmdExecuted {
				mark(c)
			}
		}
	}

	// State convergence: an outstanding command whose target state is now
	// fully contained in reported is executed. Commands whose delta still
	// diverges remain pending and follow the normal retry/expiry lifecycle.
	for _, c := range d.Commands {
		if c.Status != CmdDelivered && c.Status != CmdQueued {
			continue
		}
		if deltaSatisfied(c.Delta, d.Reported) {
			mark(c)
		}
	}
	sort.Slice(result.ExecutedSeqs, func(i, j int) bool { return result.ExecutedSeqs[i] < result.ExecutedSeqs[j] })

	s.logf("reported[%s]: applied v%d patch=%s executed=%v",
		req.DeviceID, d.ReportedVersion, describeState(req.Patch), result.ExecutedSeqs)

	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return result, nil
}

// deltaSatisfied reports whether every instruction in delta holds in state:
// a Deleted leaf must be absent; any other leaf must be present and equal.
func deltaSatisfied(delta, state map[string]any) bool {
	for key, want := range delta {
		switch w := want.(type) {
		case map[string]any:
			got, ok := state[key].(map[string]any)
			if !ok || !deltaSatisfied(w, got) {
				return false
			}
		default:
			if want == Deleted {
				if _, ok := state[key]; ok {
					return false
				}
				continue
			}
			got, ok := state[key]
			if !ok || !valuesEqual(got, want) {
				return false
			}
		}
}
	return true
}
