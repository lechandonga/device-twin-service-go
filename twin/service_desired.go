package twin

import "sort"

// UpdateDesired applies a versioned desired-state write.
func (s *Service) UpdateDesired(req DesiredRequest) (*DesiredResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deviceLocked(req.DeviceID)
	if err != nil {
		return nil, err
	}
	fp := fingerprint("desired", req.ExpectedDesiredVersion, req.Patch)
	if req.RequestID != "" {
		if prev, ok := d.AppliedRequests[req.RequestID]; ok {
			if prev != fp {
				return nil, conflict(ConflictDuplicateRequest, req.ExpectedDesiredVersion, d.DesiredVersion, req.RequestID)
			}
			s.logf("desired[%s]: duplicate request %q with identical payload, idempotent success", req.DeviceID, req.RequestID)
			return s.existingDesiredResultLocked(d), nil
		}
	}
	if err := checkVersion(req.ExpectedDesiredVersion, d.DesiredVersion, "desired"); err != nil {
		return nil, err
	}

	// 1. Merge and bump version.
	deepMerge(d.Desired, req.Patch)
	d.DesiredVersion++
	if req.RequestID != "" {
		d.AppliedRequests[req.RequestID] = fp
	}

	// 2. Earlier queued commands touching any path also present in this patch
	//    are stale: the new dispatch below carries the fresher value.
	superseded := s.supersedeOverlappingLocked(d, req.Patch)

	// 3. Generate one dispatch from desired-vs-reported.
	result := &DesiredResult{DesiredVersion: d.DesiredVersion}
	delta := diff(d.Desired, d.Reported)
	if len(delta) == 0 {
		s.logf("desired[%s]: applied v%d patch=%s, no delta vs reported%s",
			req.DeviceID, d.DesiredVersion, describeState(req.Patch), supersededNote(superseded))
	} else {
		cmd := &Command{
			Seq:                d.NextSeq + 1,
			BaseDesiredVersion: d.DesiredVersion,
			Delta:              delta,
			Status:             CmdQueued,
		}
		d.NextSeq = cmd.Seq
		d.Commands = append(d.Commands, cmd)
		result.CommandSeq = cmd.Seq
		s.logf("desired[%s]: applied v%d patch=%s -> dispatch seq=%d delta=%s (online=%v)%s",
			req.DeviceID, d.DesiredVersion, describeState(req.Patch), cmd.Seq, describeState(delta), d.Online, supersededNote(superseded))
	}

	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	s.dispatchLocked(d)
	return result, nil
}

// existingDesiredResultLocked rebuilds a result for an idempotent replay.
func (s *Service) existingDesiredResultLocked(d *Device) *DesiredResult {
	r := &DesiredResult{DesiredVersion: d.DesiredVersion}
	if n := len(d.Commands); n > 0 {
		r.CommandSeq = d.Commands[n-1].Seq
	}
	return r
}

// supersedeOverlappingLocked marks queued commands stale when the new patch
// writes any leaf path they already carried. Only queued (never delivered)
// commands are superseded; delivered ones must run their normal
// executed/expired lifecycle so the device always gets a definite answer.
func (s *Service) supersedeOverlappingLocked(d *Device, patch map[string]any) []int64 {
	paths := leafPaths(nil, patch)
	if len(paths) == 0 {
		return nil
	}
	var seqs []int64
	for _, c := range d.Commands {
		if c.Status != CmdQueued {
			continue
		}
		if pathsOverlap(leafPaths(nil, c.Delta), paths) {
			c.Status = CmdSuperseded
			seqs = append(seqs, c.Seq)
		}
	}
	return seqs
}

func supersededNote(seqs []int64) string {
	if len(seqs) == 0 {
		return ""
	}
	cp := append([]int64(nil), seqs...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return " superseded_queued=" + intsString(cp)
}

// dispatchLocked hands every outstanding command to the live connection in
// sequence order. Called after a new desired write for an online device.
func (s *Service) dispatchLocked(d *Device) {
	conn := s.liveConn[d.ID]
	if conn == nil {
		return
	}
	for _, c := range s.outstandingLocked(d) {
		s.trySendLocked(conn, d, c)
	}
}
