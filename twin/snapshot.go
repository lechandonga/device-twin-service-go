package twin

import "encoding/json"

// cloneSnapshotForStore deep-copies a snapshot by round-tripping it through
// JSON. Store implementations therefore cannot accidentally alias live state,
// and tombstones survive the round trip.
func cloneSnapshotForStore(snap *Snapshot) (*Snapshot, error) {
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	var cp Snapshot
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	if cp.Devices == nil {
		cp.Devices = []*Device{}
	}
	for _, d := range cp.Devices {
		d.Desired = asMap(restoreTombstones(d.Desired).(map[string]any))
		d.Reported = asMap(restoreTombstones(d.Reported).(map[string]any))
		for _, c := range d.Commands {
			if c.Delta != nil {
				c.Delta = asMap(restoreTombstones(c.Delta).(map[string]any))
			}
		}
		if d.AppliedRequests == nil {
			d.AppliedRequests = map[string]string{}
		}
	}
	return &cp, nil
}

func asMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}
