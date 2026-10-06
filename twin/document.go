// Package twin implements a local, persistent device shadow service.
//
// A shadow document keeps two independent state trees for each device:
//   - desired:  the state requested by the platform side;
//   - reported: the state last reported by the device side.
//
// Both trees carry their own monotonically increasing version. Every mutating
// request must present the version the caller observed. A request whose
// expected version is behind the current one is rejected with a distinct
// conflict error; the caller can then re-read the document and retry.
package twin

import (
	"fmt"
	"sort"
	"strings"
)

// Deleted is the tombstone value. Assigning Deleted to a key inside an update
// explicitly removes that key from the merged state. It must not be confused
// with nil: nil stores an explicit null, Deleted removes the entry.
//
// Example: {"temp": Deleted} removes "temp"; {"temp": nil} stores null.
// tombstone is the concrete type of Deleted. It has a custom JSON shape
// ({"__deleted__":true}) so persisted deltas keep deletion semantics after a
// restart instead of looking like empty maps.
type tombstone struct{}

func (tombstone) MarshalJSON() ([]byte, error) {
	return []byte(`{"__deleted__":true}`), nil
}

// Deleted is the tombstone value. Assigning Deleted to a key inside an update
// explicitly removes that key from the merged state. It must not be confused
// with nil: nil stores an explicit null, Deleted removes the entry.
//
// Example: {"temp": Deleted} removes "temp"; {"temp": nil} stores null.
var Deleted any = tombstone{}

const deletedMarker = "__deleted__"

// restoreTombstones walks a decoded JSON tree and converts
// {"__deleted__":true} markers back into Deleted.
func restoreTombstones(v any) any {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 1 {
			if b, ok := t[deletedMarker].(bool); ok && b {
				return Deleted
			}
		}
		for k, val := range t {
			t[k] = restoreTombstones(val)
		}
		return t
	case []any:
		for i, val := range t {
			t[i] = restoreTombstones(val)
		}
		return t
	default:
		return v
	}
}

// ConflictKind classifies why a versioned write was rejected.
type ConflictKind string

const (
	// ConflictNone means the write was accepted.
	ConflictNone ConflictKind = ""
	// ConflictExpiredVersion means expectedVersion < current version:
	// the caller based its write on stale state (lost-update protection).
	ConflictExpiredVersion ConflictKind = "expired_version"
	// ConflictVersionAhead means expectedVersion > current version:
	// the caller referenced a version this service has never produced.
	ConflictVersionAhead ConflictKind = "version_ahead"
	// ConflictDuplicateRequest means the requestId was already applied with a
	// different payload. Replays with the SAME payload are idempotent and
	// return the original result.
	ConflictDuplicateRequest ConflictKind = "duplicate_request_id"
)

// ConflictError is returned for every rejected versioned write. The Kind field
// lets callers distinguish the failure modes programmatically.
type ConflictError struct {
	Kind     ConflictKind
	Expected int64
	Current  int64
	Request  string
}

func (e *ConflictError) Error() string {
	switch e.Kind {
	case ConflictExpiredVersion:
		return fmt.Sprintf("twin: write rejected: expected version %d is behind current version %d", e.Expected, e.Current)
	case ConflictVersionAhead:
		return fmt.Sprintf("twin: write rejected: expected version %d is ahead of current version %d", e.Expected, e.Current)
	case ConflictDuplicateRequest:
		return fmt.Sprintf("twin: write rejected: request id %q was already used with a different payload", e.Request)
	default:
		return "twin: write rejected: unknown conflict"
	}
}

// IsConflict reports whether err is any kind of version/idempotency conflict.
func IsConflict(err error) bool {
	_, ok := err.(*ConflictError)
	return ok
}

func conflict(kind ConflictKind, expected, current int64, requestID string) *ConflictError {
	return &ConflictError{Kind: kind, Expected: expected, Current: current, Request: requestID}
}

// deepMerge merges src into dst in place using the shadow merge rules:
//
//  1. maps are merged key by key (nested maps merge recursively);
//  2. a key whose value is Deleted is removed from the result;
//  3. nil is a real value: the key is kept with a nil payload;
//  4. any other value (number, string, bool, slice, ...) replaces wholesale;
//  5. replacing a map with a scalar (or the reverse) replaces wholesale.
//
// The function never mutates src: values copied out of src are detached so
// later in-place updates cannot corrupt stored state.
func deepMerge(dst, src map[string]any) {
	for key, incoming := range src {
		if incoming == Deleted {
			delete(dst, key)
			continue
		}
		incomingMap, incomingIsMap := incoming.(map[string]any)
		current, exists := dst[key]
		if !exists {
			dst[key] = cloneValue(incoming)
			continue
		}
		currentMap, currentIsMap := current.(map[string]any)
	if incomingIsMap && currentIsMap {
			deepMerge(currentMap, incomingMap)
			continue
		}
		dst[key] = cloneValue(incoming)
	}
}

// cloneValue deep-copies values stored in a state tree. Maps and slices are
// rebuilt; scalar types are immutable and returned as-is.
func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(t))
		for k, val := range t {
			if val == Deleted {
				cp[k] = Deleted
				continue
			}
			cp[k] = cloneValue(val)
		}
		return cp
	case []any:
		cp := make([]any, len(t))
		for i, val := range t {
			cp[i] = cloneValue(val)
		}
	return cp
	default:
		return v
	}
}

// diff computes a stable minimal delta from desired toward reported.
//
// Result semantics:
//   - key present only in desired -> desired value (to apply);
//   - key present in both, values differ -> desired value;
//   - key present only in reported -> Deleted tombstone;
//   - equal scalars / both nil -> omitted;
//   - both maps -> recurse, empty result omitted.
//
// Keys of the returned map are sorted so that identical inputs always
// produce identical output (deterministic serialisation/logging).
func diff(desired, reported map[string]any) map[string]any {
	d := diffInner(desired, reported)
	if d == nil {
		return map[string]any{}
	}
	return d
}

func diffInner(desired, reported map[string]any) map[string]any {
	var out map[string]any
	keys := make([]string, 0, len(desired)+len(reported))
	seen := make(map[string]struct{}, len(desired)+len(reported))
	for k := range desired {
		keys = append(keys, k)
		seen[k] = struct{}{}
	}
	for k := range reported {
		if _, ok := seen[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		dv, dOk := desired[key]
		rv, rOk := reported[key]
		switch {
		case dOk && !rOk:
			if out == nil {
				out = map[string]any{}
			}
			out[key] = cloneValue(dv)
		case !dOk && rOk:
			if out == nil {
				out = map[string]any{}
			}
			out[key] = Deleted
		default:
			dm, dMap := dv.(map[string]any)
			rm, rMap := rv.(map[string]any)
			switch {
			case dMap && rMap:
				if nested := diffInner(dm, rm); nested != nil {
					if out == nil {
						out = map[string]any{}
					}
					out[key] = nested
				}
			case dMap != rMap || !valuesEqual(dv, rv):
				if out == nil {
					out = map[string]any{}
				}
				out[key] = cloneValue(dv)
			}
		}
	}
	return out
}

// valuesEqual compares JSON-decoded state values.
func valuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			w, ok := bv[k]
			if !ok || !valuesEqual(v, w) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !valuesEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}

// describeState renders a state tree on one line for logs, marking Deleted
// entries explicitly so log output stays interpretable.
func describeState(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := m[k]
		if v == Deleted {
			parts = append(parts, fmt.Sprintf("%q:<deleted>", k))
		} else {
			parts = append(parts, fmt.Sprintf("%q:%v", k, v))
		}
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
