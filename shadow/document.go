// Package shadow implements the versioned device-shadow document model.
//
// A Document keeps the desired and reported state in two independent
// sections. Every mutation is a compare-and-swap on the document version:
// the caller must present the version it based its update on, and stale
// writers are rejected with a typed ConflictError. Merge semantics follow
// RFC 7386 (JSON Merge Patch): a null value deletes the key, two objects
// merge recursively, anything else replaces. Applying the same sequence of
// patches always yields the same document.
package shadow

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// Section names a part of the shadow document.
type Section string

const (
	SectionDesired  Section = "desired"
	SectionReported Section = "reported"
)

// ConflictError reports a compare-and-swap failure: the caller based its
// write on a version that is no longer current.
type ConflictError struct {
	Device  string
	Section Section
	Base    int64
	Current int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("shadow: version conflict on %q %s: based on version %d, current version is %d",
		e.Device, e.Section, e.Base, e.Current)
}

// IsConflict reports whether err is a version-conflict rejection.
func IsConflict(err error) bool {
	var ce *ConflictError
	return errors.As(err, &ce)
}

// Document is the versioned twin state of one device.
type Document struct {
	Version  int64          `json:"version"`
	Desired  map[string]any `json:"desired"`
	Reported map[string]any `json:"reported"`
}

// NewDocument returns an empty document at version 0.
func NewDocument() *Document {
	return &Document{
		Desired:  map[string]any{},
		Reported: map[string]any{},
	}
}

// Clone deep-copies the document via JSON round-trip so callers never share
// mutable maps with the store.
func (d *Document) Clone() *Document {
	raw, _ := json.Marshal(d)
	var out Document
	_ = json.Unmarshal(raw, &out)
	out.normalize()
	return &out
}

func (d *Document) normalize() {
	if d.Desired == nil {
		d.Desired = map[string]any{}
	}
	if d.Reported == nil {
		d.Reported = map[string]any{}
	}
}

// ApplyDesired merges patch into the desired section if baseVersion matches
// the current version. Returns the new version or a *ConflictError.
func (d *Document) ApplyDesired(device string, patch map[string]any, baseVersion int64) (int64, error) {
	return d.apply(device, SectionDesired, patch, baseVersion)
}

// ApplyReported merges patch into the reported section if baseVersion
// matches the current version.
func (d *Document) ApplyReported(device string, patch map[string]any, baseVersion int64) (int64, error) {
	return d.apply(device, SectionReported, patch, baseVersion)
}

func (d *Document) apply(device string, section Section, patch map[string]any, baseVersion int64) (int64, error) {
	if baseVersion != d.Version {
		return d.Version, &ConflictError{Device: device, Section: section, Base: baseVersion, Current: d.Version}
	}
	target := d.Desired
	if section == SectionReported {
		target = d.Reported
	}
	mergePatch(target, patch)
	d.Version++
	return d.Version, nil
}

// mergePatch applies RFC 7386 semantics: a nil value deletes the key, two
// nested objects merge recursively, any other value replaces the target.
func mergePatch(dst, patch map[string]any) {
	for key, pv := range patch {
		if pv == nil {
			delete(dst, key)
			continue
		}
		patchObj, patchIsObj := asObject(pv)
		dstObj, dstIsObj := asObject(dst[key])
		if patchIsObj && dstIsObj {
			mergePatch(dstObj, patchObj)
			continue
		}
		if patchIsObj {
			// Replace a scalar with a fresh object so the patch map is
			// never aliased into the document.
			merged := map[string]any{}
			mergePatch(merged, patchObj)
			dst[key] = merged
			continue
		}
		dst[key] = pv
	}
}

func asObject(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

// Diff computes the merge patch that, applied to reported, makes it match
// desired. Keys present in reported but absent in desired are set to nil
// (delete). Returns nil when the sections already agree.
func Diff(desired, reported map[string]any) map[string]any {
	delta := map[string]any{}
	diffInto(desired, reported, delta)
	if len(delta) == 0 {
		return nil
	}
	return delta
}

func diffInto(desired, reported, out map[string]any) {
	for key, dv := range desired {
		rv, ok := reported[key]
		dObj, dIsObj := asObject(dv)
		if !ok {
			out[key] = deepCopy(dv)
			continue
		}
		rObj, rIsObj := asObject(rv)
		if dIsObj && rIsObj {
			sub := map[string]any{}
			diffInto(dObj, rObj, sub)
			if len(sub) > 0 {
				out[key] = sub
			}
			continue
		}
		if !reflect.DeepEqual(dv, rv) {
			out[key] = deepCopy(dv)
		}
	}
	for key := range reported {
		if _, ok := desired[key]; !ok {
			out[key] = nil
		}
	}
}

func deepCopy(v any) any {
	if obj, ok := asObject(v); ok {
		out := make(map[string]any, len(obj))
		for k, item := range obj {
			out[k] = deepCopy(item)
		}
		return out
	}
	return v
}
