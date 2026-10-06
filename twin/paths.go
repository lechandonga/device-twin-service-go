package twin

import (
	"sort"
	"strconv"
	"strings"
)

// leafPaths returns dotted leaf paths contained in m. Tombstones are leaves:
// {"a": Deleted} -> ["a"], {"cfg": {"x": 1}} -> ["cfg.x"].
func leafPaths(prefix []string, m map[string]any) []string {
	var out []string
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		path := append(append([]string(nil), prefix...), k)
		v := m[k]
		if v == Deleted {
			out = append(out, strings.Join(path, "."))
			continue
		}
		if nested, ok := v.(map[string]any); ok {
			out = append(out, leafPaths(path, nested)...)
			continue
		}
		out = append(out, strings.Join(path, "."))
	}
	return out
}

// pathsOverlap reports whether two path sets share any path, or one contains
// a strict ancestor of the other (e.g. "cfg" and "cfg.x" overlap).
func pathsOverlap(a, b []string) bool {
	set := make(map[string]struct{}, len(a))
	for _, p := range a {
		set[p] = struct{}{}
	}
	for _, p := range b {
		if _, ok := set[p]; ok {
			return true
		}
		for _, q := range a {
			if hasPrefixPath(p, q) || hasPrefixPath(q, p) {
				return true
			}
		}
	}
	return false
}

func hasPrefixPath(path, prefix string) bool {
	return path != prefix && strings.HasPrefix(path, prefix+".")
}

func intsString(xs []int64) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.FormatInt(x, 10)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
