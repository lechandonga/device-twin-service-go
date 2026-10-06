package shadow

import (
	"reflect"
	"testing"
)

func TestMergePatchDeleteNullAndNestedMerge(t *testing.T) {
	t.Log("业务: 影子合并语义 —— 字段删除、置空与嵌套合并规则确定且稳定")

	doc := NewDocument()
	v, err := doc.ApplyDesired("dev-1", map[string]any{
		"power": "on",
		"config": map[string]any{
			"interval": float64(10),
			"tags":     map[string]any{"a": float64(1), "b": float64(2)},
		},
	}, 0)
	if err != nil || v != 1 {
		t.Fatalf("initial apply: v=%d err=%v", v, err)
	}

	// Delete "power", null out one nested tag, merge the rest.
	v, err = doc.ApplyDesired("dev-1", map[string]any{
		"power": nil,
		"config": map[string]any{
			"tags": map[string]any{"a": nil},
		},
	}, v)
	if err != nil {
		t.Fatalf("merge apply: %v", err)
	}

	want := map[string]any{
		"config": map[string]any{
			"interval": float64(10),
			"tags":     map[string]any{"b": float64(2)},
		},
	}
	if !reflect.DeepEqual(doc.Desired, want) {
		t.Fatalf("merged desired = %#v, want %#v", doc.Desired, want)
	}
	t.Logf("用例通过: null 删除字段, 嵌套对象递归合并, 版本推进到 %d", v)
}

func TestMergeIsDeterministicAndIdempotentSequence(t *testing.T) {
	t.Log("业务: 确定性 —— 相同更新序列重复执行得到一致结果")

	run := func() *Document {
		doc := NewDocument()
		v := int64(0)
		var err error
		for _, p := range []map[string]any{
			{"a": float64(1), "n": map[string]any{"x": float64(1)}},
			{"n": map[string]any{"y": float64(2)}},
			{"a": nil},
		} {
			if v, err = doc.ApplyDesired("dev-1", p, v); err != nil {
				t.Fatalf("apply: %v", err)
			}
		}
		return doc
	}
	first, second := run(), run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same sequence diverged: %#v vs %#v", first, second)
	}
	t.Log("用例通过: 两次执行同一更新序列, 文档完全一致")
}

func TestVersionConflictRejected(t *testing.T) {
	t.Log("业务: 版本化更新 —— 基于过期版本的写入被明确拒绝且原因可区分")

	doc := NewDocument()
	v, err := doc.ApplyDesired("dev-1", map[string]any{"a": float64(1)}, 0)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	_, err = doc.ApplyDesired("dev-1", map[string]any{"a": float64(2)}, 0) // stale base
	if err == nil {
		t.Fatal("stale write was not rejected")
	}
	var ce *ConflictError
	if !IsConflict(err) || !asConflict(err, &ce) {
		t.Fatalf("error is not a ConflictError: %v", err)
	}
	if ce.Base != 0 || ce.Current != v || ce.Section != SectionDesired {
		t.Fatalf("unexpected conflict detail: %+v", ce)
	}
	if doc.Desired["a"] != float64(1) {
		t.Fatalf("rejected write mutated state: %#v", doc.Desired)
	}
	t.Logf("用例通过: 过期版本写入被拒绝 (%v), 状态未被污染", err)
}

func asConflict(err error, ce **ConflictError) bool {
	if c, ok := err.(*ConflictError); ok {
		*ce = c
		return true
	}
	return false
}

func TestDiff(t *testing.T) {
	t.Log("业务: 差异计算 —— 期望与上报的增量 diff, 含嵌套与删除")

	desired := map[string]any{
		"a": float64(1),
		"n": map[string]any{"x": float64(2), "y": float64(3)},
	}
	reported := map[string]any{
		"a": float64(1),
		"n": map[string]any{"x": float64(1)},
		"z": "stale",
	}
	delta := Diff(desired, reported)
	want := map[string]any{
		"n": map[string]any{"x": float64(2), "y": float64(3)},
		"z": nil,
	}
	if !reflect.DeepEqual(delta, want) {
		t.Fatalf("delta = %#v, want %#v", delta, want)
	}
	if Diff(desired, desired) != nil {
		t.Fatal("identical sections must produce no delta")
	}
	t.Log("用例通过: 嵌套差异、新增键与删除键(nil)均正确生成")
}
