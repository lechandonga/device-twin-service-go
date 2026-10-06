package twin

import (
	"reflect"
	"testing"
)

func TestDeepMergeAndDelete(t *testing.T) {
	biz := "字段删除/置空/嵌套合并规则"
	t.Logf("[业务:%s] 开始: Deleted 删除键, nil 保留为 null, 嵌套 map 递归合并, 标量整体替换", biz)

	state := map[string]any{
		"keep":  "v",
		"clear": "old",
		"null":  "old",
		"cfg":   map[string]any{"a": 1, "b": 2},
		"list":  []any{1, 2},
	}
	patch := map[string]any{
		"clear": Deleted,
		"null":  nil,
		"cfg":   map[string]any{"b": Deleted, "c": 3},
		"list":  []any{9},
		"added": Deleted,
	}
	deepMerge(state, patch)

	if _, ok := state["clear"]; ok {
		t.Fatalf("[业务:%s] 用例=删除已存在键: 失败, clear 仍存在", biz)
	}
	t.Logf("[业务:%s] 用例=删除已存在键: 通过", biz)

	if v, ok := state["null"]; !ok || v != nil {
		t.Fatalf("[业务:%s] 用例=置空(null)保留键: 失败, got %v ok=%v", biz, v, ok)
	}
	t.Logf("[业务:%s] 用例=置空(null)保留键: 通过", biz)

	cfg := state["cfg"].(map[string]any)
	want := map[string]any{"a": 1, "c": 3}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("[业务:%s] 用例=嵌套递归合并并删除内层键: 失败, got %v want %v", biz, cfg, want)
	}
	t.Logf("[业务:%s] 用例=嵌套递归合并并删除内层键: 通过 (%v)", biz, cfg)

	if got := state["list"]; !reflect.DeepEqual(got, []any{9}) {
		t.Fatalf("[业务:%s] 用例=标量/数组整体替换: 失败, got %v", biz, got)
	}
	t.Logf("[业务:%s] 用例=标量/数组整体替换: 通过", biz)

	// Determinism: same input sequence twice -> same result.
	base := func() map[string]any {
		return map[string]any{"x": 1, "cfg": map[string]any{"a": 1, "b": 2}}
	}
	run := func() map[string]any {
		m := base()
		deepMerge(m, map[string]any{"cfg": map[string]any{"b": Deleted, "c": 3}})
		return m
	}
	if !reflect.DeepEqual(run(), run()) {
		t.Fatalf("[业务:%s] 用例=相同更新序列结果一致: 失败", biz)
	}
	t.Logf("[业务:%s] 用例=相同更新序列结果一致: 通过", biz)
}

func TestDeepMergeDoesNotAliasInput(t *testing.T) {
	biz := "存储隔离"
	t.Logf("[业务:%s] 开始: 合并后修改入参不得污染已存状态", biz)
	svc, _ := newTestService(t)
	mustRegister(t, svc, "d1")
	patch := map[string]any{"cfg": map[string]any{"a": 1}}
	if _, err := svc.UpdateDesired(DesiredRequest{DeviceID: "d1", Patch: patch}); err != nil {
		t.Fatalf("[业务:%s] 用例=首次写入: 失败 %v", biz, err)
	}
	patch["cfg"].(map[string]any)["a"] = 999
	got := svc.Get("d1").Desired["cfg"].(map[string]any)["a"]
	if got != 1 {
		t.Fatalf("[业务:%s] 用例=入参变更不影响存储: 失败, got %v", biz, got)
	}
	t.Logf("[业务:%s] 用例=入参变更不影响存储: 通过", biz)
}

func TestDiffSemantics(t *testing.T) {
	biz := "差异(增量)生成"
	t.Logf("[业务:%s] 开始: 新增/差异取期望值, 设备多余键生成删除墓碑, 相等省略", biz)
	desired := map[string]any{
		"same":  1,
		"new":   "x",
		"nested": map[string]any{"a": 1, "b": 2},
	}
	reported := map[string]any{
		"same":   1,
		"new":    "old",
		"nested": map[string]any{"a": 1, "b": 9, "gone": 0},
		"extra":  true,
	}
	d := diff(desired, reported)
	want := map[string]any{
		"extra": Deleted,
		"new":   "x",
		"nested": map[string]any{"b": 2, "gone": Deleted},
	}
	if !valuesEqual(d, want) {
		t.Fatalf("[业务:%s] 用例=增量内容: 失败, got %v want %v", biz, d, want)
	}
	t.Logf("[业务:%s] 用例=增量内容: 通过 %s", biz, describeState(d))

	// Deterministic key order across repeated calls (check serialized shape).
	s1 := describeState(diff(desired, reported))
	s2 := describeState(diff(desired, reported))
	if s1 != s2 {
		t.Fatalf("[业务:%s] 用例=增量序列化稳定: 失败 %q != %q", biz, s1, s2)
	}
	t.Logf("[业务:%s] 用例=增量序列化稳定: 通过", biz)

	if d2 := diff(map[string]any{"a": 1}, map[string]any{"a": 1}); len(d2) != 0 {
		t.Fatalf("[业务:%s] 用例=期望与上报一致时无增量: 失败, got %v", biz, d2)
	}
	t.Logf("[业务:%s] 用例=期望与上报一致时无增量: 通过", biz)
}
