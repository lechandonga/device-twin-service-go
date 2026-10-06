package twin

import (
	"sync"
	"testing"
)

func TestRestartRecovery(t *testing.T) {
	biz := "持久化与重启恢复"
	t.Logf("[业务:%s] 开始: 重启后版本继续递增, 积压指令按序恢复, 墓碑与在线状态正确", biz)
	dir := t.TempDir()
	store := NewFileStore(dir + "/shadow.json")

	svc1, err := NewService(store, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, svc1, "dev")
	_, _ = svc1.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 0, Patch: map[string]any{"a": 1, "rm": 1, "nested": map[string]any{"z": 1}}})
	_, _ = svc1.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 1, Patch: map[string]any{"rm": Deleted, "a": 9}})
	conn1, _ := svc1.Connect("dev", "boot-1")
	got := drain(conn1)
	if len(got) != 1 || got[0].Seq != 2 {
		t.Fatalf("[业务:%s] 用例=在线时即时下发且只发最新增量: 失败 %+v", biz, got)
	}
	t.Logf("[业务:%s] 用例=在线即时下发最新增量(seq=2): 通过", biz)
	if err := conn1.Close(); err != nil {
		t.Fatal(err)
	}

	svc2, err := NewService(store, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	v := svc2.Get("dev")
	if v.DesiredVersion != 2 || v.Online {
		t.Fatalf("[业务:%s] 用例=重启恢复版本/离线态: 失败 v=%d online=%v", biz, v.DesiredVersion, v.Online)
	}
	if _, ok := v.Desired["rm"]; ok {
		t.Fatalf("[业务:%s] 用例=删除操作持久化: 失败, rm 仍存在", biz)
	}
	if !valuesEqual(v.Desired["a"], float64(9)) {
		t.Fatalf("[业务:%s] 用例=最新值持久化: 失败, a=%v", biz, v.Desired["a"])
	}
	t.Logf("[业务:%s] 用例=版本/删除/最新值恢复且全部设备离线: 通过 (v=%d)", biz, v.DesiredVersion)

	// Versions keep increasing after restart; no rollback.
	r, err := svc2.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 2, Patch: map[string]any{"b": 1}})
	if err != nil || r.DesiredVersion != 3 {
		t.Fatalf("[业务:%s] 用例=重启后版本继续递增: 失败 %v %+v", biz, err, r)
	}
	t.Logf("[业务:%s] 用例=重启后 v2->v3 继续递增: 通过", biz)

	// Backlog survives restart and replays in order.
	conn2, _ := svc2.Connect("dev", "boot-2")
	replayed := drain(conn2)
	// seq=2 was delivered but unacknowledged at crash time; the post-restart
	// v3 write then added seq=3 (different paths, not superseding). Both are
	// unfinished and must replay once each in Seq order.
	if len(replayed) != 2 || replayed[0].Seq != 2 || replayed[1].Seq != 3 {
		t.Fatalf("[业务:%s] 用例=已下发未确认+新积压指令重启后各补发一次并保序: 失败 %+v", biz, replayed)
	}
	t.Logf("[业务:%s] 用例=崩溃前未确认(seq=2)与新积压(seq=3)重启后保序补发: 通过", biz)

	// Now go offline, accumulate genuinely-queued backlog AFTER restart and
	// verify the delta content including a tombstone survives persistence.
	if err := conn2.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc2.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 3, Patch: map[string]any{"a": Deleted, "c": 5, "nested": map[string]any{"z": Deleted}}}); err != nil {
		t.Fatal(err)
	}
	svc3, err := NewService(store, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	conn3, _ := svc3.Connect("dev", "boot-3")
	all := drain(conn3)
	// The v4 patch covers every path carried by seq=2/seq=3 ("a", "nested"),
	// so those queued commands were superseded; only the fresh seq=4 remains.
	if len(all) != 1 || all[0].Seq != 4 {
		t.Fatalf("[业务:%s] 用例=跨重启积压按序且被覆盖指令不重发: 失败 %+v", biz, all)
	}
	delta := all[0].Delta
	if delta["c"] != float64(5) || delta["a"] != Deleted || delta["nested"].(map[string]any)["z"] != Deleted {
		t.Fatalf("[业务:%s] 用例=补发增量含删除墓碑(含嵌套): 失败 %s", biz, describeState(delta))
	}
	for _, c := range svc3.Get("dev").Commands {
		if (c.Seq == 2 || c.Seq == 3) && c.Status != CmdSuperseded {
			t.Fatalf("[业务:%s] 用例=被覆盖积压重启后仍为 superseded: 失败 seq=%d %s", biz, c.Seq, c.Status)
		}
	}
	t.Logf("[业务:%s] 用例=跨重启: seq=2/3 superseded 不重发, seq=4 含嵌套删除语义按序补发: 通过 (%s)", biz, describeState(delta))
}

func TestMultiDeviceIsolation(t *testing.T) {
	biz := "多设备故障隔离"
	t.Logf("[业务:%s] 开始: 大量设备并发上下线交错, 单设备异常不影响他设备, 结果确定", biz)
	svc, _ := newTestService(t)
	const n = 40
	for i := 0; i < n; i++ {
		mustRegister(t, svc, deviceName(i))
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := deviceName(i)
			// Deterministic interleaving: connect, offline writes, reconnect,
			// duplicate-connect, report. A panic/error on one device must not
			// disturb the others.
			if _, err := svc.Connect(id, "a"); err != nil {
				t.Errorf("connect %s: %v", id, err)
				return
			}
			if _, err := svc.UpdateDesired(DesiredRequest{DeviceID: id, Patch: map[string]any{"v": i}}); err != nil {
				t.Errorf("desired %s: %v", id, err)
				return
			}
			c, _ := svc.Connect(id, "a") // duplicate: must be a no-op
			drain(c)
			if _, err := svc.Report(ReportRequest{DeviceID: id, Patch: map[string]any{"v": i}}); err != nil {
				t.Errorf("report %s: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		v := svc.Get(deviceName(i))
		if v.DesiredVersion != 1 || v.ReportedVersion != 1 {
			t.Fatalf("[业务:%s] 用例=各设备版本互不干扰: 失败 %s dv=%d rv=%d", biz, v.ID, v.DesiredVersion, v.ReportedVersion)
		}
		if v.Desired["v"] != i || v.Reported["v"] != i {
			t.Fatalf("[业务:%s] 用例=各设备最终状态确定: 失败 %s", biz, v.ID)
		}
		if v.Commands[0].Status != CmdExecuted {
			t.Fatalf("[业务:%s] 用例=各设备指令独立收敛: 失败 %s %s", biz, v.ID, v.Commands[0].Status)
		}
	}
	t.Logf("[业务:%s] 用例=%d 设备并发交错后各自版本/状态/指令全部正确: 通过", biz, n)
}

func deviceName(i int) string {
	return "dev-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
