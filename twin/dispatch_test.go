package twin

import "testing"

func TestOfflineBacklogOrderAndDedup(t *testing.T) {
	biz := "离线积压保序补发与重复去重"
	t.Logf("[业务:%s] 开始: 离线多次期望更新按 seq 保序, 重复连接/重复上报不重复下发", biz)
	svc, _ := newTestService(t)
	mustRegister(t, svc, "dev")

	for i := 1; i <= 3; i++ {
		if _, err := svc.UpdateDesired(DesiredRequest{
			DeviceID:               "dev",
			ExpectedDesiredVersion: int64(i - 1),
			Patch:                  map[string]any{keyN(i): i},
		}); err != nil {
			t.Fatalf("[业务:%s] 离线期望更新 #%d: %v", biz, i, err)
		}
	}

	conn, err := svc.Connect("dev", "c1")
	if err != nil {
		t.Fatal(err)
	}
	first := drain(conn)
	if len(first) != 3 {
		t.Fatalf("[业务:%s] 用例=重连补齐全部积压: 失败, got %d 条", biz, len(first))
	}
	for i, c := range first {
		if c.Seq != int64(i+1) {
			t.Fatalf("[业务:%s] 用例=按序补发: 失败, 位置 %d seq=%d", biz, i, c.Seq)
		}
	}
	t.Logf("[业务:%s] 用例=重连按 seq 顺序补发 3 条: 通过", biz)

	again, err := svc.Connect("dev", "c1")
	if err != nil || again != conn {
		t.Fatalf("[业务:%s] 用例=同连接重复连接去重: 失败 err=%v same=%v", biz, err, again == conn)
	}
	if got := drain(conn); len(got) != 0 {
		t.Fatalf("[业务:%s] 用例=重复连接不重复下发: 失败, got %d", biz, len(got))
	}
	t.Logf("[业务:%s] 用例=重复连接返回同一句柄且零重复下发: 通过", biz)

	conn2, err := svc.Connect("dev", "c2")
	if err != nil {
		t.Fatal(err)
	}
	second := drain(conn2)
	if len(second) != 3 {
		t.Fatalf("[业务:%s] 用例=断线重连补发未确认指令: 失败, got %d", biz, len(second))
	}
	t.Logf("[业务:%s] 用例=新连接重连按序补发未确认指令: 通过 (%d 条)", biz, len(second))

	if _, err := svc.Report(ReportRequest{
		DeviceID: "dev", Patch: map[string]any{"k1": 1, "k2": 2, "k3": 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Report(ReportRequest{
		DeviceID:                "dev",
		ExpectedReportedVersion: 1,
		Patch:                   map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}
	conn3, _ := svc.Connect("dev", "c3")
	if got := drain(conn3); len(got) != 0 {
		t.Fatalf("[业务:%s] 用例=已执行指令重连不再下发: 失败, got %d", biz, len(got))
	}
	statuses := map[int64]CommandStatus{}
	for _, c := range svc.Get("dev").Commands {
		statuses[c.Seq] = c.Status
	}
	for seq := int64(1); seq <= 3; seq++ {
		if statuses[seq] != CmdExecuted {
			t.Fatalf("[业务:%s] 用例=重复上报不重复执行: 失败, seq=%d status=%s", biz, seq, statuses[seq])
		}
	}
	t.Logf("[业务:%s] 用例=执行后任意重复上报/重连零重复下发: 通过", biz)
}

func TestSupersededAndExpired(t *testing.T) {
	biz := "过期/被覆盖/已执行可区分"
	t.Logf("[业务:%s] 开始: 被新更新覆盖的排队指令标记 superseded, 重试耗尽标记 expired", biz)
	svc, _ := newTestService(t)
	mustRegister(t, svc, "dev")

	_, _ = svc.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 0, Patch: map[string]any{"x": 1}})
	_, _ = svc.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 1, Patch: map[string]any{"x": 2}})
	cmds := svc.Get("dev").Commands
	if cmds[0].Status != CmdSuperseded || cmds[1].Status != CmdQueued {
		t.Fatalf("[业务:%s] 用例=旧排队指令被覆盖为 superseded: 失败, %s/%s", biz, cmds[0].Status, cmds[1].Status)
	}
	t.Logf("[业务:%s] 用例=同字段排队旧指令 superseded, 新指令 queued: 通过 (seq1=%s seq2=%s)", biz, cmds[0].Status, cmds[1].Status)

	_, _ = svc.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 2, Patch: map[string]any{"y": 7}})
	if svc.Get("dev").Commands[1].Status != CmdQueued {
		t.Fatalf("[业务:%s] 用例=不相关字段不覆盖: 失败", biz)
	}
	t.Logf("[业务:%s] 用例=不同路径更新不互相覆盖: 通过", biz)

	conn, _ := svc.Connect("dev", "c1")
	drain(conn)
	for i := 0; i < MaxAttempts+2; i++ {
		_, _, _ = svc.RetryOutstanding("dev")
	}
	for _, c := range svc.Get("dev").Commands {
		if c.Seq == 2 || c.Seq == 3 {
			if c.Status != CmdExpired {
				t.Fatalf("[业务:%s] 用例=重试耗尽标记 expired 且不再发送: 失败, seq=%d %s attempts=%d", biz, c.Seq, c.Status, c.Attempts)
			}
		}
	}
	if svc.Get("dev").Commands[0].Status != CmdSuperseded {
		t.Fatalf("[业务:%s] 用例=三类终态互不串改: 失败", biz)
	}
	t.Logf("[业务:%s] 用例=未确认指令重试上限后 expired 并保留结论, 终态可区分: 通过", biz)
}

func TestExplicitAckExecutes(t *testing.T) {
	biz := "显式确认执行"
	t.Logf("[业务:%s] 开始: AckSeq 可在上报状态尚未收敛时确认指令执行", biz)
	svc, _ := newTestService(t)
	mustRegister(t, svc, "dev")
	_, _ = svc.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 0, Patch: map[string]any{"x": 1}})
	conn, _ := svc.Connect("dev", "c1")
	seq := drain(conn)[0].Seq

	res, err := svc.Report(ReportRequest{DeviceID: "dev", Patch: map[string]any{"other": 1}, AckSeq: seq})
	if err != nil || len(res.ExecutedSeqs) != 1 || res.ExecutedSeqs[0] != seq {
		t.Fatalf("[业务:%s] 用例=AckSeq 标记执行: 失败 %v %+v", biz, err, res)
	}
	conn2, _ := svc.Connect("dev", "c2")
	if len(drain(conn2)) != 0 {
		t.Fatalf("[业务:%s] 用例=确认后重连不再下发: 失败", biz)
	}
	t.Logf("[业务:%s] 用例=显式 Ack 后指令 executed 且不再补发: 通过", biz)
}

func keyN(i int) string {
	return "k" + string(rune('0'+i))
}
