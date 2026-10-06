package twin

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lechandonga/device-twin-service-go/shadow"
)

func open(t *testing.T, dir string, opts ...Option) *Service {
	t.Helper()
	s, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open service: %v", err)
	}
	return s
}

func TestVersionConflictOnConcurrentWriters(t *testing.T) {
	t.Log("业务: 版本冲突 —— 并发写同一设备, 基于过期版本的写入被拒绝, 不丢写也不旧值覆盖新值")
	s := open(t, t.TempDir())

	const writers = 32
	var wg sync.WaitGroup
	wins := make(chan int64, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// All writers base on version 0 and race to commit; exactly one wins.
			v, err := s.UpdateDesired("dev-race", map[string]any{"writer": float64(i)}, 0)
			if err == nil {
				wins <- v
				return
			}
			if !shadow.IsConflict(err) {
				t.Errorf("unexpected error type: %v", err)
			}
		}(i)
	}
	wg.Wait()
	close(wins)

	count := 0
	for range wins {
		count++
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 winning writer, got %d", count)
	}
	if got := s.Get("dev-race").Version; got != 1 {
		t.Fatalf("version = %d, want 1", got)
	}
	t.Logf("用例通过: %d 个并发写者中 1 个成功, %d 个收到版本冲突", writers, writers-count)
}

func TestOfflineReplayOrderAndAckResults(t *testing.T) {
	t.Log("业务: 离线补发 —— 离线期间积压的增量与指令按设备保序, 重连后按序补齐, ack 结果可区分")
	s := open(t, t.TempDir())

	// Device offline: queue a delta and two commands.
	if _, err := s.UpdateDesired("dev-1", map[string]any{"light": "on"}, 0); err != nil {
		t.Fatalf("update desired: %v", err)
	}
	if _, err := s.SendCommand("dev-1", "reboot", map[string]any{"delay": float64(5)}); err != nil {
		t.Fatalf("send command: %v", err)
	}
	if _, err := s.SendCommand("dev-1", "ota", map[string]any{"fw": "1.2.3"}); err != nil {
		t.Fatalf("send command: %v", err)
	}

	entries, err := s.Connect("dev-1")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("delivered %d entries, want 3", len(entries))
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Seq <= entries[i-1].Seq {
			t.Fatalf("out of order delivery: %+v", entries)
		}
	}
	if entries[0].Kind != KindDelta || entries[1].Command != "reboot" || entries[2].Command != "ota" {
		t.Fatalf("unexpected delivery order: %+v", entries)
	}

	// Ack outcomes must be distinguishable.
	if res, _ := s.Ack("dev-1", entries[0].Seq); res != AckApplied {
		t.Fatalf("first ack = %s, want applied", res)
	}
	if res, _ := s.Ack("dev-1", entries[0].Seq); res != AckDuplicate {
		t.Fatalf("repeat ack = %s, want duplicate", res)
	}
	if res, _ := s.Ack("dev-1", 9999); res != AckUnknown {
		t.Fatalf("unknown ack = %s, want unknown", res)
	}
	t.Logf("用例通过: 3 条积压按 seq 顺序补发, ack 结果覆盖 applied/duplicate/unknown")
}

func TestDuplicateConnectDeliversNothing(t *testing.T) {
	t.Log("业务: 重复连接去重 —— 重复连接/重试不会造成重复下发")
	s := open(t, t.TempDir())
	if _, err := s.UpdateDesired("dev-1", map[string]any{"light": "on"}, 0); err != nil {
		t.Fatalf("update: %v", err)
	}

	first, err := s.Connect("dev-1")
	if err != nil || len(first) != 1 {
		t.Fatalf("first connect: entries=%d err=%v", len(first), err)
	}
	if _, err := s.Connect("dev-1"); err != ErrAlreadyConnected {
		t.Fatalf("duplicate connect err = %v, want ErrAlreadyConnected", err)
	}

	// Disconnect without ack: the entry returns to pending and is
	// redelivered exactly once on reconnect.
	if err := s.Disconnect("dev-1"); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	second, err := s.Connect("dev-1")
	if err != nil || len(second) != 1 || second[0].Seq != first[0].Seq {
		t.Fatalf("reconnect redelivery = %+v, want seq %d", second, first[0].Seq)
	}
	tLogFree(t, s, "dev-1", first[0].Seq)
	t.Log("用例通过: 重复连接被拒绝且不重复下发, 断线未 ack 的条目重连后恰好补发一次")
}

// tLogFree acks the entry and fails the test on unexpected results.
func tLogFree(t *testing.T, s *Service, device string, seq int64) {
	t.Helper()
	if res, err := s.Ack(device, seq); err != nil || res != AckApplied {
		t.Fatalf("ack after reconnect = %s, %v", res, err)
	}
}

func TestSupersededDeltaNotDelivered(t *testing.T) {
	t.Log("业务: 覆盖与过期 —— 已被新写入覆盖的增量标记为 superseded, 不会旧值覆盖新值")
	s := open(t, t.TempDir())

	v, err := s.UpdateDesired("dev-1", map[string]any{"light": "on"}, 0)
	if err != nil {
		t.Fatalf("update 1: %v", err)
	}
	if _, err := s.UpdateDesired("dev-1", map[string]any{"light": "off"}, v); err != nil {
		t.Fatalf("update 2: %v", err)
	}

	entries, err := s.Connect("dev-1")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if len(entries) != 1 || entries[0].Patch["light"] != "off" {
		t.Fatalf("delivered %+v, want only the newest delta", entries)
	}

	// The superseded entry's ack result is distinguishable.
	var supersededSeq int64
	for _, e := range s.Outbox("dev-1") {
		if e.Status == StatusSuperseded {
			supersededSeq = e.Seq
		}
	}
	if supersededSeq == 0 {
		t.Fatal("no superseded entry recorded")
	}
	if res, _ := s.Ack("dev-1", supersededSeq); res != AckSuperseded {
		t.Fatalf("ack of superseded = %s, want superseded", res)
	}
	t.Log("用例通过: 旧增量被覆盖, 仅最新差异下发, superseded 状态可区分")
}

func TestDeliveryAttemptsExpire(t *testing.T) {
	t.Log("业务: 有限重发 —— 投递次数有上限, 超限标记 expired, 不无限重发也不静默丢弃")
	s := open(t, t.TempDir(), WithMaxAttempts(2))
	if _, err := s.SendCommand("dev-1", "reboot", nil); err != nil {
		t.Fatalf("command: %v", err)
	}

	// Two connect/disconnect cycles without ack consume the attempts.
	for i := 0; i < 2; i++ {
		entries, err := s.Connect("dev-1")
		if err != nil || len(entries) != 1 {
			t.Fatalf("cycle %d: entries=%d err=%v", i, len(entries), err)
		}
		if err := s.Disconnect("dev-1"); err != nil {
			t.Fatalf("disconnect: %v", err)
		}
	}
	entries, err := s.Connect("dev-1")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expired entry redelivered: %+v", entries)
	}
	outbox := s.Outbox("dev-1")
	if len(outbox) != 1 || outbox[0].Status != StatusExpired {
		t.Fatalf("outbox = %+v, want one expired entry", outbox)
	}
	if res, _ := s.Ack("dev-1", outbox[0].Seq); res != AckExpired {
		t.Fatalf("ack of expired = %s, want expired", res)
	}
	t.Log("用例通过: 达到重发上限后条目标记 expired 且保留可查, 不再投递")
}

func TestRestartRecovery(t *testing.T) {
	t.Log("业务: 重启恢复 —— 版本、未完成的下发与指令状态一致恢复, 版本继续递增不回退")
	dir := t.TempDir()

	s1 := open(t, dir)
	v, err := s1.UpdateDesired("dev-1", map[string]any{"light": "on"}, 0)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := s1.SendCommand("dev-1", "reboot", nil); err != nil {
		t.Fatalf("command: %v", err)
	}
	entries, err := s1.Connect("dev-1")
	if err != nil || len(entries) != 2 {
		t.Fatalf("connect: entries=%d err=%v", len(entries), err)
	}
	if res, _ := s1.Ack("dev-1", entries[0].Seq); res != AckApplied {
		t.Fatalf("ack = %s", res)
	}
	// Crash simulation: drop s1 without disconnect (sent command unacked).

	s2 := open(t, dir)
	doc := s2.Get("dev-1")
	if doc.Version != v {
		t.Fatalf("recovered version = %d, want %d (no rollback)", doc.Version, v)
	}
	// The unacked command must be pending again after recovery.
	replayed, err := s2.Connect("dev-1")
	if err != nil {
		t.Fatalf("reconnect after restart: %v", err)
	}
	if len(replayed) != 1 || replayed[0].Kind != KindCommand || replayed[0].Command != "reboot" {
		t.Fatalf("replayed after restart = %+v, want the unacked command", replayed)
	}
	// Version keeps increasing from the recovered value.
	if _, err := s2.UpdateDesired("dev-1", map[string]any{"light": "off"}, 0); !shadow.IsConflict(err) {
		t.Fatalf("stale base after restart: err=%v, want conflict", err)
	}
	v2, err := s2.UpdateDesired("dev-1", map[string]any{"light": "off"}, doc.Version)
	if err != nil || v2 != v+1 {
		t.Fatalf("post-restart version = %d err=%v, want %d", v2, err, v+1)
	}
	t.Logf("用例通过: 重启后版本 %d→%d 单调递增, 未 ack 指令恢复为 pending 并补发", v, v2)
}

func TestConcurrentDevicesIsolation(t *testing.T) {
	t.Log("业务: 并发隔离 —— 大量设备并发上下线与更新交错, 单设备异常不影响其他设备, 最终状态确定")
	s := open(t, t.TempDir())

	const devices = 16
	const rounds = 20
	var wg sync.WaitGroup
	for d := 0; d < devices; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			id := fmt.Sprintf("dev-%d", d)
			var version int64
			for r := 0; r < rounds; r++ {
				var err error
				if version, err = s.UpdateDesired(id, map[string]any{"n": float64(r)}, version); err != nil {
					t.Errorf("%s update: %v", id, err)
					return
				}
				if _, err := s.Connect(id); err != nil && err != ErrAlreadyConnected {
					t.Errorf("%s connect: %v", id, err)
					return
				}
				if err := s.Disconnect(id); err != nil {
					t.Errorf("%s disconnect: %v", id, err)
					return
				}
			}
		}(d)
	}
	wg.Wait()

	for d := 0; d < devices; d++ {
		doc := s.Get(fmt.Sprintf("dev-%d", d))
		if doc.Version != rounds {
			t.Errorf("dev-%d version = %d, want %d", d, doc.Version, rounds)
		}
		if doc.Desired["n"] != float64(rounds-1) {
			t.Errorf("dev-%d desired = %#v, want n=%d", d, doc.Desired, rounds-1)
		}
	}
	t.Logf("用例通过: %d 台设备 x %d 轮并发更新与上下线, 全部版本与终态一致", devices, rounds)
}
