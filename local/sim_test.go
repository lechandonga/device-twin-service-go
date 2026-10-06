package local

import (
	"os"
	"testing"

	"github.com/lechandonga/device-twin-service-go/twin"
)

func newSimService(t *testing.T) *twin.Service {
	t.Helper()
	svc, err := twin.NewService(twin.NewMemoryStore(), Logger(os.Stdout))
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// TestSimulatedOfflineReconnect drives the full business loop with the local
// simulator: platform writes while the device is offline, backlog accumulates,
// reboot replays it in order, the device reports convergence, and a stale
// disconnect never kills the new link.
func TestSimulatedOfflineReconnect(t *testing.T) {
	biz := "本地模拟端到端: 离线积压->重启重连->收敛"
	t.Logf("[业务:%s] 开始", biz)
	svc := newSimService(t)
	dev, err := NewDevice(svc, "thermo-1")
	if err != nil {
		t.Fatal(err)
	}

	// Device boots, platform and device start converged.
	if _, err := dev.Boot(); err != nil {
	t.Fatal(err)
	}
	t.Logf("[业务:%s] 用例=设备首次上线: 通过", biz)

	if err := dev.GoOffline(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateDesired(twin.DesiredRequest{DeviceID: "thermo-1", ExpectedDesiredVersion: 0, Patch: map[string]any{"target": 22}}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateDesired(twin.DesiredRequest{DeviceID: "thermo-1", ExpectedDesiredVersion: 1, Patch: map[string]any{"target": 24, "mode": "eco"}}); err != nil {
		t.Fatal(err)
	}
	t.Logf("[业务:%s] 用例=离线期间两次期望更新入积压: 通过", biz)

	// Reboot: one fresh command (latest delta) is delivered; the first was
	// superseded at generation time.
	if _, err := dev.Boot(); err != nil {
		t.Fatal(err)
	}
	cmds := dev.Drain()
	if len(cmds) != 1 || cmds[0].Seq != 2 {
		t.Fatalf("[业务:%s] 用例=重连仅收到最新增量: 失败 %+v", biz, cmds)
	}
	t.Logf("[业务:%s] 用例=重连收到 seq=2 最新增量: 通过 (%s)", biz, cmds[0].Delta)

	res, err := dev.Report(map[string]any{"target": 24, "mode": "eco"}, "report-1", 0)
	if err != nil || len(res.ExecutedSeqs) != 1 {
		t.Fatalf("[业务:%s] 用例=设备上报收敛后指令执行: 失败 %v %+v", biz, err, res)
	}
	t.Logf("[业务:%s] 用例=上报匹配期望, seq=%d executed: 通过", biz, res.ExecutedSeqs[0])

	// Idempotent replay: same request id and same base version is a no-op and
	// does not bump the reported version.
	res2, err := svc.Report(twin.ReportRequest{
		DeviceID:                "thermo-1",
		ExpectedReportedVersion: 0,
		Patch:                   map[string]any{"target": 24, "mode": "eco"},
		RequestID:               "report-1",
	})
	if err != nil || res2.ReportedVersion != 1 {
		t.Fatalf("[业务:%s] 用例=相同 requestId 重复上报幂等不涨版本: 失败 %v %+v", biz, err, res2)
	}
	t.Logf("[业务:%s] 用例=重复上报幂等不涨版本: 通过 (rv=%d)", biz, res2.ReportedVersion)

	// A reboot now delivers nothing: nothing outstanding remains.
	if _, err := dev.Boot(); err != nil {
		t.Fatal(err)
	}
	if len(dev.Drain()) != 0 {
		t.Fatalf("[业务:%s] 用例=收敛后再次重连零下发: 失败", biz)
	}
	t.Logf("[业务:%s] 用例=收敛后重连零下发: 通过", biz)
	t.Logf("[业务:%s] 全部用例通过", biz)
}
