package twin

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestVersionConflicts(t *testing.T) {
	biz := "版本化更新与冲突拒绝"
	t.Logf("[业务:%s] 开始: 过期版本/超前版本/重复请求不同负载必须被明确区分", biz)
	svc, _ := newTestService(t)
	mustRegister(t, svc, "dev")

	r1, err := svc.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 0, Patch: map[string]any{"a": 1}})
	if err != nil || r1.DesiredVersion != 1 {
		t.Fatalf("[业务:%s] 用例=首次写 v0->v1: 失败 %v %+v", biz, err, r1)
	}
	t.Logf("[业务:%s] 用例=首次写 v0->v1: 通过", biz)

	_, err = svc.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 0, Patch: map[string]any{"a": 2}})
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Kind != ConflictExpiredVersion {
		t.Fatalf("[业务:%s] 用例=过期版本写拒绝(expired_version): 失败, got %v", biz, err)
	}
	t.Logf("[业务:%s] 用例=过期版本写拒绝(expired_version): 通过 (%s)", biz, ce.Error())

	_, err = svc.UpdateDesired(DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 5, Patch: map[string]any{"a": 2}})
	if !errors.As(err, &ce) || ce.Kind != ConflictVersionAhead {
		t.Fatalf("[业务:%s] 用例=超前版本写拒绝(version_ahead): 失败, got %v", biz, err)
	}
	t.Logf("[业务:%s] 用例=超前版本写拒绝(version_ahead): 通过 (%s)", biz, ce.Error())

	req := DesiredRequest{DeviceID: "dev", ExpectedDesiredVersion: 1, RequestID: "r2", Patch: map[string]any{"b": 2}}
	r2a, err := svc.UpdateDesired(req)
	if err != nil {
		t.Fatalf("[业务:%s] 用例=首次带 requestId 写: 失败 %v", biz, err)
	}
	r2b, err := svc.UpdateDesired(req)
	if err != nil || r2a.DesiredVersion != r2b.DesiredVersion {
		t.Fatalf("[业务:%s] 用例=相同请求幂等重试不涨版本: 失败 %v %+v %+v", biz, err, r2a, r2b)
	}
	t.Logf("[业务:%s] 用例=相同请求幂等重试不涨版本: 通过 (v=%d)", biz, r2b.DesiredVersion)

	req.Patch = map[string]any{"b": 3}
	_, err = svc.UpdateDesired(req)
	if !errors.As(err, &ce) || ce.Kind != ConflictDuplicateRequest {
		t.Fatalf("[业务:%s] 用例=同 requestId 不同负载拒绝(duplicate_request_id): 失败, got %v", biz, err)
	}
	t.Logf("[业务:%s] 用例=同 requestId 不同负载拒绝(duplicate_request_id): 通过 (%s)", biz, ce.Error())

	if got := svc.Get("dev").DesiredVersion; got != 2 {
		t.Fatalf("[业务:%s] 用例=冲突写不改变版本: 失败, got v%d", biz, got)
	}
	t.Logf("[业务:%s] 用例=冲突写不改变版本: 通过", biz)
}

func TestConcurrentDesiredNoLostUpdate(t *testing.T) {
	biz := "并发更新无丢写"
	t.Logf("[业务:%s] 开始: 多协程基于最新版本自旋写入, 每个唯一键必须保留", biz)
	svc, _ := newTestService(t)
	mustRegister(t, svc, "dev")

	const n = 50
	var wg sync.WaitGroup
	var ok, rejected int64
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				v := svc.Get("dev")
				_, err := svc.UpdateDesired(DesiredRequest{
					DeviceID:               "dev",
					ExpectedDesiredVersion: v.DesiredVersion,
					Patch:                  map[string]any{fmt.Sprintf("k%d", i): i},
				})
				if err == nil {
					mu.Lock()
					ok++
					mu.Unlock()
					return
				}
				var ce *ConflictError
				if errors.As(err, &ce) && ce.Kind == ConflictExpiredVersion {
					mu.Lock()
					rejected++
					mu.Unlock()
					continue
				}
				t.Errorf("unexpected error: %v", err)
				return
			}
		}(i)
	}
	wg.Wait()

	view := svc.Get("dev")
	if view.DesiredVersion != n {
		t.Fatalf("[业务:%s] 用例=成功写入次数等于版本增量: 失败, ok=%d v=%d", biz, ok, view.DesiredVersion)
	}
	for i := 0; i < n; i++ {
		if view.Desired[fmt.Sprintf("k%d", i)] != i {
			t.Fatalf("[业务:%s] 用例=无丢写无旧覆盖新: 失败, 缺少 k%d", biz, i)
		}
	}
	t.Logf("[业务:%s] 用例=50 并发全部落库无丢写: 通过 (成功=%d 冲突重试=%d 最终版本=%d)", biz, ok, rejected, view.DesiredVersion)
}

func drain(conn *Connection) []*Command {
	var out []*Command
	for {
		select {
		case c := <-conn.Ch:
			out = append(out, c)
		default:
			return out
		}
	}
}
