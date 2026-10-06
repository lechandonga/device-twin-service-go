package twin

import "testing"

// discardLog keeps tests quiet by default; individual tests annotate business
// coverage via t.Logf, which is visible with -v.
func newTestService(t *testing.T) (*Service, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	svc, err := NewService(store, testLogger{t})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, store
}

func mustRegister(t *testing.T, svc *Service, id string) {
	t.Helper()
	if err := svc.Register(id); err != nil {
		t.Fatalf("Register(%q): %v", id, err)
	}
}

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("    [svc] "+format, args...)
}
