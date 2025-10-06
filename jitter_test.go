package crdbpgx_test

import (
	"testing"
	"time"

	"github.com/sumup/crdbpgx"
)

func TestJitteredTicker(t *testing.T) {
	t.Parallel()

	dur := 100 * time.Millisecond
	jit := 200 * time.Millisecond
	tk := crdbpgx.NewJitteredTicker(dur, &mockJitter{nextJitter: jit})

	select {
	case <-tk.C:
		t.Fatal("unexpected tick before jitter")
	case <-time.After(jit):
		// We must not return before the first tick with jitter applied.
	}

	select {
	case <-tk.C:
		// We must return after the first tick with jitter applied.
		tk.Stop()
	case <-time.After(2 * (dur + jit)):
		t.Fatal("expected tick after jitter expired")
	}

	// Check close
	select {
	case tt := <-tk.C:
		if !tt.IsZero() {
			t.Fatal("closed ticker must return the zero time")
		}
	case <-time.After(2 * (dur + jit)):
		t.Fatal("closed ticker must return immediately")
	}
}

type mockJitter struct {
	nextJitter time.Duration
}

var _ crdbpgx.Jitter = (*mockJitter)(nil)

func (m *mockJitter) Jitter(d time.Duration) time.Duration {
	return d + m.nextJitter
}
