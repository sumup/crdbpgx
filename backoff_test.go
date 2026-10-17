package crdbpgx

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExponentialBackoffWithJitter(t *testing.T) {
	t.Parallel()

	t.Run("doubles the delay", func(t *testing.T) {
		t.Parallel()

		backoff := exponentialBackoffWithJitter(100*time.Millisecond, 0)
		for attempt, want := range []time.Duration{
			100 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond,
			400 * time.Millisecond, 800 * time.Millisecond,
		} {
			assert.Equal(t, want, backoff(t.Context(), uint(attempt)))
		}
	})

	t.Run("jitter stays within bounds", func(t *testing.T) {
		t.Parallel()

		backoff := exponentialBackoffWithJitter(100*time.Millisecond, 0.1)
		for range 100 {
			got := backoff(t.Context(), 3)
			assert.GreaterOrEqual(t, got, 360*time.Millisecond)
			assert.LessOrEqual(t, got, 440*time.Millisecond)
		}
	})

	t.Run("saturates on overflow", func(t *testing.T) {
		t.Parallel()

		backoff := exponentialBackoffWithJitter(100*time.Millisecond, 0.1)
		for _, attempt := range []uint{38, 61, 62, 63, 64, math.MaxUint} {
			assert.Equal(t, time.Duration(math.MaxInt64), backoff(t.Context(), attempt))
		}
	})
}
