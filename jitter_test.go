package crdbpgx_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

func TestJitteredTicker(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const interval = 300 * time.Millisecond
		ticker := crdbpgx.NewJitteredTicker(100*time.Millisecond, &mockJitter{nextJitter: 200 * time.Millisecond})
		t.Cleanup(ticker.Stop)

		ticks := make(chan time.Time, 2)
		go func() {
			for tick := range ticker.C {
				ticks <- tick
			}
		}()
		synctest.Wait()

		start := time.Now()
		time.Sleep(interval - time.Nanosecond)
		synctest.Wait()
		assert.Empty(t, ticks, "must not tick before the jittered interval")

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.Len(t, ticks, 1)
		assert.Equal(t, start.Add(interval), <-ticks)

		time.Sleep(interval)
		synctest.Wait()
		require.Len(t, ticks, 1)
		assert.Equal(t, start.Add(2*interval), <-ticks)
	})
}

func TestJitteredTicker_Stop(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ticker := crdbpgx.NewJitteredTicker(time.Hour, &mockJitter{})
		synctest.Wait()
		ticker.Stop()
		synctest.Wait()

		tick, ok := <-ticker.C
		assert.False(t, ok, "Stop must close the tick channel")
		assert.True(t, tick.IsZero())
	})
}

type mockJitter struct {
	nextJitter time.Duration
}

var _ crdbpgx.Jitter = (*mockJitter)(nil)

func (m *mockJitter) Jitter(d time.Duration) time.Duration {
	return d + m.nextJitter
}
