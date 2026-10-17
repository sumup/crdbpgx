package crdbpgx

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeConnectionBalancer_Run(t *testing.T) {
	t.Parallel()

	testTopologyChange := func(t *testing.T, change func(*nodeHealthTracker)) {
		t.Helper()

		synctest.Test(t, func(t *testing.T) {
			tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{})
			require.NoError(t, err)
			tracker.SetStatus(1, true)

			balancer := NewNodeConnectionBalancer(
				WithBalancingInterval(time.Hour),
				WithTopologyChangeBalancingInterval(time.Minute, 3*time.Minute),
			).(*nodeConnectionBalancer)
			var rebalances atomic.Int64
			balancer.rebalance = func(context.Context, *Pool) { rebalances.Add(1) }

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				balancer.Run(ctx, &Pool{health: tracker})
			}()
			synctest.Wait()
			assert.Zero(t, rebalances.Load())

			change(tracker)
			synctest.Wait()
			assert.Equal(t, int64(1), rebalances.Load(), "topology change triggers an immediate rebalance")

			time.Sleep(time.Minute)
			synctest.Wait()
			assert.Equal(t, int64(2), rebalances.Load(), "rebalance uses the accelerated interval")

			time.Sleep(2 * time.Minute)
			synctest.Wait()
			assert.Equal(t, int64(4), rebalances.Load())

			time.Sleep(time.Hour - time.Nanosecond)
			synctest.Wait()
			assert.Equal(t, int64(4), rebalances.Load(), "accelerated balancing stops when the window expires")

			time.Sleep(time.Nanosecond)
			synctest.Wait()
			assert.Equal(t, int64(5), rebalances.Load(), "regular balancing resumes")

			cancel()
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Fatal("balancer did not stop after cancellation")
			}
		})
	}

	t.Run("node left", func(t *testing.T) {
		t.Parallel()

		testTopologyChange(t, func(tracker *nodeHealthTracker) {
			for range 3 {
				tracker.SetStatus(1, false)
			}
		})
	})

	t.Run("node joined", func(t *testing.T) {
		t.Parallel()

		testTopologyChange(t, func(tracker *nodeHealthTracker) {
			tracker.SetStatus(2, true)
		})
	})
}
