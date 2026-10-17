package crdbpgx_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

func TestNodeHealthTracker(t *testing.T) {
	t.Parallel()

	t.Run("tracks health", func(t *testing.T) {
		t.Parallel()

		tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{})
		require.NoError(t, err)

		require := require.New(t)

		tracker.SetStatus(1, true)
		require.True(tracker.IsHealthy(1))
		require.False(tracker.IsHealthy(2))
		require.Equal(1, tracker.NumHealthy())

		tracker.SetStatus(2, true)
		require.True(tracker.IsHealthy(1))
		require.True(tracker.IsHealthy(2))
		require.Equal(2, tracker.NumHealthy())

		// Marking a node once isn't enough to mark it unhealth
		tracker.SetStatus(1, false)
		require.True(tracker.IsHealthy(1))
		require.True(tracker.IsHealthy(2))
		require.Equal(2, tracker.NumHealthy())

		tracker.SetStatus(1, false)
		tracker.SetStatus(1, false)
		require.False(tracker.IsHealthy(1))
		require.True(tracker.IsHealthy(2))
		require.Equal(1, tracker.NumHealthy())
	})

	t.Run("notifies topology changes when healthy node becomes unhealthy", func(t *testing.T) {
		t.Parallel()

		tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{})
		require.NoError(t, err)

		topologyChanges, unsubscribe := tracker.SubscribeTopologyChanges()
		t.Cleanup(unsubscribe)

		tracker.SetStatus(1, true)
		requireNoTopologyChange(t, topologyChanges)

		tracker.SetStatus(1, false)
		tracker.SetStatus(1, false)
		requireNoTopologyChange(t, topologyChanges)

		tracker.SetStatus(1, false)
		requireTopologyChange(t, topologyChanges)

		tracker.SetStatus(1, false)
		requireNoTopologyChange(t, topologyChanges)
	})

	t.Run("notifies topology changes when a new node becomes healthy", func(t *testing.T) {
		t.Parallel()

		tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{})
		require.NoError(t, err)

		topologyChanges, unsubscribe := tracker.SubscribeTopologyChanges()
		t.Cleanup(unsubscribe)

		tracker.SetStatus(1, true)
		requireNoTopologyChange(t, topologyChanges)

		tracker.SetStatus(1, true)
		requireNoTopologyChange(t, topologyChanges)

		tracker.SetStatus(2, true)
		requireTopologyChange(t, topologyChanges)

		tracker.SetStatus(2, true)
		requireNoTopologyChange(t, topologyChanges)
	})

	t.Run("unsubscribes topology changes", func(t *testing.T) {
		t.Parallel()

		tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{})
		require.NoError(t, err)

		topologyChanges, unsubscribe := tracker.SubscribeTopologyChanges()
		unsubscribe()
		unsubscribe()

		tracker.SetStatus(1, true)
		tracker.SetStatus(2, true)

		requireNoTopologyChange(t, topologyChanges)
	})

	t.Run("poll context cancellation", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{}, crdbpgx.WithPollInterval(time.Hour))
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				tracker.Poll(ctx)
			}()
			synctest.Wait()
			cancel()
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Fatal("Poll did not stop after cancellation")
			}
		})
	})

	t.Run("refills the failure allowance", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{})
			require.NoError(t, err)
			tracker.SetStatus(1, true)
			tracker.SetStatus(1, false)
			tracker.SetStatus(1, false)
			require.True(t, tracker.IsHealthy(1))

			time.Sleep(time.Minute)
			tracker.SetStatus(1, false)
			require.True(t, tracker.IsHealthy(1), "one failure is allowed after refill")
			tracker.SetStatus(1, false)
			require.False(t, tracker.IsHealthy(1))
		})
	})

	t.Run("recovery resets the failure allowance", func(t *testing.T) {
		t.Parallel()

		tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{})
		require.NoError(t, err)
		tracker.SetStatus(1, true)
		for range 3 {
			tracker.SetStatus(1, false)
		}
		require.False(t, tracker.IsHealthy(1))

		tracker.SetStatus(1, true)
		tracker.SetStatus(1, false)
		tracker.SetStatus(1, false)
		require.True(t, tracker.IsHealthy(1))
		tracker.SetStatus(1, false)
		require.False(t, tracker.IsHealthy(1))
	})
}

func requireTopologyChange(t *testing.T, topologyChanges <-chan struct{}) {
	t.Helper()

	select {
	case <-topologyChanges:
	default:
		t.Fatal("expected topology change")
	}
}

func requireNoTopologyChange(t *testing.T, topologyChanges <-chan struct{}) {
	t.Helper()

	select {
	case <-topologyChanges:
		t.Fatal("unexpected topology change")
	default:
	}
}
