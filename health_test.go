package crdbpgx_test

import (
	"context"
	"testing"
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

	t.Run("poll context cancelation", func(t *testing.T) {
		tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{}, crdbpgx.WithPollInterval(time.Hour))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			tracker.Poll(ctx)
		}()

		cancel()

		require.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)
	})
}
