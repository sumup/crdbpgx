package crdbpgx_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

type healthTraceRecorder struct {
	crdbpgx.NopTracer

	mu      sync.Mutex
	changes []crdbpgx.TraceNodeHealthChangeData
}

func (r *healthTraceRecorder) TraceNodeHealthChange(
	_ context.Context,
	data crdbpgx.TraceNodeHealthChangeData,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.changes = append(r.changes, data)
}

func (r *healthTraceRecorder) snapshot() []crdbpgx.TraceNodeHealthChangeData {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]crdbpgx.TraceNodeHealthChangeData(nil), r.changes...)
}

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

	t.Run("traces health transitions", func(t *testing.T) {
		t.Parallel()

		tracer := new(healthTraceRecorder)
		tracker, err := crdbpgx.NewNodeHealthTracker(
			&pgx.ConnConfig{},
			crdbpgx.WithNodeHealthTracer(tracer),
		)
		require.NoError(t, err)

		tracker.SetStatusContext(t.Context(), 1, true)
		tracker.SetStatusContext(t.Context(), 1, true)
		tracker.SetStatusContext(t.Context(), 1, false)
		tracker.SetStatusContext(t.Context(), 1, false)
		tracker.SetStatusContext(t.Context(), 1, false)

		require.Equal(t, []crdbpgx.TraceNodeHealthChangeData{
			{NodeID: 1, Healthy: true, HealthyNodes: 1},
			{NodeID: 1, Healthy: false, HealthyNodes: 0},
		}, tracer.snapshot())
	})

	t.Run("notifies topology changes when healthy node becomes unhealthy", func(t *testing.T) {
		t.Parallel()

		tracker, err := crdbpgx.NewNodeHealthTracker(&pgx.ConnConfig{})
		require.NoError(t, err)

		topologyChanges, unsubscribe := tracker.SubscribeTopologyChanges()
		defer unsubscribe()

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
		defer unsubscribe()

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

func requireTopologyChange(t *testing.T, topologyChanges <-chan struct{}) {
	t.Helper()

	select {
	case <-topologyChanges:
	case <-time.After(time.Second):
		t.Fatal("expected topology change")
	}
}

func requireNoTopologyChange(t *testing.T, topologyChanges <-chan struct{}) {
	t.Helper()

	select {
	case <-topologyChanges:
		t.Fatal("unexpected topology change")
	case <-time.After(25 * time.Millisecond):
	}
}
