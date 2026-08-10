package crdbpgx

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestNodeHealthTracker_ExpiresInactiveNodes(t *testing.T) {
	t.Parallel()

	tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{})
	require.NoError(t, err)

	topologyChanges, unsubscribe := tracker.SubscribeTopologyChanges()
	t.Cleanup(unsubscribe)

	tracker.SetStatus(1, true)
	tracker.expireStaleNodes(time.Now().Add(defaultNodeStaleTimeout + time.Second))

	require.False(t, tracker.IsHealthy(1))
	require.Equal(t, 0, tracker.NumHealthy())
	select {
	case <-topologyChanges:
	default:
		t.Fatal("expected topology change after stale node expiry")
	}
}

func TestNodeHealthTracker_DoesNotExpireNodesWithLiveConnections(t *testing.T) {
	t.Parallel()

	tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{})
	require.NoError(t, err)

	tracker.connectionOpened(1)
	tracker.connectionOpened(1)
	future := time.Now().Add(defaultNodeStaleTimeout + time.Second)
	tracker.expireStaleNodes(future)
	require.True(t, tracker.IsHealthy(1))

	tracker.connectionClosed(1)
	tracker.expireStaleNodes(future)
	require.True(t, tracker.IsHealthy(1))

	tracker.connectionClosed(1)
	tracker.expireStaleNodes(future)
	require.False(t, tracker.IsHealthy(1))
}

func TestNodeHealthTracker_KeepsRecentlyDiscoveredNodes(t *testing.T) {
	t.Parallel()

	tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{})
	require.NoError(t, err)

	tracker.SetStatus(1, true)
	tracker.expireStaleNodes(time.Now().Add(defaultNodeStaleTimeout - time.Second))

	require.True(t, tracker.IsHealthy(1))
}
