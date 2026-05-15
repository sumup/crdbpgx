package crdbpgx

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestDetachedContextWithoutHooks(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	parent = WithDisableRetries(parent)

	pool := &Pool{}
	detached := pool.detachedContext(parent)

	cancel()

	require.True(t, RetriesDisabled(detached))
	require.NoError(t, detached.Err())
	require.NotSame(t, parent, detached)
}

func TestNodeHealthTrackerHooks(t *testing.T) {
	var events []NodeHealthChangeEvent

	tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{}, WithNodeHealthHooks(Hooks{
		OnNodeHealthChange: func(_ context.Context, event NodeHealthChangeEvent) {
			events = append(events, event)
		},
	}))
	require.NoError(t, err)

	tracker.SetStatus(7, true)

	require.Len(t, events, 1)
	require.Equal(t, NodeHealthChangeEvent{
		NodeID:       7,
		Healthy:      true,
		HealthyNodes: 1,
	}, events[0])
}
