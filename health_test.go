package crdbpgx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

func TestNodeHealthTracker(t *testing.T) {
	tracker, err := crdbpgx.NewNodeHealthChecker("")
	require.NoError(t, err)

	require := require.New(t)

	tracker.SetNodeStatus(1, true)
	require.True(tracker.IsHealthy(1))
	require.False(tracker.IsHealthy(2))
	require.Equal(1, tracker.NumHealthyNodes())

	tracker.SetNodeStatus(2, true)
	require.True(tracker.IsHealthy(1))
	require.True(tracker.IsHealthy(2))
	require.Equal(2, tracker.NumHealthyNodes())

	// Marking a node once isn't enough to mark it unhealth
	tracker.SetNodeStatus(1, false)
	require.True(tracker.IsHealthy(1))
	require.True(tracker.IsHealthy(2))
	require.Equal(2, tracker.NumHealthyNodes())

	tracker.SetNodeStatus(1, false)
	tracker.SetNodeStatus(1, false)
	require.False(tracker.IsHealthy(1))
	require.True(tracker.IsHealthy(2))
	require.Equal(1, tracker.NumHealthyNodes())
}
