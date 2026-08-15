package crdbpgx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolStatsSnapshot(t *testing.T) {
	t.Parallel()

	stats := new(poolStats)
	stats.operations.Add(7)
	stats.retries.Add(6)
	stats.resets.Add(5)
	stats.maxRetryErrors.Add(4)
	stats.balancerRuns.Add(3)
	stats.balancerAcquiredConnections.Add(2)
	stats.balancerPrunedConnections.Add(1)

	snapshot := stats.snapshot(nil, 8)
	assert.Equal(t, uint64(7), snapshot.Operations())
	assert.Equal(t, uint64(6), snapshot.Retries())
	assert.Equal(t, uint64(5), snapshot.Resets())
	assert.Equal(t, uint64(4), snapshot.MaxRetryErrors())
	assert.Equal(t, 8, snapshot.HealthyNodes())
	assert.Equal(t, uint64(3), snapshot.BalancerRuns())
	assert.Equal(t, uint64(2), snapshot.BalancerAcquiredConnections())
	assert.Equal(t, uint64(1), snapshot.BalancerPrunedConnections())
}
