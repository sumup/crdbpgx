package crdbpgx

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticHealthTracker struct {
	healthy int
}

func (staticHealthTracker) SetStatus(uint32, bool) {}
func (staticHealthTracker) IsHealthy(uint32) bool  { return true }
func (h staticHealthTracker) NumHealthy() int      { return h.healthy }

type balancerTraceRecorder struct {
	NopTracer

	mu     sync.Mutex
	events []TraceBalancerData
}

func (r *balancerTraceRecorder) TraceBalancer(_ context.Context, data TraceBalancerData) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, data)
}

func (r *balancerTraceRecorder) snapshot() []TraceBalancerData {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]TraceBalancerData(nil), r.events...)
}

func Test_maxConnsPerNode(t *testing.T) {
	t.Parallel()

	t.Run("allows any node to hold the remainder connection", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 4, maxConnsPerNode(10, 3))
	})

	t.Run("does not add headroom when connections divide evenly", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 5, maxConnsPerNode(10, 2))
	})
}

func TestNodeConnectionBalancer_StatsAndTracing(t *testing.T) {
	t.Parallel()

	cfg, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:1/db?pool_min_conns=0&pool_min_idle_conns=0")
	require.NoError(t, err)

	pool, err := NewPool(t.Context(), cfg, WithHealthTracker(staticHealthTracker{healthy: 2}))
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	tracer := new(balancerTraceRecorder)
	balancer := NewNodeConnectionBalancer(WithBalancerTracer(tracer))
	balancer.Rebalance(t.Context(), pool)

	stats := pool.Stat()
	require.Equal(t, uint64(1), stats.BalancerRuns())
	require.Zero(t, stats.BalancerAcquiredConnections())
	require.Zero(t, stats.BalancerPrunedConnections())
	require.Equal(t, []TraceBalancerData{{PoolName: "crdbpgx", HealthyNodes: 2}}, tracer.snapshot())
}
