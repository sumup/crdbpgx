package crdbotel_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/sumup/crdbpgx"
)

func TestMeter_PoolStats(t *testing.T) {
	t.Parallel()

	meter, reader := newTestMeter(t)
	var pools []*crdbpgx.Pool
	for i, name := range []string{"orders", "billing"} {
		cfg, err := pgxpool.ParseConfig("postgres://localhost/test")
		require.NoError(t, err)
		cfg.MaxConns = int32(i + 2)
		tracker, err := crdbpgx.NewNodeHealthTracker(cfg.ConnConfig)
		require.NoError(t, err)
		pool, err := crdbpgx.NewPool(t.Context(), cfg,
			crdbpgx.WithName(name),
			crdbpgx.WithHealthTracker(tracker),
			crdbpgx.WithContextPropagationBehaviour(crdbpgx.ContextPropagationPassthrough),
			crdbpgx.WithHooks(meter),
		)
		require.NoError(t, err)
		t.Cleanup(pool.Close)

		// A canceled acquisition updates real pool stats without opening a connection.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		for range i + 1 {
			_, err := pool.Acquire(ctx)
			require.ErrorIs(t, err, context.Canceled)
		}
		pools = append(pools, pool)
	}

	// Repeated collections report cumulative values rather than counting them twice.
	for range 2 {
		metrics := collectMetrics(t, reader)
		counts := metrics["db.client.connection.count"].Data.(metricdata.Sum[int64])
		require.Len(t, counts.DataPoints, 4)
		assert.False(t, counts.IsMonotonic)
		for _, point := range counts.DataPoints {
			assert.Zero(t, point.Value)
		}
		limits := metrics["db.client.connection.max"].Data.(metricdata.Sum[int64]).DataPoints
		require.Len(t, limits, 2)
		for _, point := range limits {
			name, ok := point.Attributes.Value("db.client.connection.pool.name")
			require.True(t, ok)
			assert.Equal(t, map[string]int64{"orders": 2, "billing": 3}[name.AsString()], point.Value)
		}
		canceled := metrics["crdbpgx.pool.acquires.canceled"].Data.(metricdata.Sum[int64])
		assert.True(t, canceled.IsMonotonic)
		require.Len(t, canceled.DataPoints, 2)
		for _, point := range canceled.DataPoints {
			name, _ := point.Attributes.Value("db.client.connection.pool.name")
			assert.Equal(t, map[string]int64{"orders": 1, "billing": 2}[name.AsString()], point.Value)
		}
		assert.Equal(t, "s", metrics["crdbpgx.pool.acquire.wait.duration"].Unit)
	}

	pools[0].Close()
	pools[0].Close()
	metrics := collectMetrics(t, reader)
	limits := metrics["db.client.connection.max"].Data.(metricdata.Sum[int64]).DataPoints
	require.Len(t, limits, 1)
	name, _ := limits[0].Attributes.Value("db.client.connection.pool.name")
	assert.Equal(t, "billing", name.AsString())
}
