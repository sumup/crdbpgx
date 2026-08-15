package crdbprom

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promdto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

type staticHealthTracker struct {
	healthy int
}

func (staticHealthTracker) SetStatus(uint32, bool) {}
func (staticHealthTracker) IsHealthy(uint32) bool  { return true }
func (h staticHealthTracker) NumHealthy() int      { return h.healthy }

func TestCollector(t *testing.T) {
	t.Parallel()

	cfg, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:1/db?pool_min_conns=0&pool_min_idle_conns=0&pool_max_conns=5")
	require.NoError(t, err)
	pool, err := crdbpgx.NewPool(
		t.Context(),
		cfg,
		crdbpgx.WithName("orders"),
		crdbpgx.WithHealthTracker(staticHealthTracker{healthy: 2}),
	)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	balancer := crdbpgx.NewNodeConnectionBalancer()
	balancer.Rebalance(t.Context(), pool)

	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(NewCollector(
		pool,
		WithNamespace("test"),
		WithLabels(prometheus.Labels{"zone": "eu-1"}),
	)))

	metrics, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, metrics, 21)

	health := requireMetricFamily(t, metrics, "test_health_nodes").Metric[0]
	require.Equal(t, float64(2), health.GetGauge().GetValue())
	requireMetricLabel(t, health, "pool", "orders")
	requireMetricLabel(t, health, "zone", "eu-1")

	balancerRuns := requireMetricFamily(t, metrics, "test_balancer_runs_total").Metric[0]
	require.Equal(t, float64(1), balancerRuns.GetCounter().GetValue())

	maxConnections := requireMetricFamily(t, metrics, "test_pool_max_connections").Metric[0]
	require.Equal(t, float64(5), maxConnections.GetGauge().GetValue())

	require.Equal(
		t,
		promdto.MetricType_COUNTER,
		requireMetricFamily(t, metrics, "test_pool_acquire_seconds_total").GetType(),
	)
	require.Equal(
		t,
		promdto.MetricType_COUNTER,
		requireMetricFamily(t, metrics, "test_client_retry_exhaustions_total").GetType(),
	)
}

func requireMetricFamily(
	t *testing.T,
	metrics []*promdto.MetricFamily,
	name string,
) *promdto.MetricFamily {
	t.Helper()

	for _, metric := range metrics {
		if metric.GetName() == name {
			return metric
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func requireMetricLabel(t *testing.T, metric *promdto.Metric, name, value string) {
	t.Helper()

	for _, label := range metric.Label {
		if label.GetName() == name {
			require.Equal(t, value, label.GetValue())
			return
		}
	}
	t.Errorf("metric label %q not found", name)
}
