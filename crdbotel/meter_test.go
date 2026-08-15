package crdbotel

import (
	"context"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/sumup/crdbpgx"
)

type staticHealthTracker struct {
	healthy int
}

func (staticHealthTracker) SetStatus(uint32, bool) {}
func (staticHealthTracker) IsHealthy(uint32) bool  { return true }
func (h staticHealthTracker) NumHealthy() int      { return h.healthy }

func TestRecordStats(t *testing.T) {
	t.Parallel()

	cfg, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:1/db?pool_min_conns=0&pool_min_idle_conns=0&pool_max_conns=7")
	require.NoError(t, err)
	pool, err := crdbpgx.NewPool(
		t.Context(),
		cfg,
		crdbpgx.WithName("orders"),
		crdbpgx.WithHealthTracker(staticHealthTracker{healthy: 3}),
	)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	require.NoError(t, RecordStats(
		pool,
		WithStatsMeterProvider(provider),
		WithStatsAttributes(attribute.String("db.client.connection.pool.name", "wrong")),
	))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))

	healthPoint := requireInt64Metric(t, metrics, "crdbpgx.health.node.count")
	require.Equal(t, int64(3), healthPoint.Value)
	value, ok := healthPoint.Attributes.Value(attribute.Key("db.system.name"))
	require.True(t, ok)
	require.Equal(t, "cockroachdb", value.AsString())
	value, ok = healthPoint.Attributes.Value(attribute.Key("db.client.connection.pool.name"))
	require.True(t, ok)
	require.Equal(t, "orders", value.AsString())

	maxConnections := requireInt64Metric(t, metrics, "db.client.connection.max")
	require.Equal(t, int64(7), maxConnections.Value)
	value, ok = maxConnections.Attributes.Value(attribute.Key("db.client.connection.pool.name"))
	require.True(t, ok)
	require.Equal(t, "orders", value.AsString())
	_, ok = maxConnections.Attributes.Value(attribute.Key("db.system.name"))
	require.False(t, ok, "standard connection pool metrics do not define db.system.name")

	idleConnections := requireInt64MetricWithAttribute(
		t,
		metrics,
		"db.client.connection.count",
		"db.client.connection.state",
		"idle",
	)
	require.Equal(t, int64(0), idleConnections.Value)
	usedConnections := requireInt64MetricWithAttribute(
		t,
		metrics,
		"db.client.connection.count",
		"db.client.connection.state",
		"used",
	)
	require.Equal(t, int64(0), usedConnections.Value)

	require.Equal(t, "{connection}", requireMetric(t, metrics, "db.client.connection.count").Unit)
	require.Equal(t, "{connection}", requireMetric(t, metrics, "db.client.connection.max").Unit)
	require.Equal(t, "s", requireMetric(t, metrics, "crdbpgx.pool.connection.acquire.time").Unit)
	require.Equal(t, "s", requireMetric(t, metrics, "crdbpgx.pool.connection.acquire.wait_time").Unit)

	metricNames := make([]string, 0, 18)
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			metricNames = append(metricNames, metric.Name)
		}
	}
	sort.Strings(metricNames)
	require.Equal(t, []string{
		"crdbpgx.balancer.connection.acquisitions",
		"crdbpgx.balancer.connection.prunes",
		"crdbpgx.balancer.runs",
		"crdbpgx.client.operations",
		"crdbpgx.client.resets",
		"crdbpgx.client.retries",
		"crdbpgx.client.retry.exhaustions",
		"crdbpgx.health.node.count",
		"crdbpgx.pool.connection.acquire.cancellations",
		"crdbpgx.pool.connection.acquire.operations",
		"crdbpgx.pool.connection.acquire.time",
		"crdbpgx.pool.connection.acquire.wait_time",
		"crdbpgx.pool.connection.acquire.waits",
		"crdbpgx.pool.connection.construction.count",
		"crdbpgx.pool.connection.create.operations",
		"crdbpgx.pool.connection.destroy.operations",
		"db.client.connection.count",
		"db.client.connection.max",
	}, metricNames)
}

func requireMetric(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.Metrics {
	t.Helper()
	for _, scope := range metrics.ScopeMetrics {
		for _, candidate := range scope.Metrics {
			if candidate.Name == name {
				return candidate
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func requireInt64Metric(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.DataPoint[int64] {
	t.Helper()

	for _, scope := range metrics.ScopeMetrics {
		for _, candidate := range scope.Metrics {
			if candidate.Name != name {
				continue
			}
			switch data := candidate.Data.(type) {
			case metricdata.Gauge[int64]:
				require.NotEmpty(t, data.DataPoints)
				return data.DataPoints[0]
			case metricdata.Sum[int64]:
				require.NotEmpty(t, data.DataPoints)
				return data.DataPoints[0]
			default:
				t.Fatalf("metric %q has unexpected type %T", name, candidate.Data)
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.DataPoint[int64]{}
}

func requireInt64MetricWithAttribute(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
	key string,
	expected string,
) metricdata.DataPoint[int64] {
	t.Helper()

	metric := requireMetric(t, metrics, name)
	var points []metricdata.DataPoint[int64]
	switch data := metric.Data.(type) {
	case metricdata.Gauge[int64]:
		points = data.DataPoints
	case metricdata.Sum[int64]:
		points = data.DataPoints
	default:
		t.Fatalf("metric %q has unexpected type %T", name, metric.Data)
	}
	for _, point := range points {
		value, ok := point.Attributes.Value(attribute.Key(key))
		if ok && value.AsString() == expected {
			return point
		}
	}
	t.Fatalf("metric %q has no point with %s=%s", name, key, expected)
	return metricdata.DataPoint[int64]{}
}
