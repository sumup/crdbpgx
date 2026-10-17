package crdbotel_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/sumup/crdbpgx"
	"github.com/sumup/crdbpgx/crdbotel"
)

func TestMeter_Metrics(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	hooks := crdbotel.NewMeter(crdbotel.WithMeterProvider(provider))
	ctx := t.Context()

	// Record actual retries, including on exhaustion. Zero-retry successes
	// contribute to the histogram; initial acquisition failures do not.
	hooks.OnOperationEnd(ctx, crdbpgx.OperationEndData{PoolName: "orders", Attempts: 1})
	hooks.OnOperationEnd(ctx, crdbpgx.OperationEndData{PoolName: "orders", Attempts: 3, Retries: 2})
	hooks.OnOperationEnd(ctx, crdbpgx.OperationEndData{
		PoolName: "orders", Attempts: 3, Retries: 2, RetryLimitReached: true, Err: assert.AnError,
	})
	hooks.OnOperationEnd(ctx, crdbpgx.OperationEndData{PoolName: "orders", Err: assert.AnError})
	hooks.OnOperationEnd(ctx, crdbpgx.OperationEndData{PoolName: "billing", Attempts: 1})
	hooks.OnRebalanceEnd(ctx, crdbpgx.RebalanceEndData{PoolName: "orders", Acquired: 8, Pruned: 2})
	hooks.OnHealthUpdate(crdbpgx.HealthUpdateData{HealthyNodes: 3})
	hooks.OnHealthUpdate(crdbpgx.HealthUpdateData{HealthyNodes: 2})

	var resource metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &resource))
	require.Len(t, resource.ScopeMetrics, 1)

	orders := attribute.NewSet(attribute.String("pool.id", "orders"))
	billing := attribute.NewSet(attribute.String("pool.id", "billing"))
	retryBounds := []float64{0, 1, 2, 5, 10, 20, 50}
	connectionBounds := []float64{0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50}

	want := metricdata.ScopeMetrics{
		Scope: instrumentation.Scope{Name: "github.com/sumup/crdbpgx"},
		Metrics: []metricdata.Metrics{
			{
				Name: "crdbpgx.resets", Description: "CockroachDB client-side transaction reset distribution",
				Data: metricdata.Histogram[int64]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[int64]{
						{
							Attributes: orders, Count: 3, Sum: 4, Bounds: retryBounds,
							BucketCounts: []uint64{1, 0, 2, 0, 0, 0, 0, 0},
							Min:          metricdata.NewExtrema[int64](0), Max: metricdata.NewExtrema[int64](2),
						},
						{
							Attributes: billing, Count: 1, Bounds: retryBounds,
							BucketCounts: []uint64{1, 0, 0, 0, 0, 0, 0, 0},
							Min:          metricdata.NewExtrema[int64](0), Max: metricdata.NewExtrema[int64](0),
						},
					},
				},
			},
			{
				Name: "crdbpgx.balancer.acquired", Description: "connections acquired for pruning", Unit: "{connection}",
				Data: metricdata.Histogram[int64]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[int64]{{
						Attributes: orders, Count: 1, Sum: 8, Bounds: connectionBounds,
						BucketCounts: []uint64{0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0},
						Min:          metricdata.NewExtrema[int64](8), Max: metricdata.NewExtrema[int64](8),
					}},
				},
			},
			{
				Name: "crdbpgx.balancer.pruned", Description: "connections pruned per balancing run", Unit: "{connection}",
				Data: metricdata.Histogram[int64]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[int64]{{
						Attributes: orders, Count: 1, Sum: 2, Bounds: connectionBounds,
						BucketCounts: []uint64{0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						Min:          metricdata.NewExtrema[int64](2), Max: metricdata.NewExtrema[int64](2),
					}},
				},
			},
			{
				Name: "crdb_healthy_nodes", Description: "Number of healthy CRDB nodes detected",
				Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{{Value: 2}}},
			},
		},
	}
	metricdatatest.AssertEqual(t, want, resource.ScopeMetrics[0], metricdatatest.IgnoreTimestamp())
}

func TestNewMeter_DefaultProvider(t *testing.T) {
	t.Parallel()
	require.NotNil(t, crdbotel.NewMeter())
	require.NotNil(t, crdbotel.NewMeter(crdbotel.WithMeterProvider(nil)))
}

func TestMeter_Attributes(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	attrs := []attribute.KeyValue{attribute.String("cluster", "production")}
	option := crdbotel.WithMeterAttributes(attrs...)

	attrs[0] = attribute.String("cluster", "mutated")

	meter := crdbotel.NewMeter(
		crdbotel.WithMeterProvider(provider),
		option,
		crdbotel.WithMeterAttributes(attribute.String("component", "database")),
	)
	meter.OnOperationEnd(t.Context(), crdbpgx.OperationEndData{PoolName: "orders", Attempts: 1})
	meter.OnRebalanceEnd(t.Context(), crdbpgx.RebalanceEndData{PoolName: "orders", Acquired: 2, Pruned: 1})
	meter.OnHealthUpdate(crdbpgx.HealthUpdateData{HealthyNodes: 3})

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 4)
	metricdatatest.AssertHasAttributes(t, metrics,
		attribute.String("cluster", "production"),
		attribute.String("component", "database"),
	)
}

func newTestMeter(t *testing.T) (*crdbotel.Meter, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	return crdbotel.NewMeter(crdbotel.WithMeterProvider(provider)), reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()

	var resource metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &resource))

	result := make(map[string]metricdata.Metrics)
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			result[m.Name] = m
		}
	}
	return result
}

func TestMeter_QueryMetrics(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		meter, reader := newTestMeter(t)

		// Query metrics work without a tracing provider or recorded spans.
		for _, err := range []error{nil, assert.AnError} {
			ctx := meter.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{})
			time.Sleep(time.Second)
			meter.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: err})
		}

		metrics := collectMetrics(t, reader)
		duration := metrics["db.client.operation.duration"]
		assert.Equal(t, "s", duration.Unit)
		points := duration.Data.(metricdata.Histogram[float64]).DataPoints
		require.Len(t, points, 2)
		for _, point := range points {
			assert.Equal(t, uint64(1), point.Count)
			assert.Equal(t, float64(1), point.Sum)
		}

		errors := metrics["db.client.operation.errors"].Data.(metricdata.Sum[int64]).DataPoints
		require.Len(t, errors, 1)
		assert.Equal(t, int64(1), errors[0].Value)
	})
}

func TestMeter_BatchMetrics(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		meter, reader := newTestMeter(t)
		ctx := meter.TraceBatchStart(t.Context(), nil, pgx.TraceBatchStartData{})
		time.Sleep(time.Second)
		meter.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{Err: assert.AnError})
		meter.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: assert.AnError})
		meter.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: assert.AnError})

		// pgx can report the same failure from SendBatch and BatchResults.Close.
		metrics := collectMetrics(t, reader)
		points := metrics["db.client.operation.duration"].Data.(metricdata.Histogram[float64]).DataPoints
		require.Len(t, points, 1)
		assert.Equal(t, uint64(1), points[0].Count)
		assert.Equal(t, float64(1), points[0].Sum)

		errors := metrics["db.client.operation.errors"].Data.(metricdata.Sum[int64]).DataPoints
		require.Len(t, errors, 1)
		assert.Equal(t, int64(1), errors[0].Value)
	})
}
