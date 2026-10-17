package crdbprom_test

import (
	"maps"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
	"github.com/sumup/crdbpgx/crdbprom"
)

func TestMeter_Metrics(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewPedanticRegistry()
	meter, err := crdbprom.NewMeter(registry)
	require.NoError(t, err)
	ctx := t.Context()
	meter.OnOperationEnd(ctx, crdbpgx.OperationEndData{
		PoolName: "orders",
		Attempts: 1,
	})
	meter.OnOperationEnd(ctx, crdbpgx.OperationEndData{
		PoolName: "orders",
		Attempts: 3,
		Retries:  2,
	})
	meter.OnOperationEnd(ctx, crdbpgx.OperationEndData{
		PoolName:          "orders",
		Attempts:          3,
		Retries:           2,
		RetryLimitReached: true,
		Err:               assert.AnError,
	})
	meter.OnOperationEnd(ctx, crdbpgx.OperationEndData{
		PoolName: "orders",
		Err:      assert.AnError,
	})
	meter.OnOperationEnd(ctx, crdbpgx.OperationEndData{
		PoolName: "billing",
		Attempts: 1,
	})
	meter.OnRebalanceEnd(ctx, crdbpgx.RebalanceEndData{
		PoolName: "orders",
		Acquired: 8,
		Pruned:   2,
	})
	meter.OnHealthUpdate(crdbpgx.HealthUpdateData{
		HealthyNodes: 3,
	})
	meter.OnHealthUpdate(crdbpgx.HealthUpdateData{
		HealthyNodes: 2,
	})

	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 4)
	retryBounds := []float64{0, 1, 2, 5, 10, 20, 50}
	connectionBounds := []float64{0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50}
	for _, tc := range []struct {
		name   string
		pool   string
		count  uint64
		sum    float64
		bounds []float64
	}{
		{"crdbpgx_resets", "orders", 3, 4, retryBounds},
		{"crdbpgx_resets", "billing", 1, 0, retryBounds},
		{"crdbpgx_balancer_acquired", "orders", 1, 8, connectionBounds},
		{"crdbpgx_balancer_pruned", "orders", 1, 2, connectionBounds},
	} {
		t.Run(tc.name+"/"+tc.pool, func(t *testing.T) {
			t.Parallel()

			metric := findMetric(t, families, tc.name, map[string]string{"pool_id": tc.pool})
			histogram := metric.GetHistogram()
			require.NotNil(t, histogram)
			assert.Equal(t, tc.count, histogram.GetSampleCount())
			assert.Equal(t, tc.sum, histogram.GetSampleSum())

			var bounds []float64
			for _, bucket := range histogram.Bucket {
				bounds = append(bounds, bucket.GetUpperBound())
			}
			assert.Equal(t, tc.bounds, bounds)
		})
	}

	health := findMetric(t, families, "crdb_healthy_nodes", nil).GetGauge()
	require.NotNil(t, health)
	assert.Equal(t, float64(2), health.GetValue())
}

func TestNewMeter_Registration(t *testing.T) {
	t.Parallel()

	t.Run("requires a registerer", func(t *testing.T) {
		t.Parallel()

		meter, err := crdbprom.NewMeter(nil)
		require.Error(t, err)
		assert.Nil(t, meter)
	})

	t.Run("duplicate registration preserves the existing meter", func(t *testing.T) {
		t.Parallel()

		registry := prometheus.NewPedanticRegistry()
		meter, err := crdbprom.NewMeter(registry)
		require.NoError(t, err)
		duplicate, err := crdbprom.NewMeter(registry)
		var registrationErr prometheus.AlreadyRegisteredError
		require.ErrorAs(t, err, &registrationErr)
		assert.Same(t, meter, registrationErr.ExistingCollector)
		assert.Nil(t, duplicate)
		assert.True(t, registry.Unregister(meter))
		_, err = crdbprom.NewMeter(registry)
		require.NoError(t, err)
	})

	t.Run("failed registration leaves no partial collectors", func(t *testing.T) {
		t.Parallel()

		registry := prometheus.NewPedanticRegistry()
		existing := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "crdb_healthy_nodes", Help: "Number of healthy CRDB nodes detected",
		})
		require.NoError(t, registry.Register(existing))
		meter, err := crdbprom.NewMeter(registry)
		require.Error(t, err)
		assert.Nil(t, meter)
		assert.True(t, registry.Unregister(existing))
		// Any histogram registered before the health conflict would prevent retry.
		_, err = crdbprom.NewMeter(registry)
		require.NoError(t, err)
	})
}

func findMetric(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	t.Helper()

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			got := make(map[string]string, len(metric.Label))
			for _, label := range metric.Label {
				got[label.GetName()] = label.GetValue()
			}
			if maps.Equal(labels, got) {
				return metric
			}
		}
	}

	t.Fatalf("metric %q with labels %v not found", name, labels)

	return nil
}
