// Package crdbprom provides optional Prometheus metrics for crdbpgx.
// Create a [Meter] with an explicit registerer and pass it to crdbpgx.WithHooks.
package crdbprom

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sumup/crdbpgx"
)

// Meter records retry, balancing, and health metrics. Construct it with
// [NewMeter]. It is safe to share across pools and to collect concurrently.
type Meter struct {
	// HooksDefaults supplies no-op callbacks for events this meter does not record.
	crdbpgx.HooksDefaults

	resets       *prometheus.HistogramVec
	acquired     *prometheus.HistogramVec
	pruned       *prometheus.HistogramVec
	healthyNodes prometheus.Gauge
}

var (
	_ prometheus.Collector = (*Meter)(nil)
	_ crdbpgx.Hook         = (*Meter)(nil)
)

// NewMeter creates and registers metrics with registerer, which must be non-nil.
// Registration is performed as one collector, so failure leaves none of this
// meter's metrics registered. Registration conflicts are returned as errors;
// reuse the same meter across pools sharing a registry.
//
// The caller owns the registry and HTTP handler. The meter remains registered
// when a pool closes. To remove it, call registerer.Unregister(meter) after all
// pools and caller-owned trackers using it have stopped.
func NewMeter(registerer prometheus.Registerer) (*Meter, error) {
	if registerer == nil {
		return nil, fmt.Errorf("prometheus registerer must not be nil")
	}
	m := &Meter{
		resets: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "crdbpgx_resets",
			Help:    "CockroachDB client-side transaction reset distribution",
			Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
		}, []string{"pool_id"}),
		acquired: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "crdbpgx_balancer_acquired",
			Help:    "connections acquired for pruning",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50},
		}, []string{"pool_id"}),
		pruned: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "crdbpgx_balancer_pruned",
			Help:    "connections pruned per balancing run",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50},
		}, []string{"pool_id"}),
		healthyNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "crdb_healthy_nodes",
			Help: "Number of healthy CRDB nodes detected",
		}),
	}
	if err := registerer.Register(m); err != nil {
		return nil, fmt.Errorf("register crdbprom metrics: %w", err)
	}
	return m, nil
}

// Describe implements [prometheus.Collector].
func (m *Meter) Describe(ch chan<- *prometheus.Desc) {
	m.resets.Describe(ch)
	m.acquired.Describe(ch)
	m.pruned.Describe(ch)
	m.healthyNodes.Describe(ch)
}

// Collect implements [prometheus.Collector].
func (m *Meter) Collect(ch chan<- prometheus.Metric) {
	m.resets.Collect(ch)
	m.acquired.Collect(ch)
	m.pruned.Collect(ch)
	m.healthyNodes.Collect(ch)
}

// OnOperationEnd records retry counts for completed operations.
// Operations that fail before the first attempt are excluded.
func (m *Meter) OnOperationEnd(_ context.Context, data crdbpgx.OperationEndData) {
	// Acquisition failures before the first attempt have no retry count.
	if data.Attempts == 0 {
		return
	}
	m.resets.WithLabelValues(data.PoolName).Observe(float64(data.Retries))
}

// OnRebalanceEnd records acquired and pruned connections for a balancing run.
func (m *Meter) OnRebalanceEnd(_ context.Context, data crdbpgx.RebalanceEndData) {
	m.acquired.WithLabelValues(data.PoolName).Observe(float64(data.Acquired))
	m.pruned.WithLabelValues(data.PoolName).Observe(float64(data.Pruned))
}

// OnHealthUpdate records the tracker's effective healthy-node count.
func (m *Meter) OnHealthUpdate(data crdbpgx.HealthUpdateData) {
	m.healthyNodes.Set(float64(data.HealthyNodes))
}
