// Package crdbotel provides optional OpenTelemetry instrumentation for crdbpgx.
// Register a [Meter], a [Tracer], or both with crdbpgx.WithHooks. A Tracer creates
// query spans and adds connection failover events to the caller's span.
package crdbotel

import (
	"context"
	"fmt"
	"slices"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/sumup/crdbpgx"
)

type meterConfig struct {
	meterProvider metric.MeterProvider
	attributes    []attribute.KeyValue
}

// MeterOption configures OpenTelemetry metrics.
type MeterOption func(*meterConfig)

// WithMeterProvider configures the provider used for metrics. A nil provider
// uses the global OpenTelemetry meter provider.
func WithMeterProvider(provider metric.MeterProvider) MeterOption {
	return func(cfg *meterConfig) {
		cfg.meterProvider = provider
	}
}

// WithMeterAttributes adds attributes to every metric. Repeated calls append
// attributes; later values win for duplicate keys. The configured pool name
// takes precedence over any additional pool.id attribute on pool metrics.
func WithMeterAttributes(attrs ...attribute.KeyValue) MeterOption {
	attrs = slices.Clone(attrs)
	return func(cfg *meterConfig) {
		cfg.attributes = append(cfg.attributes, attrs...)
	}
}

// Meter records retry, balancing, and health metrics. Construct it with
// [NewMeter] and register it with [crdbpgx.WithHooks]. It is safe to share across
// pools. A caller-owned health tracker uses [crdbpgx.WithHealthTrackerHooks].
type Meter struct {
	// HooksDefaults supplies no-op callbacks for events this meter does not record.
	crdbpgx.HooksDefaults

	resets       metric.Int64Histogram
	acquired     metric.Int64Histogram
	pruned       metric.Int64Histogram
	healthyNodes metric.Int64Gauge
	attributes   attribute.Set
}

var _ crdbpgx.Hook = (*Meter)(nil)

// NewMeter creates retry, balancing, and health metrics for [crdbpgx.WithHooks].
// The caller owns provider setup, flushing, and shutdown. Tracing is enabled
// separately with [NewTracer].
// Instrument creation failures are reported through [otel.Handle]; failed
// instruments become no-ops while the remaining metrics stay enabled.
func NewMeter(opts ...MeterOption) *Meter {
	cfg := meterConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.meterProvider == nil {
		cfg.meterProvider = otel.GetMeterProvider()
	}
	meter := cfg.meterProvider.Meter("github.com/sumup/crdbpgx")
	resets, err := meter.Int64Histogram("crdbpgx.resets",
		metric.WithDescription("CockroachDB client-side transaction reset distribution"),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 5, 10, 20, 50),
	)
	if err != nil {
		otel.Handle(fmt.Errorf("initialize crdb client resets histogram: %w", err))
		resets = noop.Int64Histogram{}
	}
	acquired, err := meter.Int64Histogram("crdbpgx.balancer.acquired",
		metric.WithUnit("{connection}"),
		metric.WithDescription("connections acquired for pruning"),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50),
	)
	if err != nil {
		otel.Handle(fmt.Errorf("initialize crdb balancer acquire histogram: %w", err))
		acquired = noop.Int64Histogram{}
	}
	pruned, err := meter.Int64Histogram("crdbpgx.balancer.pruned",
		metric.WithUnit("{connection}"),
		metric.WithDescription("connections pruned per balancing run"),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50),
	)
	if err != nil {
		otel.Handle(fmt.Errorf("initialize crdb balancer prune histogram: %w", err))
		pruned = noop.Int64Histogram{}
	}
	healthyNodes, err := meter.Int64Gauge("crdb_healthy_nodes",
		metric.WithDescription("Number of healthy CRDB nodes detected"),
	)
	if err != nil {
		otel.Handle(fmt.Errorf("initialize crdb health nodes gauge: %w", err))
		healthyNodes = noop.Int64Gauge{}
	}

	return &Meter{
		resets: resets, acquired: acquired, pruned: pruned, healthyNodes: healthyNodes,
		attributes: attribute.NewSet(cfg.attributes...),
	}
}

// OnOperationEnd records retry counts for completed operations.
// Operations that fail before the first attempt are excluded.
func (m *Meter) OnOperationEnd(ctx context.Context, data crdbpgx.OperationEndData) {
	// Acquisition failures before the first attempt have no retry count.
	if data.Attempts == 0 {
		return
	}
	m.resets.Record(ctx, int64(data.Retries), metric.WithAttributeSet(m.attributes), metric.WithAttributes(attribute.String("pool.id", data.PoolName)))
}

// OnRebalanceEnd records acquired and pruned connections for a balancing run.
func (m *Meter) OnRebalanceEnd(ctx context.Context, data crdbpgx.RebalanceEndData) {
	attrs := metric.WithAttributes(attribute.String("pool.id", data.PoolName))
	m.acquired.Record(ctx, int64(data.Acquired), metric.WithAttributeSet(m.attributes), attrs)
	m.pruned.Record(ctx, int64(data.Pruned), metric.WithAttributeSet(m.attributes), attrs)
}

// OnHealthUpdate records the tracker's effective healthy-node count.
func (m *Meter) OnHealthUpdate(data crdbpgx.HealthUpdateData) {
	m.healthyNodes.Record(context.Background(), int64(data.HealthyNodes), metric.WithAttributeSet(m.attributes))
}
