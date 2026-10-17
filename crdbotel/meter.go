// Package crdbotel provides optional OpenTelemetry instrumentation for crdbpgx.
// Register a [Meter], a [Tracer], or both with crdbpgx.WithHooks. A Tracer creates
// query/batch spans and adds connection failover events to the caller's span.
package crdbotel

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/dbconv"

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

// Meter records query/batch durations and errors, retries, balancing, and health.
// Construct it with [NewMeter] and register it with [crdbpgx.WithHooks]. It is safe
// to share across pools. A caller-owned health tracker uses [crdbpgx.WithHealthTrackerHooks].
type Meter struct {
	// HooksDefaults supplies no-op callbacks for events this meter does not record.
	crdbpgx.HooksDefaults

	resets       metric.Int64Histogram
	acquired     metric.Int64Histogram
	pruned       metric.Int64Histogram
	healthyNodes metric.Int64Gauge
	attributes   attribute.Set
	duration     metric.Float64Histogram
	errors       metric.Int64Counter
}

var _ crdbpgx.Hook = (*Meter)(nil)
var _ pgx.BatchTracer = (*Meter)(nil)

type meterOperationKey struct {
	meter *Meter
	batch bool
}

type meterOperation struct {
	start time.Time
	attrs []attribute.KeyValue
	ended atomic.Bool
}

// NewMeter creates query, batch, retry, balancing, and health metrics for [crdbpgx.WithHooks].
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
	duration, err := dbconv.NewClientOperationDuration(meter,
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		otel.Handle(fmt.Errorf("initialize database operation duration histogram: %w", err))
	}
	operationErrors, err := meter.Int64Counter("db.client.operation.errors",
		metric.WithDescription("The count of database client operation errors"),
	)
	if err != nil {
		otel.Handle(fmt.Errorf("initialize database operation errors counter: %w", err))
		operationErrors = noop.Int64Counter{}
	}
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
		duration:   duration.Inst(), errors: operationErrors,
	}
}

// TraceQueryStart starts timing an individual Exec, Query, or QueryRow attempt.
// Metrics use connection and error attributes, never SQL text or parameter values.
func (m *Meter) TraceQueryStart(ctx context.Context, conn *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return m.startOperation(ctx, conn, false)
}

// TraceQueryEnd records duration and, on failure, an error independently of spans.
func (m *Meter) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	m.endOperation(ctx, false, data.Err)
}

// TraceBatchStart starts timing one SendBatch operation.
func (m *Meter) TraceBatchStart(ctx context.Context, conn *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return m.startOperation(ctx, conn, true)
}

// TraceBatchQuery is a no-op: batches contribute one duration and error count at
// batch end, rather than counting each statement and the whole batch together.
func (*Meter) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

// TraceBatchEnd records the aggregate operation, including failures before any
// statement completes. Empty results are not classified as failures.
func (m *Meter) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	m.endOperation(ctx, true, data.Err)
}

func (m *Meter) startOperation(ctx context.Context, conn *pgx.Conn, batch bool) context.Context {
	start := time.Now()
	var cfg *pgx.ConnConfig
	if conn != nil {
		cfg = conn.Config()
	}
	_, attrs := querySpanAttributes(cfg)
	if batch {
		attrs = append(attrs, semconv.DBQuerySummary("BATCH"))
	}
	return context.WithValue(ctx, meterOperationKey{m, batch}, &meterOperation{start: start, attrs: attrs})
}

func (m *Meter) endOperation(ctx context.Context, batch bool, err error) {
	operation, ok := ctx.Value(meterOperationKey{m, batch}).(*meterOperation)
	// pgx can report an early batch failure from SendBatch and report it again
	// when BatchResults.Close is called. Record that operation only once.
	if !ok || operation.ended.Swap(true) {
		return
	}
	errorAttrs := operationErrorAttributes(err)
	attrs := append(slices.Clone(operation.attrs), errorAttrs...)
	opts := metric.WithAttributes(attrs...)
	m.duration.Record(ctx, time.Since(operation.start).Seconds(), metric.WithAttributeSet(m.attributes), opts)
	if len(errorAttrs) > 0 {
		m.errors.Add(ctx, 1, metric.WithAttributeSet(m.attributes), opts)
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
