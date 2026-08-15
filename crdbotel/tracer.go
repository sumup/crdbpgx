package crdbotel

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	dbconv "go.opentelemetry.io/otel/semconv/v1.40.0/dbconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/sumup/crdbpgx"
)

const instrumentationName = "github.com/sumup/crdbpgx/crdbotel"

var (
	_ crdbpgx.Tracer        = (*Tracer)(nil)
	_ pgx.QueryTracer       = (*Tracer)(nil)
	_ pgx.BatchTracer       = (*Tracer)(nil)
	_ pgx.CopyFromTracer    = (*Tracer)(nil)
	_ pgx.ConnectTracer     = (*Tracer)(nil)
	_ pgx.PrepareTracer     = (*Tracer)(nil)
	_ pgxpool.AcquireTracer = (*Tracer)(nil)
)

type tracerConfig struct {
	tracerProvider           trace.TracerProvider
	meterProvider            metric.MeterProvider
	tracerAttributes         []attribute.KeyValue
	meterAttributes          []attribute.KeyValue
	spanNameCtxFunc          SpanNameCtxFunc
	includeConnectionDetails bool
	sqlStatementMode         sqlStatementMode
	includeQueryParameters   bool
	includeRowCounts         bool
	disableAcquireTracer     bool
}

// Tracer instruments pgx database operations and CockroachDB-specific crdbpgx
// retry, health, node-selection, and balancing lifecycle events.
type Tracer struct {
	tracer                   trace.Tracer
	attributes               []attribute.KeyValue
	meterAttributes          []attribute.KeyValue
	operationDuration        dbconv.ClientOperationDuration
	connectionCreateTime     dbconv.ClientConnectionCreateTime
	connectionWaitTime       dbconv.ClientConnectionWaitTime
	spanNameCtxFunc          SpanNameCtxFunc
	includeConnectionDetails bool
	sqlStatementMode         sqlStatementMode
	includeQueryParameters   bool
	includeRowCounts         bool
	disableAcquireTracer     bool
}

// NewTracer creates a combined pgx and crdbpgx OpenTelemetry tracer.
func NewTracer(opts ...Option) *Tracer {
	cfg := tracerConfig{
		tracerProvider:           otel.GetTracerProvider(),
		meterProvider:            otel.GetMeterProvider(),
		spanNameCtxFunc:          defaultSpanNameCtxFunc,
		includeConnectionDetails: true,
		sqlStatementMode:         sqlStatementParameterized,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	tracerAttributes := append([]attribute.KeyValue(nil), cfg.tracerAttributes...)
	// Append the required system last so caller-provided attributes cannot
	// accidentally identify these operations as a different database system.
	tracerAttributes = append(tracerAttributes, semconv.DBSystemNameCockroachDB)
	tracer := &Tracer{
		tracer: cfg.tracerProvider.Tracer(
			instrumentationName,
			trace.WithInstrumentationVersion(instrumentationVersion()),
		),
		attributes:               tracerAttributes,
		spanNameCtxFunc:          cfg.spanNameCtxFunc,
		includeConnectionDetails: cfg.includeConnectionDetails,
		sqlStatementMode:         cfg.sqlStatementMode,
		includeQueryParameters:   cfg.includeQueryParameters,
		includeRowCounts:         cfg.includeRowCounts,
		disableAcquireTracer:     cfg.disableAcquireTracer,
	}
	tracer.configureMetrics(cfg.meterProvider, cfg.meterAttributes)
	return tracer
}

func (t *Tracer) TraceOperationStart(
	ctx context.Context,
	data crdbpgx.TraceOperationStartData,
) context.Context {
	attrs := t.spanAttributes(
		semconv.DBClientConnectionPoolName(data.PoolName),
		attribute.String("crdbpgx.operation.name", string(data.Operation)),
	)
	ctx, _ = t.tracer.Start(
		ctx,
		"crdbpgx."+string(data.Operation),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrs...),
	)
	return ctx
}

func (t *Tracer) TraceOperationEnd(ctx context.Context, data crdbpgx.TraceOperationEndData) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.Int64("crdbpgx.operation.retry.count", int64(data.Retries)),
		attribute.Int64("crdbpgx.operation.reset.count", int64(data.Resets)),
		attribute.Bool("crdbpgx.operation.retry.exhausted", data.Exhausted),
	)
	recordError(span, data.Err)
	span.End()
}

func (t *Tracer) TraceRetry(ctx context.Context, data crdbpgx.TraceRetryData) {
	attrs := []attribute.KeyValue{
		attribute.String("crdbpgx.retry.reason", string(data.Reason)),
		attribute.Int64("crdbpgx.retry.attempt", int64(data.Attempt)),
	}
	if data.Err != nil {
		attrs = append(attrs, attribute.String(
			"crdbpgx.retry.error.type",
			errorType(data.Err),
		))
	}
	if data.NodeID != 0 {
		attrs = append(attrs, attribute.Int64("crdbpgx.node.id", int64(data.NodeID)))
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Int64("crdbpgx.operation.retry.count", int64(data.Attempt)))
	span.AddEvent("crdbpgx.retry", trace.WithAttributes(attrs...))
}

func (t *Tracer) TraceAcquireFromDifferentNodeStart(
	ctx context.Context,
	data crdbpgx.TraceAcquireFromDifferentNodeStartData,
) context.Context {
	attrs := t.spanAttributes(
		semconv.DBClientConnectionPoolName(data.PoolName),
		attribute.Int64("crdbpgx.node.current_id", int64(data.CurrentNodeID)),
	)
	ctx, _ = t.tracer.Start(
		ctx,
		"crdbpgx.acquire_from_different_node",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrs...),
	)
	return ctx
}

func (t *Tracer) TraceAcquireFromDifferentNodeEnd(
	ctx context.Context,
	data crdbpgx.TraceAcquireFromDifferentNodeEndData,
) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Bool("crdbpgx.node.reused", data.ReusedSameNode))
	if data.NodeID != 0 {
		span.SetAttributes(attribute.Int64("crdbpgx.node.id", int64(data.NodeID)))
	}
	recordError(span, data.Err)
	span.End()
}

func (t *Tracer) TraceNodeHealthChange(ctx context.Context, data crdbpgx.TraceNodeHealthChangeData) {
	attrs := t.spanAttributes(
		attribute.Int64("crdbpgx.node.id", int64(data.NodeID)),
		attribute.Bool("crdbpgx.node.healthy", data.Healthy),
		attribute.Int("crdbpgx.health.node.count", data.HealthyNodes),
	)
	trace.SpanFromContext(ctx).AddEvent(
		"crdbpgx.node.health_change",
		trace.WithAttributes(attrs...),
	)
}

func (t *Tracer) TraceBalancer(ctx context.Context, data crdbpgx.TraceBalancerData) {
	attrs := t.spanAttributes(
		semconv.DBClientConnectionPoolName(data.PoolName),
		attribute.Int("crdbpgx.health.node.count", data.HealthyNodes),
		attribute.Int("crdbpgx.balancer.connection.acquired.count", data.AcquiredConnections),
		attribute.Int("crdbpgx.balancer.connection.pruned.count", data.PrunedConnections),
	)
	trace.SpanFromContext(ctx).AddEvent(
		"crdbpgx.balancer.rebalance",
		trace.WithAttributes(attrs...),
	)
}

func (t *Tracer) spanAttributes(attrs ...attribute.KeyValue) []attribute.KeyValue {
	all := make([]attribute.KeyValue, 0, len(t.attributes)+len(attrs))
	all = append(all, t.attributes...)
	return append(all, attrs...)
}

func recordError(span trace.Span, err error) {
	if err == nil || errors.Is(err, pgx.ErrNoRows) {
		return
	}
	span.RecordError(err)
	span.SetAttributes(errorAttributes(err)...)
	span.SetStatus(codes.Error, err.Error())
}

func errorAttributes(err error) []attribute.KeyValue {
	if err == nil || errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return []attribute.KeyValue{
			semconv.DBResponseStatusCode(pgErr.Code),
			semconv.ErrorTypeKey.String(pgErr.Code),
		}
	}
	return []attribute.KeyValue{semconv.ErrorType(err)}
}

func errorType(err error) string {
	for _, attr := range errorAttributes(err) {
		if attr.Key == semconv.ErrorTypeKey {
			return attr.Value.AsString()
		}
	}
	return "_OTHER"
}
