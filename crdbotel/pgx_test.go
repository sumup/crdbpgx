package crdbotel

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTracer_PGXQuery(t *testing.T) {
	t.Parallel()

	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	metricReader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })

	tracer := NewTracer(
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithIncludeQueryParameters(),
		WithRowCountsInAttributes(),
	)
	ctx, parent := tracerProvider.Tracer("test").Start(t.Context(), "parent")
	parentID := parent.SpanContext().SpanID()
	ctx = tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{
		SQL:  "select * from orders where id = $1",
		Args: []any{42},
	})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{
		CommandTag: pgconn.NewCommandTag("SELECT 2"),
	})
	parent.End()

	span := requireSpan(t, spanRecorder.Ended(), "select")
	assert.Equal(t, sdktrace.Status{Code: codes.Unset}, span.Status())
	assert.Equal(t, trace.SpanKindClient, span.SpanKind())
	assert.Equal(t, parentID, span.Parent().SpanID())
	assertAttribute(t, span.Attributes(), "db.system.name", "cockroachdb")
	assertAttribute(t, span.Attributes(), "db.operation.name", "select")
	assertAttribute(t, span.Attributes(), "db.query.summary", "select")
	assertAttribute(t, span.Attributes(), "db.query.text", "select * from orders where id = $1")
	assertAttribute(t, span.Attributes(), "db.query.parameter.0", "42")
	assertAttribute(t, span.Attributes(), "db.response.returned_rows", int64(2))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(t.Context(), &metrics))
	duration := requireFloat64HistogramMetric(t, metrics, "db.client.operation.duration")
	require.Equal(t, []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10}, duration.Bounds)
	assertAttributeSetValue(t, duration.Attributes, "db.system.name", "cockroachdb")
	assertAttributeSetValue(t, duration.Attributes, "db.operation.name", "select")
	assertAttributeSetValue(t, duration.Attributes, "db.query.summary", "select")
	_, hasErrorType := duration.Attributes.Value(attribute.Key("error.type"))
	assert.False(t, hasErrorType)
}

func TestTracer_PGXQueryError(t *testing.T) {
	t.Parallel()

	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	metricReader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })

	tracer := NewTracer(
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithDisableSQLStatementInAttributes(),
		WithIncludeQueryParameters(),
	)
	ctx, parent := tracerProvider.Tracer("test").Start(t.Context(), "parent")
	ctx = tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{
		SQL:  "select secret from credentials where id = $1",
		Args: []any{"do-not-export"},
	})
	queryErr := &pgconn.PgError{Code: "40001", Message: "serialization failure"}
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: queryErr})
	parent.End()

	span := requireSpan(t, spanRecorder.Ended(), "select")
	assert.Equal(t, codes.Error, span.Status().Code)
	assertAttribute(t, span.Attributes(), "db.system.name", "cockroachdb")
	assertAttribute(t, span.Attributes(), "error.type", "40001")
	assertAttribute(t, span.Attributes(), "db.response.status_code", "40001")
	assertNotAttribute(t, span.Attributes(), "db.query.text")
	assertNotAttribute(t, span.Attributes(), "db.query.parameter.0")

	var metrics metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(t.Context(), &metrics))
	duration := requireFloat64HistogramMetric(t, metrics, "db.client.operation.duration")
	assertAttributeSetValue(t, duration.Attributes, "db.system.name", "cockroachdb")
	assertAttributeSetValue(t, duration.Attributes, "db.operation.name", "select")
	assertAttributeSetValue(t, duration.Attributes, "error.type", "40001")
	assertAttributeSetValue(t, duration.Attributes, "db.response.status_code", "40001")
	assertMetricAbsent(t, metrics, "db.client.operation.errors")
}

func TestTracer_PGXNoRowsIsNotAnError(t *testing.T) {
	t.Parallel()

	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	tracer := NewTracer(WithTracerProvider(provider))
	ctx, parent := provider.Tracer("test").Start(t.Context(), "parent")
	ctx = tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "select 1"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: pgx.ErrNoRows})
	parent.End()

	span := requireSpan(t, spanRecorder.Ended(), "select")
	assert.Equal(t, codes.Unset, span.Status().Code)
	assert.Empty(t, span.Events())
	assertNotAttribute(t, span.Attributes(), "db.query.text")
}

func TestTracer_PGXLifecycleInterfaces(t *testing.T) {
	t.Parallel()

	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	tracer := NewTracer(
		WithTracerProvider(provider),
		WithIncludeQueryParameters(),
		WithRowCountsInAttributes(),
	)
	ctx, parent := provider.Tracer("test").Start(t.Context(), "parent")

	copyCtx := tracer.TraceCopyFromStart(ctx, nil, pgx.TraceCopyFromStartData{
		TableName: pgx.Identifier{"public", "orders"},
	})
	tracer.TraceCopyFromEnd(copyCtx, nil, pgx.TraceCopyFromEndData{
		CommandTag: pgconn.NewCommandTag("COPY 3"),
	})

	batch := &pgx.Batch{}
	batch.Queue("insert into orders (id) values ($1)", 1)
	batch.Queue("insert into orders (id) values ($1)", 2)
	batchCtx := tracer.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{Batch: batch})
	tracer.TraceBatchQuery(batchCtx, nil, pgx.TraceBatchQueryData{
		SQL:        "insert into orders (id) values ($1)",
		Args:       []any{1},
		CommandTag: pgconn.NewCommandTag("INSERT 0 1"),
	})
	tracer.TraceBatchQuery(batchCtx, nil, pgx.TraceBatchQueryData{
		SQL:        "insert into orders (id) values ($1)",
		Args:       []any{2},
		CommandTag: pgconn.NewCommandTag("INSERT 0 1"),
	})
	tracer.TraceBatchEnd(batchCtx, nil, pgx.TraceBatchEndData{})

	connectCtx := tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{})
	tracer.TraceConnectEnd(connectCtx, pgx.TraceConnectEndData{})

	prepareCtx := tracer.TracePrepareStart(ctx, nil, pgx.TracePrepareStartData{
		Name: "update_order",
		SQL:  "update orders set status = $1",
	})
	tracer.TracePrepareEnd(prepareCtx, nil, pgx.TracePrepareEndData{})

	acquireCtx := tracer.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(acquireCtx, nil, pgxpool.TraceAcquireEndData{})
	parent.End()

	spans := spanRecorder.Ended()
	for _, name := range []string{
		"COPY public.orders",
		"BATCH insert",
		"CONNECT",
		"PREPARE update",
		"crdbpgx.pool.acquire",
	} {
		requireSpan(t, spans, name)
	}
	copySpan := requireSpan(t, spans, "COPY public.orders")
	assertAttribute(t, copySpan.Attributes(), "db.operation.name", "COPY")
	assertAttribute(t, copySpan.Attributes(), "db.collection.name", "public.orders")

	batchSpan := requireSpan(t, spans, "BATCH insert")
	assertAttribute(t, batchSpan.Attributes(), "db.operation.name", "BATCH insert")
	assertAttribute(t, batchSpan.Attributes(), "db.query.summary", "BATCH insert")
	assertAttribute(t, batchSpan.Attributes(), "db.operation.batch.size", int64(2))
	assertAttribute(
		t,
		batchSpan.Attributes(),
		"db.query.text",
		"insert into orders (id) values ($1); insert into orders (id) values ($1)",
	)
	assertNotAttribute(t, batchSpan.Attributes(), "db.query.parameter.0")
	require.Len(t, batchSpan.Events(), 2)
	assert.Equal(t, "crdbpgx.batch.query", batchSpan.Events()[0].Name)
	assertAttribute(t, batchSpan.Events()[0].Attributes, "db.operation.name", "insert")
	assertAttribute(t, batchSpan.Events()[0].Attributes, "db.query.summary", "insert")
	assertAttribute(t, batchSpan.Events()[0].Attributes, "db.query.text", "insert into orders (id) values ($1)")
	assertNotAttribute(t, batchSpan.Events()[0].Attributes, "db.query.parameter.0")
	assertAttribute(t, batchSpan.Events()[0].Attributes, string(RowsAffectedKey), int64(1))

	prepareSpan := requireSpan(t, spans, "PREPARE update")
	assertAttribute(t, prepareSpan.Attributes(), string(PrepareStmtNameKey), "update_order")
	assertNotAttribute(t, prepareSpan.Attributes(), "db.query.text")

	acquireSpan := requireSpan(t, spans, "crdbpgx.pool.acquire")
	assert.Equal(t, trace.SpanKindInternal, acquireSpan.SpanKind())
	for _, span := range spans {
		assert.NotEqual(t, "insert", span.Name(), "batch items must be events, not zero-duration spans")
	}
}

func TestTracer_SQLStatementCapturePolicy(t *testing.T) {
	t.Parallel()

	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	defaultTracer := NewTracer(WithTracerProvider(provider))
	allSQLTracer := NewTracer(
		WithTracerProvider(provider),
		WithIncludeSQLStatementInAttributes(),
	)
	ctx, parent := provider.Tracer("test").Start(t.Context(), "parent")
	selectCtx := defaultTracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "select 1"})
	defaultTracer.TraceQueryEnd(selectCtx, nil, pgx.TraceQueryEndData{})
	deleteCtx := allSQLTracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "delete from orders"})
	allSQLTracer.TraceQueryEnd(deleteCtx, nil, pgx.TraceQueryEndData{})
	parent.End()

	selectSpan := requireSpan(t, spanRecorder.Ended(), "select")
	assertNotAttribute(t, selectSpan.Attributes(), "db.query.text")
	assertNotAttribute(t, selectSpan.Attributes(), "db.response.returned_rows")
	deleteSpan := requireSpan(t, spanRecorder.Ended(), "delete")
	assertAttribute(t, deleteSpan.Attributes(), "db.query.text", "delete from orders")
}

func TestTracer_SingleItemBatchIsARegularOperation(t *testing.T) {
	t.Parallel()

	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	tracer := NewTracer(WithTracerProvider(provider))
	batch := &pgx.Batch{}
	batch.Queue("select * from orders where id = $1", 1)
	ctx, parent := provider.Tracer("test").Start(t.Context(), "parent")
	batchCtx := tracer.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{Batch: batch})
	tracer.TraceBatchEnd(batchCtx, nil, pgx.TraceBatchEndData{})
	parent.End()

	span := requireSpan(t, spanRecorder.Ended(), "select")
	assertAttribute(t, span.Attributes(), "db.operation.name", "select")
	assertAttribute(t, span.Attributes(), "db.query.summary", "select")
	assertAttribute(t, span.Attributes(), "db.query.text", "select * from orders where id = $1")
	assertNotAttribute(t, span.Attributes(), "db.operation.batch.size")
}

func TestTracer_DisableAcquireTracer(t *testing.T) {
	t.Parallel()

	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	tracer := NewTracer(WithTracerProvider(provider), WithDisableAcquireTracer())
	ctx, parent := provider.Tracer("test").Start(t.Context(), "parent")
	acquireCtx := tracer.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(acquireCtx, nil, pgxpool.TraceAcquireEndData{})
	parent.End()

	spans := spanRecorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "parent", spans[0].Name())
}

func TestTracer_PGXInterfaces(t *testing.T) {
	t.Parallel()

	tracer := NewTracer()
	require.NotNil(t, tracer)

	var _ pgx.QueryTracer = tracer
	var _ pgx.BatchTracer = tracer
	var _ pgx.CopyFromTracer = tracer
	var _ pgx.ConnectTracer = tracer
	var _ pgx.PrepareTracer = tracer
	var _ pgxpool.AcquireTracer = tracer
}

func requireSpan(
	t *testing.T,
	spans []sdktrace.ReadOnlySpan,
	name string,
) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("span %q not found", name)
	return nil
}

func assertNotAttribute(t *testing.T, attrs []attribute.KeyValue, key string) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			t.Errorf("attribute %q unexpectedly found", key)
			return
		}
	}
}

func assertAttributeSetValue(t *testing.T, attrs attribute.Set, key string, expected any) {
	t.Helper()
	value, ok := attrs.Value(attribute.Key(key))
	require.True(t, ok, "attribute %q not found", key)
	assert.Equal(t, expected, value.AsInterface())
}

func requireFloat64HistogramMetric(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	for _, scope := range metrics.ScopeMetrics {
		for _, candidate := range scope.Metrics {
			if candidate.Name != name {
				continue
			}
			histogram, ok := candidate.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "metric %q has unexpected type %T", name, candidate.Data)
			require.NotEmpty(t, histogram.DataPoints)
			return histogram.DataPoints[0]
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.HistogramDataPoint[float64]{}
}

func assertMetricAbsent(t *testing.T, metrics metricdata.ResourceMetrics, name string) {
	t.Helper()
	for _, scope := range metrics.ScopeMetrics {
		for _, candidate := range scope.Metrics {
			if candidate.Name == name {
				t.Errorf("metric %q unexpectedly found", name)
				return
			}
		}
	}
}
