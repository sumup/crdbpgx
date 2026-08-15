package crdbotel

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/sumup/crdbpgx"
)

func TestTracer_CRDBLifecycle(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	tracer := NewTracer(
		WithTracerProvider(provider),
		WithTracerAttributes(
			attribute.String("test.scope", "crdbotel"),
			attribute.String("db.system.name", "postgresql"),
		),
	)
	ctx := tracer.TraceOperationStart(t.Context(), crdbpgx.TraceOperationStartData{
		PoolName:  "orders",
		Operation: crdbpgx.OperationExec,
	})
	tracer.TraceRetry(ctx, crdbpgx.TraceRetryData{
		Reason:  crdbpgx.RetryReasonRetryable,
		Attempt: 1,
		NodeID:  42,
		Err:     errors.New("serialization failure"),
	})
	tracer.TraceNodeHealthChange(ctx, crdbpgx.TraceNodeHealthChangeData{
		NodeID:       42,
		Healthy:      false,
		HealthyNodes: 2,
	})
	tracer.TraceBalancer(ctx, crdbpgx.TraceBalancerData{
		PoolName:            "orders",
		HealthyNodes:        2,
		AcquiredConnections: 3,
		PrunedConnections:   1,
	})
	tracer.TraceOperationEnd(ctx, crdbpgx.TraceOperationEndData{
		Err:     errors.New("operation failed"),
		Retries: 1,
	})

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "crdbpgx.exec", span.Name())
	assert.Equal(t, codes.Error, span.Status().Code)
	assertAttribute(t, span.Attributes(), "db.system.name", "cockroachdb")
	assertAttribute(t, span.Attributes(), "db.client.connection.pool.name", "orders")
	assertAttribute(t, span.Attributes(), "crdbpgx.operation.name", "exec")
	assertNotAttribute(t, span.Attributes(), "db.operation.name")
	assertAttribute(t, span.Attributes(), "crdbpgx.operation.retry.count", int64(1))
	assertAttribute(t, span.Attributes(), "error.type", "*errors.errorString")
	require.Len(t, span.Events(), 4)
	assert.Equal(t, "crdbpgx.retry", span.Events()[0].Name)
	assertAttribute(t, span.Events()[0].Attributes, "crdbpgx.retry.error.type", "*errors.errorString")
	assertNotAttribute(t, span.Events()[0].Attributes, "error.type")
	assert.Equal(t, "crdbpgx.node.health_change", span.Events()[1].Name)
	assertAttribute(t, span.Events()[1].Attributes, "crdbpgx.node.id", int64(42))
	assertAttribute(t, span.Events()[1].Attributes, "crdbpgx.node.healthy", false)
	assertAttribute(t, span.Events()[1].Attributes, "crdbpgx.health.node.count", int64(2))
	assert.Equal(t, "crdbpgx.balancer.rebalance", span.Events()[2].Name)
	assertAttribute(t, span.Events()[2].Attributes, "db.client.connection.pool.name", "orders")
	assertAttribute(t, span.Events()[2].Attributes, "crdbpgx.health.node.count", int64(2))
	assertAttribute(
		t,
		span.Events()[2].Attributes,
		"crdbpgx.balancer.connection.acquired.count",
		int64(3),
	)
	assertAttribute(
		t,
		span.Events()[2].Attributes,
		"crdbpgx.balancer.connection.pruned.count",
		int64(1),
	)
	assert.Equal(t, "exception", span.Events()[3].Name)
}

func TestTracer_TraceRetryUpdatesSpanRetryCount(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	tracer := NewTracer(WithTracerProvider(provider))
	ctx := tracer.TraceOperationStart(t.Context(), crdbpgx.TraceOperationStartData{
		Operation: crdbpgx.OperationQuery,
	})
	tracer.TraceRetry(ctx, crdbpgx.TraceRetryData{Attempt: 2})
	trace.SpanFromContext(ctx).End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes(), "crdbpgx.operation.retry.count", int64(2))
}

func assertAttribute(t *testing.T, attrs []attribute.KeyValue, key string, expected any) {
	t.Helper()

	for _, attr := range attrs {
		if string(attr.Key) != key {
			continue
		}
		assert.Equal(t, expected, attr.Value.AsInterface())
		return
	}
	t.Errorf("attribute %q not found", key)
}
