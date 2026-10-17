package crdbotel_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/sumup/crdbpgx"
	"github.com/sumup/crdbpgx/crdbotel"
)

func TestTracer_QuerySpans(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		err       error
		errorType string
		sqlstate  string
	}{
		{
			name: "success",
		},
		{
			name: "empty result",
			err:  pgx.ErrNoRows,
		},
		{
			name:      "client failure",
			err:       assert.AnError,
			errorType: "*errors.errorString",
		},
		{
			name:      "wrapped client failure",
			err:       fmt.Errorf("query: %w", assert.AnError),
			errorType: "*errors.errorString",
		},
		{
			name:      "database failure without code",
			err:       &pgconn.PgError{Message: "failed"},
			errorType: "*pgconn.PgError",
		},
		{
			name:      "wrapped database failure",
			err:       fmt.Errorf("query: %w", &pgconn.PgError{Code: "40001", Message: "retry transaction"}),
			errorType: "40001",
			sqlstate:  "40001",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			t.Cleanup(func() {
				require.NoError(t, provider.Shutdown(context.Background()))
			})

			tracer := crdbotel.NewTracer(crdbotel.WithTracerProvider(provider))

			ctx, parent := provider.Tracer("test").Start(t.Context(), "caller")
			defer parent.End()

			// An end callback without a matching start must not finish the caller.
			tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: tc.err})
			require.Empty(t, recorder.Ended())

			type contextKey struct{}
			ctx = context.WithValue(context.WithoutCancel(ctx), contextKey{}, "caller")

			queryCtx := tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{
				SQL: "SELECT 'private'",
			})
			assert.Equal(t, "caller", queryCtx.Value(contextKey{}))
			assert.NotEqual(t, parent.SpanContext().SpanID(), trace.SpanFromContext(queryCtx).SpanContext().SpanID())
			require.Empty(t, recorder.Ended(), "query span stays open until pgx completes the query")

			tracer.TraceQueryEnd(queryCtx, nil, pgx.TraceQueryEndData{Err: tc.err})
			require.Len(t, recorder.Ended(), 1)

			span := recorder.Ended()[0]
			assert.Equal(t, "cockroachdb", span.Name())
			assert.Equal(t, trace.SpanKindClient, span.SpanKind())
			assert.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
			assert.Equal(t, parent.SpanContext().TraceID(), span.SpanContext().TraceID())
			assert.Equal(t, "github.com/sumup/crdbpgx", span.InstrumentationScope().Name)
			assert.Equal(t, "https://opentelemetry.io/schemas/1.43.0", span.InstrumentationScope().SchemaURL)
			assert.True(t, parent.IsRecording(), "the caller span stays open")
			wantAttrs := []attribute.KeyValue{attribute.String("db.system.name", "cockroachdb")}
			wantStatus := codes.Unset
			if tc.errorType != "" {
				wantStatus = codes.Error
				wantAttrs = append(wantAttrs, attribute.String("error.type", tc.errorType))
			}
			if tc.sqlstate != "" {
				wantAttrs = append(wantAttrs, attribute.String("db.response.status_code", tc.sqlstate))
			}
			assert.ElementsMatch(t, wantAttrs, span.Attributes(), "only safe database and error attributes are collected")
			assert.Equal(t, sdktrace.Status{Code: wantStatus}, span.Status())
			assert.Empty(t, span.Events(), "returned errors do not produce duplicate exception events")
		})
	}
}

// ambientSpanTracer models a pgx tracer that uses the active context span to
// complete queries, as common pgx instrumentation does.
type ambientSpanTracer struct{ tracer trace.Tracer }

func (t ambientSpanTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	ctx, _ = t.tracer.Start(ctx, "existing")
	return ctx
}

func (ambientSpanTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	trace.SpanFromContext(ctx).End()
}

func TestTracer_Composition(t *testing.T) {
	t.Parallel()

	firstRecorder, secondRecorder := tracetest.NewSpanRecorder(), tracetest.NewSpanRecorder()

	firstProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(firstRecorder))
	t.Cleanup(func() {
		require.NoError(t, firstProvider.Shutdown(context.Background()))
	})

	secondProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(secondRecorder))
	t.Cleanup(func() {
		require.NoError(t, secondProvider.Shutdown(context.Background()))
	})

	first := crdbotel.NewTracer(crdbotel.WithTracerProvider(firstProvider))
	second := crdbotel.NewTracer(crdbotel.WithTracerProvider(secondProvider))
	tracer := multitracer.New(first, second, ambientSpanTracer{firstProvider.Tracer("existing")})

	ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	require.Len(t, firstRecorder.Ended(), 2, "both the hook and existing tracer finish their own spans")
	require.Len(t, secondRecorder.Ended(), 1, "the second hook uses its own provider")
	assert.Equal(t, firstRecorder.Ended()[0].SpanContext().SpanID(), secondRecorder.Ended()[0].Parent().SpanID())
	assert.Equal(t, secondRecorder.Ended()[0].SpanContext().SpanID(), firstRecorder.Ended()[1].Parent().SpanID())
}

func TestTracer_GlobalProvider(t *testing.T) {
	// The global tracer provider is process-wide; keep this test sequential.
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	for _, tracer := range []*crdbotel.Tracer{
		crdbotel.NewTracer(), crdbotel.NewTracer(crdbotel.WithTracerProvider(nil)), {},
	} {
		ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{})
		tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	}
	assert.Len(t, recorder.Ended(), 3)
}

func TestTracer_SpanEvents(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	hooks := crdbotel.NewTracer()
	ctx, span := provider.Tracer("test").Start(t.Context(), "caller")
	ctx = context.WithoutCancel(ctx)

	hooks.OnNodeSwitchStart(ctx, crdbpgx.NodeSwitchStartData{FromNodeID: 1})
	hooks.OnNodeSwitchEnd(ctx, crdbpgx.NodeSwitchEndData{FromNodeID: 1, ToNodeID: 2})
	hooks.OnNodeSwitchStart(ctx, crdbpgx.NodeSwitchStartData{FromNodeID: 2})
	hooks.OnNodeSwitchEnd(ctx, crdbpgx.NodeSwitchEndData{FromNodeID: 2, ToNodeID: 2, Reused: true})
	hooks.OnNodeSwitchStart(ctx, crdbpgx.NodeSwitchStartData{FromNodeID: 2})
	hooks.OnNodeSwitchEnd(ctx, crdbpgx.NodeSwitchEndData{Err: assert.AnError})

	span.End()

	require.Len(t, recorder.Ended(), 1, "hooks add events without creating spans")
	events := recorder.Ended()[0].Events()
	require.Len(t, events, 5)
	assert.Equal(t, "acquiring connection from different node", events[0].Name)
	assert.Equal(t, []attribute.KeyValue{attribute.Int64("current_node_id", 1)}, events[0].Attributes)
	assert.Equal(t, "acquired connection from different node", events[1].Name)
	assert.Equal(t, []attribute.KeyValue{attribute.Int64("node_id", 2)}, events[1].Attributes)
	assert.Equal(t, "only one healthy node available, reuse connection", events[3].Name)
	assert.Equal(t, []attribute.KeyValue{attribute.Int64("current_node_id", 2)}, events[3].Attributes)
}
