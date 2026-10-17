package crdbotel

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type queryTestConn struct{ net.Conn }

func (queryTestConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 26257}
}

func newQueryTestConn(t *testing.T) *pgx.Conn {
	t.Helper()
	var sessions sync.WaitGroup
	t.Cleanup(sessions.Wait)
	cfg, err := pgx.ParseConfig("postgres://test@127.0.0.1:26257/orders?sslmode=disable")
	require.NoError(t, err)
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		sessions.Go(func() {
			defer func() { assert.NoError(t, server.Close()) }()
			backend := pgproto3.NewBackend(server, server)
			if _, err := backend.ReceiveStartupMessage(); !assert.NoError(t, err) {
				return
			}
			backend.Send(&pgproto3.AuthenticationOk{})
			backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if !assert.NoError(t, backend.Flush()) {
				return
			}
			_, _ = backend.Receive() // Connection cleanup sends Terminate or closes the pipe.
		})
		return queryTestConn{client}, nil
	}
	conn, err := pgx.ConnectConfig(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, conn.Close(ctx))
	})
	return conn
}

func recordQuerySpan(t *testing.T, ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData, opts ...TracerOption) sdktrace.ReadOnlySpan {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	options := append([]TracerOption{WithTracerProvider(provider)}, opts...)
	tracer := NewTracer(options...)
	ctx = tracer.TraceQueryStart(ctx, conn, data)
	tracer.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{})
	require.Len(t, recorder.Ended(), 1)
	return recorder.Ended()[0]
}

func spanAttributeMap(span sdktrace.ReadOnlySpan) map[string]any {
	attrs := make(map[string]any)
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsInterface()
	}
	return attrs
}

func TestTracer_Options(t *testing.T) {
	t.Parallel()
	type nameKey struct{}
	for _, tc := range []struct {
		name       string
		options    []TracerOption
		spanName   string
		summary    string
		disableSQL bool
	}{
		{
			name:     "defaults",
			spanName: "cockroachdb",
		},
		{
			name: "custom name",
			options: []TracerOption{WithSpanNameFunc(func(_ context.Context, sql string) string {
				return "FetchOrder"
			})},
			spanName: "FetchOrder",
			summary:  "FetchOrder",
		},
		{
			name: "context name",
			options: []TracerOption{WithSpanNameFunc(func(ctx context.Context, sql string) string {
				return ctx.Value(nameKey{}).(string) + " " + strings.Fields(sql)[0]
			})},
			spanName: "FetchOrder SELECT",
			summary:  "FetchOrder SELECT",
		},
		{
			name: "empty name fallback",
			options: []TracerOption{WithSpanNameFunc(func(context.Context, string) string {
				return ""
			})},
			spanName: "cockroachdb",
		},
		{
			name: "nil restores default",
			options: []TracerOption{
				WithSpanNameFunc(func(context.Context, string) string {
					return "unused"
				}),
				WithSpanNameFunc(nil),
			},
			spanName: "cockroachdb",
		},
		{
			name: "SQL disabled independently of naming",
			options: []TracerOption{
				WithDisableSQLStatementInAttributes(),
				WithSpanNameFunc(func(context.Context, string) string {
					return "FetchOrder"
				}),
			},
			spanName:   "FetchOrder",
			summary:    "FetchOrder",
			disableSQL: true,
		},
		{
			name: "bounded summary",
			options: []TracerOption{
				WithSpanNameFunc(func(context.Context, string) string {
					return strings.Repeat("€", 100)
				}),
			},
			spanName: strings.Repeat("€", 85),
			summary:  strings.Repeat("€", 85),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.WithValue(t.Context(), nameKey{}, "FetchOrder")
			span := recordQuerySpan(t, ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT $1", Args: []any{1}}, tc.options...)
			attrs := spanAttributeMap(span)
			assert.Equal(t, tc.spanName, span.Name())
			assert.Equal(t, "cockroachdb", attrs["db.system.name"])
			if tc.summary == "" {
				assert.NotContains(t, attrs, "db.query.summary")
			} else {
				assert.Equal(t, tc.summary, attrs["db.query.summary"])
			}
			if tc.disableSQL {
				assert.NotContains(t, attrs, "db.query.text")
			} else {
				assert.Equal(t, "SELECT $1", attrs["db.query.text"])
			}
		})
	}
}

func TestTracer_ConnectionDetails(t *testing.T) {
	t.Parallel()
	for _, disabled := range []bool{false, true} {
		name := "enabled"
		if disabled {
			name = "disabled"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var opts []TracerOption
			if disabled {
				opts = append(opts, WithDisableConnectionDetailsInAttributes())
			}
			span := recordQuerySpan(t, t.Context(), newQueryTestConn(t), pgx.TraceQueryStartData{}, opts...)
			want := []attribute.KeyValue{attribute.String("db.system.name", "cockroachdb")}
			if disabled {
				assert.Equal(t, "cockroachdb", span.Name())
			} else {
				assert.Equal(t, "orders", span.Name())
				want = append(want,
					attribute.String("db.namespace", "orders"),
					attribute.String("server.address", "127.0.0.1"),
					attribute.String("network.peer.address", "127.0.0.2"),
					attribute.Int("network.peer.port", 26257),
				)
			}
			assert.ElementsMatch(t, want, span.Attributes())
		})
	}
}

func TestTracer_QueryParameters(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		data    pgx.TraceQueryStartData
		options []TracerOption
		want    map[string]any
	}{
		{
			name:    "opt-in positional",
			data:    pgx.TraceQueryStartData{SQL: "SELECT $1, $2", Args: []any{pgx.QueryExecModeExec, "value", 7}},
			options: []TracerOption{WithIncludeQueryParameters()},
			want:    map[string]any{"db.query.parameter.0": "value", "db.query.parameter.1": "7"},
		},
		{
			name: "named excludes unused values",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT '@unused', @value -- @unused",
				Args: []any{pgx.NamedArgs{"value": "recorded", "unused": "omitted"}},
			},
			options: []TracerOption{WithIncludeQueryParameters()},
			want:    map[string]any{"db.query.parameter.value": "recorded"},
		},
		{
			name: "strict named",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT @value",
				Args: []any{pgx.StrictNamedArgs{"value": 7}},
			},
			options: []TracerOption{WithIncludeQueryParameters()},
			want:    map[string]any{"db.query.parameter.value": "7"},
		},
		{
			name: "filter permits query",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT $1",
				Args: []any{7},
			},
			options: []TracerOption{
				WithIncludeQueryParameters(func(sql string) bool {
					return sql == "SELECT $1"
				}),
			},
			want: map[string]any{"db.query.parameter.0": "7"},
		},
		{
			name: "filter excludes query",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT $1",
				Args: []any{7},
			},
			options: []TracerOption{
				WithIncludeQueryParameters(func(string) bool {
					return false
				}),
			},
		},
		{
			name: "SQL disabling wins",
			data: pgx.TraceQueryStartData{SQL: "SELECT $1", Args: []any{7}},
			options: []TracerOption{
				WithDisableSQLStatementInAttributes(),
				WithIncludeQueryParameters(func(string) bool {
					panic("the filter must not run when SQL collection is disabled")
				}),
			},
		},
		{
			name: "opaque rewriter",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT $1",
				Args: []any{testQueryRewriter{}, 7},
			},
			options: []TracerOption{WithIncludeQueryParameters()},
		},
		{
			name: "disabled by default",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT $1",
				Args: []any{7},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			span := recordQuerySpan(t, t.Context(), nil, tc.data, tc.options...)
			parameters := make(map[string]any)
			for key, value := range spanAttributeMap(span) {
				if strings.HasPrefix(key, "db.query.parameter.") {
					parameters[key] = value
				}
			}
			if len(tc.want) == 0 {
				assert.Empty(t, parameters)
			} else {
				assert.Equal(t, tc.want, parameters)
			}
		})
	}
}

func TestTracer_Attributes(t *testing.T) {
	t.Parallel()
	attrs := []attribute.KeyValue{attribute.String("cluster", "production")}
	option := WithTracerAttributes(attrs...)
	attrs[0] = attribute.String("cluster", "mutated")
	for _, name := range []string{"first tracer", "second tracer"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			span := recordQuerySpan(t, t.Context(), nil, pgx.TraceQueryStartData{}, option,
				WithTracerAttributes(attribute.String("component", "database"), attribute.String("db.system.name", "overridden")),
			)
			got := spanAttributeMap(span)
			assert.Equal(t, "production", got["cluster"], "option attributes are copied and reusable")
			assert.Equal(t, "database", got["component"])
			assert.Equal(t, "cockroachdb", got["db.system.name"], "instrumented attributes take precedence")
		})
	}
}
