package crdbotel

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestSQLSpanAttributes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		cfg       *pgx.ConnConfig
		spanName  string
		address   string
		port      int
		namespace string
	}{
		{
			name:     "unknown connection",
			spanName: "cockroachdb",
		},
		{
			name: "database target",
			cfg: &pgx.ConnConfig{
				Config: pgconn.Config{
					Host:     "db.example.com",
					Port:     26257,
					Database: "orders",
				},
			},
			spanName:  "orders",
			address:   "db.example.com",
			namespace: "orders",
		},
		{
			name: "server target",
			cfg: &pgx.ConnConfig{
				Config: pgconn.Config{
					Host: "db.example.com",
					Port: 26257,
				},
			},
			spanName: "db.example.com",
			address:  "db.example.com",
		},
		{
			name: "nondefault IPv6 port",
			cfg: &pgx.ConnConfig{
				Config: pgconn.Config{
					Host: "2001:db8::1",
					Port: 5432,
				},
			},
			spanName: "[2001:db8::1]:5432",
			address:  "2001:db8::1",
			port:     5432,
		},
		{
			name: "Unix socket",
			cfg: &pgx.ConnConfig{
				Config: pgconn.Config{
					Host: "/tmp",
					Port: 26257,
				},
			},
			spanName: filepath.Join("/tmp", ".s.PGSQL.26257"),
			address:  filepath.Join("/tmp", ".s.PGSQL.26257"),
		},
		{
			name: "TLS fallback deduplication",
			cfg: &pgx.ConnConfig{Config: pgconn.Config{
				Host: "db-a",
				Port: 26257,
				Fallbacks: []*pgconn.FallbackConfig{
					{Host: "db-a", Port: 26257},
					{Host: "db-b", Port: 26257},
				},
			}},
			spanName: "db-a,db-b",
			address:  "db-a,db-b",
		},
		{
			name: "multiple ports",
			cfg: &pgx.ConnConfig{
				Config: pgconn.Config{
					Host: "db-a",
					Port: 26257,
					Fallbacks: []*pgconn.FallbackConfig{
						{Host: "2001:db8::1", Port: 5432},
					},
				},
			},
			spanName: "db-a:26257,[2001:db8::1]:5432",
			address:  "db-a:26257,[2001:db8::1]:5432",
		},
		{
			name: "endpoint limit",
			cfg: &pgx.ConnConfig{
				Config: pgconn.Config{
					Host: "db-a",
					Port: 26257,
					Fallbacks: []*pgconn.FallbackConfig{
						{Host: "db-b", Port: 26257},
						{Host: "db-c", Port: 26257},
						{Host: "db-d", Port: 26257},
						{Host: "db-e", Port: 26257},
						{Host: "db-f", Port: 26257},
					},
				},
			},
			spanName: "db-a,db-b,db-c,db-d,db-e",
			address:  "db-a,db-b,db-c,db-d,db-e",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			name, attrs := querySpanAttributes(tc.cfg)
			assert.Equal(t, tc.spanName, name)
			want := []attribute.KeyValue{attribute.String("db.system.name", "cockroachdb")}
			if tc.address != "" {
				want = append(want, attribute.String("server.address", tc.address))
			}
			if tc.port != 0 {
				want = append(want, attribute.Int("server.port", tc.port))
			}
			if tc.namespace != "" {
				want = append(want, attribute.String("db.namespace", tc.namespace))
			}
			assert.ElementsMatch(t, want, attrs)
		})
	}
}

type testQueryRewriter struct{}

func (testQueryRewriter) RewriteQuery(context.Context, *pgx.Conn, string, []any) (string, []any, error) {
	panic("instrumentation must not invoke query rewriters")
}

func TestTracer_QueryText(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data pgx.TraceQueryStartData
		text string
	}{
		{
			name: "literal SQL",
			data: pgx.TraceQueryStartData{SQL: "SELECT 'private'"},
		},
		{
			name: "positional parameters",
			data: pgx.TraceQueryStartData{SQL: "SELECT $1", Args: []any{"private"}},
			text: "SELECT $1",
		},
		{
			name: "query options only",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT 'private'",
				Args: []any{pgx.QueryExecModeSimpleProtocol, pgx.QueryResultFormats{0}, pgx.QueryResultFormatsByOID{23: 0}},
			},
		},
		{
			name: "options and parameters",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT $1",
				Args: []any{pgx.QueryResultFormats{0}, pgx.QueryExecModeExec, "private"},
			},
			text: "SELECT $1",
		},
		{
			name: "named parameters",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT @value",
				Args: []any{pgx.NamedArgs{"value": "private"}},
			},
			text: "SELECT @value",
		},
		{
			name: "strict named parameters",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT @value",
				Args: []any{pgx.StrictNamedArgs{"value": "private"}},
			},
			text: "SELECT @value",
		},
		{
			name: "empty named parameters",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT 'private'",
				Args: []any{pgx.NamedArgs{}},
			},
		},
		{
			name: "opaque rewriter",
			data: pgx.TraceQueryStartData{
				SQL:  "SELECT 'private'",
				Args: []any{testQueryRewriter{}, "private"},
			},
		},
		{
			name: "bounded UTF-8 text",
			data: pgx.TraceQueryStartData{
				SQL:  strings.Repeat("€", 6000),
				Args: []any{"private"},
			},
			text: strings.Repeat("€", 5461),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			span := recordQuerySpan(t, t.Context(), nil, tc.data)
			attrs := attribute.NewSet(span.Attributes()...)
			text, ok := attrs.Value("db.query.text")
			assert.Equal(t, tc.text != "", ok)
			if ok {
				assert.Equal(t, tc.text, text.AsString())
			}
			for _, attr := range attrs.ToSlice() {
				assert.NotContains(t, string(attr.Key), "db.query.parameter.")
				assert.NotContains(t, attr.Value.String(), "private")
			}
		})
	}
}
