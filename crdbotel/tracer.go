package crdbotel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/sumup/crdbpgx"
)

type tracerConfig struct {
	tracerProvider           trace.TracerProvider
	attributes               []attribute.KeyValue
	spanName                 SpanNameFunc
	disableConnectionDetails bool
	disableSQLStatement      bool
	includeQueryParameters   bool
	queryParametersFilter    QueryParametersFilterFunc
}

// TracerOption configures OpenTelemetry tracing.
type TracerOption func(*tracerConfig)

// WithTracerProvider configures the provider used for query spans. A nil provider
// uses the global OpenTelemetry tracer provider.
func WithTracerProvider(provider trace.TracerProvider) TracerOption {
	return func(cfg *tracerConfig) {
		cfg.tracerProvider = provider
	}
}

// WithTracerAttributes adds attributes to query spans. Repeated calls append
// attributes; later values win for duplicate keys. Automatically collected
// attributes take precedence over additional attributes with the same key.
func WithTracerAttributes(attrs ...attribute.KeyValue) TracerOption {
	attrs = slices.Clone(attrs)
	return func(cfg *tracerConfig) {
		cfg.attributes = append(cfg.attributes, attrs...)
	}
}

// SpanNameFunc returns a low-cardinality summary using the query context and
// SQL text. Callbacks must be safe for concurrent use and return promptly.
// An empty string uses the default span name.
type SpanNameFunc func(context.Context, string) string

// WithSpanNameFunc sets the query span name and db.query.summary using fn.
// Summaries are limited to 255 bytes. A nil function restores default naming.
// Naming is independent of SQL text and connection-detail collection.
func WithSpanNameFunc(fn SpanNameFunc) TracerOption {
	return func(cfg *tracerConfig) {
		cfg.spanName = fn
	}
}

// WithDisableConnectionDetailsInAttributes disables automatic collection of the
// database namespace, configured endpoints, and network peer. Default span names
// then fall back to the DBMS name.
func WithDisableConnectionDetailsInAttributes() TracerOption {
	return func(cfg *tracerConfig) {
		cfg.disableConnectionDetails = true
	}
}

// WithDisableSQLStatementInAttributes disables automatic query text and
// parameter collection, including when [WithIncludeQueryParameters] is set.
func WithDisableSQLStatementInAttributes() TracerOption {
	return func(cfg *tracerConfig) {
		cfg.disableSQLStatement = true
	}
}

// QueryParametersFilterFunc decides whether to collect parameter values for a
// SQL query. It must be safe for concurrent use and return promptly.
type QueryParametersFilterFunc func(sql string) bool

// WithIncludeQueryParameters enables db.query.parameter.<key> attributes for
// queries whose text is collected. Values may contain sensitive data. An optional
// filter restricts collection to selected queries; only the first filter is used.
// A missing or nil filter enables collection for all supported query templates.
// Positional keys start at zero; named parameters use their argument names.
// Each parameter value is formatted as text and limited to 16 KiB.
func WithIncludeQueryParameters(filter ...QueryParametersFilterFunc) TracerOption {
	return func(cfg *tracerConfig) {
		cfg.includeQueryParameters = true
		cfg.queryParametersFilter = nil
		if len(filter) > 0 {
			cfg.queryParametersFilter = filter[0]
		}
	}
}

// Tracer creates query spans and adds failover events to the caller's span.
// It creates no metrics and is safe to share across pools. Its zero value uses
// the global OpenTelemetry tracer provider.
type Tracer struct {
	// HooksDefaults supplies no-op callbacks for events this tracer does not record.
	crdbpgx.HooksDefaults

	tracer trace.Tracer
	config tracerConfig
}

type querySpanKey struct{ tracer *Tracer }

var _ crdbpgx.Hook = (*Tracer)(nil)

// NewTracer creates query and failover tracing for [crdbpgx.WithHooks]. Each
// Exec, Query, and QueryRow attempt gets a client span. Query spans end when pgx
// completes the operation or closes its rows. Parameterized query text is
// recorded up to 16 KiB; parameter values are opt-in and non-parameterized text
// is not collected. The caller owns provider flushing and shutdown.
func NewTracer(opts ...TracerOption) *Tracer {
	cfg := tracerConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.tracerProvider == nil {
		cfg.tracerProvider = otel.GetTracerProvider()
	}
	return &Tracer{
		tracer: cfg.tracerProvider.Tracer("github.com/sumup/crdbpgx", trace.WithSchemaURL(semconv.SchemaURL)),
		config: cfg,
	}
}

// TraceQueryStart starts a span for an individual pgx query attempt.
func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer := t.tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/sumup/crdbpgx", trace.WithSchemaURL(semconv.SchemaURL))
	}

	var cfg *pgx.ConnConfig
	if conn != nil && !t.config.disableConnectionDetails {
		cfg = conn.Config()
	}

	name, attrs := querySpanAttributes(cfg)
	attrs = append(slices.Clone(t.config.attributes), attrs...)
	if !t.config.disableSQLStatement {
		if text := parameterizedQueryText(data); text != "" {
			attrs = append(attrs, semconv.DBQueryText(text))
			if t.config.includeQueryParameters && (t.config.queryParametersFilter == nil || t.config.queryParametersFilter(data.SQL)) {
				attrs = append(attrs,
					queryParameterAttributes(ctx, data)...,
				)
			}
		}
	}
	if conn != nil && !t.config.disableConnectionDetails {
		switch addr := conn.PgConn().Conn().RemoteAddr().(type) {
		case *net.TCPAddr:
			attrs = append(attrs,
				semconv.NetworkPeerAddress(addr.IP.String()),
				semconv.NetworkPeerPort(addr.Port),
			)
		case *net.UnixAddr:
			attrs = append(attrs,
				semconv.NetworkPeerAddress(addr.Name),
			)
		}
	}
	if t.config.spanName != nil {
		if summary := truncateUTF8(t.config.spanName(ctx, data.SQL), 255); summary != "" {
			name = summary
			attrs = append(attrs, semconv.DBQuerySummary(summary))
		}
	}
	ctx, span := tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	return context.WithValue(ctx, querySpanKey{t}, span)
}

// TraceQueryEnd completes this tracer's query span. PostgreSQL failures set
// error.type and db.response.status_code to the SQLSTATE; other failures use
// their unwrapped Go error type. An empty result is not treated as a failure.
func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, ok := ctx.Value(querySpanKey{t}).(trace.Span)
	if !ok {
		return
	}
	defer span.End()
	if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) {
		cause := data.Err
		for unwrapped := errors.Unwrap(cause); unwrapped != nil; unwrapped = errors.Unwrap(cause) {
			cause = unwrapped
		}
		errorType := fmt.Sprintf("%T", cause)
		var pgErr *pgconn.PgError
		if errors.As(data.Err, &pgErr) && pgErr.Code != "" {
			errorType = pgErr.Code
			span.SetAttributes(semconv.DBResponseStatusCode(pgErr.Code))
		}
		span.SetAttributes(semconv.ErrorTypeKey.String(errorType))
		span.SetStatus(codes.Error, "")
	}
}

// querySpanAttributes uses the SQL fallback span name: namespace, server
// endpoint, or DBMS. Generic query calls do not supply an operation or table;
// those attributes must not be guessed from SQL text.
func querySpanAttributes(cfg *pgx.ConnConfig) (string, []attribute.KeyValue) {
	attrs := []attribute.KeyValue{semconv.DBSystemNameCockroachDB}
	name := semconv.DBSystemNameCockroachDB.Value.AsString()
	if cfg == nil {
		return name, attrs
	}
	type endpoint struct {
		host string
		port uint16
	}
	var endpoints []endpoint
	seen := make(map[endpoint]struct{})
	addEndpoint := func(host string, port uint16) {
		ep := endpoint{host, port}
		if _, ok := seen[ep]; ok || host == "" || len(endpoints) == 5 {
			return
		}
		seen[ep] = struct{}{}
		endpoints = append(endpoints, ep)
	}
	addEndpoint(cfg.Host, cfg.Port)
	for _, fallback := range cfg.Fallbacks {
		addEndpoint(fallback.Host, fallback.Port)
	}
	if len(endpoints) == 1 {
		ep := endpoints[0]
		network, address := pgconn.NetworkAddress(ep.host, ep.port)
		if network == "unix" {
			attrs = append(attrs, semconv.ServerAddress(address))
			name = address
		} else {
			attrs = append(attrs, semconv.ServerAddress(ep.host))
			name = ep.host
			// CockroachDB's default SQL port is 26257.
			if ep.port != 0 && ep.port != 26257 {
				attrs = append(attrs, semconv.ServerPort(int(ep.port)))
				name = net.JoinHostPort(ep.host, strconv.Itoa(int(ep.port)))
			}
		}
	} else if len(endpoints) > 1 {
		includePorts := false
		for _, ep := range endpoints {
			if ep.port != 0 && ep.port != 26257 {
				includePorts = true
			}
		}
		addresses := make([]string, 0, len(endpoints))
		for _, ep := range endpoints {
			network, address := pgconn.NetworkAddress(ep.host, ep.port)
			switch {
			case network == "unix":
				addresses = append(addresses, address)
			case includePorts:
				addresses = append(addresses, net.JoinHostPort(ep.host, strconv.Itoa(int(ep.port))))
			default:
				addresses = append(addresses, ep.host)
			}
		}
		name = strings.Join(addresses, ",")
		attrs = append(attrs, semconv.ServerAddress(name))
	}
	if cfg.Database != "" {
		attrs = append(attrs, semconv.DBNamespace(cfg.Database))
		name = cfg.Database
	}
	return name, attrs
}

// Query tracing runs before pgx consumes query options or invokes a rewriter.
// Capture parameterized templates, excluding values and option-only calls.
// Opaque rewriters are not invoked by instrumentation.
func parameterizedQueryText(data pgx.TraceQueryStartData) string {
	positional, named := queryArguments(data.Args)
	if len(positional) == 0 && len(named) == 0 {
		return ""
	}
	return truncateUTF8(data.SQL, 16*1024)
}

func queryArguments(args []any) ([]any, map[string]any) {
	var named map[string]any
	rewritten := false
	for len(args) > 0 {
		switch arg := args[0].(type) {
		case pgx.QueryExecMode, pgx.QueryResultFormats, pgx.QueryResultFormatsByOID:
		case pgx.NamedArgs:
			rewritten = true
			named = arg
		case pgx.StrictNamedArgs:
			rewritten = true
			named = arg
		case pgx.QueryRewriter:
			return nil, nil
		default:
			if !rewritten {
				return args, nil
			}
			return nil, named
		}
		args = args[1:]
	}
	return nil, named
}

func queryParameterAttributes(ctx context.Context, data pgx.TraceQueryStartData) []attribute.KeyValue {
	positional, named := queryArguments(data.Args)
	attrs := make([]attribute.KeyValue, 0, len(positional)+len(named))
	for i, value := range positional {
		attrs = append(attrs, semconv.DBQueryParameter(strconv.Itoa(i), truncateUTF8(fmt.Sprint(value), 16*1024)))
	}
	if len(named) > 0 {
		// Reuse pgx's built-in named-argument lexer to select names referenced by
		// the template, without recording unused entries or invoking user code.
		type parameter struct {
			name  string
			value any
		}
		probes := make(pgx.NamedArgs, len(named))
		for name, value := range named {
			probes[name] = parameter{name, value}
		}
		_, bound, err := probes.RewriteQuery(ctx, nil, data.SQL, nil)
		if err != nil {
			return attrs // An unresolvable template has no reliable named values.
		}
		for _, value := range bound {
			if p, ok := value.(parameter); ok {
				attrs = append(attrs, semconv.DBQueryParameter(p.name, truncateUTF8(fmt.Sprint(p.value), 16*1024)))
			}
		}
	}
	return attrs
}

func truncateUTF8(text string, maxBytes int) string {
	if len(text) > maxBytes {
		end := maxBytes
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		text = text[:end]
	}
	return text
}

// OnNodeSwitchStart records the start of replacement connection acquisition.
func (*Tracer) OnNodeSwitchStart(ctx context.Context, data crdbpgx.NodeSwitchStartData) {
	trace.SpanFromContext(ctx).AddEvent("acquiring connection from different node",
		trace.WithAttributes(attribute.Int64("current_node_id", int64(data.FromNodeID))),
	)
}

// OnNodeSwitchEnd records successful failover or the single-node fallback.
func (*Tracer) OnNodeSwitchEnd(ctx context.Context, data crdbpgx.NodeSwitchEndData) {
	if data.Err != nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	if data.Reused {
		span.AddEvent("only one healthy node available, reuse connection",
			trace.WithAttributes(attribute.Int64("current_node_id", int64(data.FromNodeID))),
		)
		return
	}
	span.AddEvent("acquired connection from different node",
		trace.WithAttributes(attribute.Int64("node_id", int64(data.ToNodeID))),
	)
}
