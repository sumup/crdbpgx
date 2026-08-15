package crdbotel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Option configures a Tracer.
type Option interface {
	apply(*tracerConfig)
}

type optionFunc func(*tracerConfig)

func (f optionFunc) apply(cfg *tracerConfig) { f(cfg) }

type sqlStatementMode uint8

const (
	sqlStatementParameterized sqlStatementMode = iota
	sqlStatementDisabled
	sqlStatementAll
)

// WithTracerProvider specifies the provider used for CockroachDB and pgx spans.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return optionFunc(func(cfg *tracerConfig) {
		if provider != nil {
			cfg.tracerProvider = provider
		}
	})
}

// WithMeterProvider specifies the provider used for pgx operation metrics.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return optionFunc(func(cfg *tracerConfig) {
		if provider != nil {
			cfg.meterProvider = provider
		}
	})
}

// WithTracerAttributes adds attributes to CockroachDB and pgx spans.
func WithTracerAttributes(attrs ...attribute.KeyValue) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.tracerAttributes = append(cfg.tracerAttributes, attrs...)
	})
}

// WithAttributes adds attributes to CockroachDB and pgx spans.
// Deprecated: use WithTracerAttributes.
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return WithTracerAttributes(attrs...)
}

// WithMeterAttributes adds attributes to pgx operation and connection metrics.
func WithMeterAttributes(attrs ...attribute.KeyValue) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.meterAttributes = append(cfg.meterAttributes, attrs...)
	})
}

// WithTrimSQLInSpanName is retained for compatibility. SQL span names are
// always low-cardinality query summaries.
// Deprecated: SQL span names always follow OpenTelemetry naming conventions.
func WithTrimSQLInSpanName() Option {
	return optionFunc(func(*tracerConfig) {})
}

// WithDisableAcquireTracer disables pgx connection-acquire spans.
func WithDisableAcquireTracer() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.disableAcquireTracer = true
	})
}

// SpanNameFunc generates a low-cardinality query summary from a SQL statement.
// The summary is used for db.query.summary and the span name.
type SpanNameFunc func(stmt string) string

// SpanNameCtxFunc generates a low-cardinality query summary from a context and
// SQL statement. The summary is used for db.query.summary and the span name.
type SpanNameCtxFunc func(ctx context.Context, stmt string) string

// WithSpanNameFunc configures the SQL query summary function. The returned
// value must not contain identifiers, literals, or other high-cardinality data.
func WithSpanNameFunc(fn SpanNameFunc) Option {
	return WithSpanNameCtxFunc(func(_ context.Context, stmt string) string {
		return fn(stmt)
	})
}

// WithSpanNameCtxFunc configures the context-aware SQL query summary function.
// The returned value must not contain identifiers, literals, or other
// high-cardinality data.
func WithSpanNameCtxFunc(fn SpanNameCtxFunc) Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.spanNameCtxFunc = fn
	})
}

// WithDisableQuerySpanNamePrefix is retained for compatibility. SQL span names
// are never prefixed with implementation-specific operation labels.
// Deprecated: SQL span names always follow OpenTelemetry naming conventions.
func WithDisableQuerySpanNamePrefix() Option {
	return optionFunc(func(*tracerConfig) {})
}

// WithDisableConnectionDetailsInAttributes omits connection details from pgx
// spans and operation metrics.
func WithDisableConnectionDetailsInAttributes() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.includeConnectionDetails = false
	})
}

// WithDisableSQLStatementInAttributes omits SQL statements from pgx spans.
func WithDisableSQLStatementInAttributes() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.sqlStatementMode = sqlStatementDisabled
	})
}

// WithIncludeSQLStatementInAttributes includes all SQL statements in
// db.query.text, including non-parameterized statements. Literal values may
// contain sensitive data; enable this only after evaluating that risk.
func WithIncludeSQLStatementInAttributes() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.sqlStatementMode = sqlStatementAll
	})
}

// WithIncludeQueryParameters includes SQL parameters in pgx span attributes.
// Parameters may contain sensitive data; enable this only after evaluating that risk.
func WithIncludeQueryParameters() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.includeQueryParameters = true
	})
}

// WithRowCountsInAttributes includes result row counts in pgx span attributes.
// SELECT operations use the opt-in db.response.returned_rows convention;
// operations that modify rows use a crdbpgx attribute.
func WithRowCountsInAttributes() Option {
	return optionFunc(func(cfg *tracerConfig) {
		cfg.includeRowCounts = true
	})
}

// StatsOption configures RecordStats.
type StatsOption interface {
	applyStats(*statsConfig)
}

type statsOptionFunc func(*statsConfig)

func (f statsOptionFunc) applyStats(cfg *statsConfig) { f(cfg) }

// WithStatsMeterProvider specifies the provider used for pool statistics.
func WithStatsMeterProvider(provider metric.MeterProvider) StatsOption {
	return statsOptionFunc(func(cfg *statsConfig) {
		if provider != nil {
			cfg.meterProvider = provider
		}
	})
}

// WithStatsAttributes adds attributes to every pool statistic metric.
func WithStatsAttributes(attrs ...attribute.KeyValue) StatsOption {
	return statsOptionFunc(func(cfg *statsConfig) {
		cfg.attributes = append(cfg.attributes, attrs...)
	})
}

// WithMinimumReadDBStatsInterval sets the minimum interval between pgx pool
// statistic snapshots. Negative values are ignored.
func WithMinimumReadDBStatsInterval(interval time.Duration) StatsOption {
	return statsOptionFunc(func(cfg *statsConfig) {
		if interval >= 0 {
			cfg.minimumReadDBStatsInterval = interval
		}
	})
}
