package crdbotel

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	dbconv "go.opentelemetry.io/otel/semconv/v1.40.0/dbconv"
	"go.opentelemetry.io/otel/trace"
)

const (
	pgxOperationCopy    = "COPY"
	pgxOperationBatch   = "BATCH"
	pgxOperationConnect = "CONNECT"
	pgxOperationPrepare = "PREPARE"
)

var (
	// RowsAffectedKey identifies rows affected by an operation. OpenTelemetry
	// defines returned rows, but not affected rows, so this remains namespaced.
	RowsAffectedKey = attribute.Key("crdbpgx.db.response.affected_rows")
	// PrepareStmtNameKey identifies a pgx prepared statement name.
	PrepareStmtNameKey = attribute.Key("crdbpgx.db.prepared_statement.name")
	// SQLStateKey is the canonical database response status code key.
	SQLStateKey = semconv.DBResponseStatusCodeKey
	// PGXOperationTypeKey is retained as an alias for the canonical operation key.
	// Deprecated: use semconv.DBOperationNameKey.
	PGXOperationTypeKey = semconv.DBOperationNameKey
)

type operationStateKey struct{}

type operationState struct {
	started     time.Time
	operation   string
	metricAttrs []attribute.KeyValue
	poolName    string
}

func (t *Tracer) configureMetrics(provider metric.MeterProvider, attrs []attribute.KeyValue) {
	meter := provider.Meter(
		instrumentationName,
		metric.WithInstrumentationVersion(instrumentationVersion()),
	)

	var err error
	t.operationDuration, err = dbconv.NewClientOperationDuration(
		meter,
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		otel.Handle(fmt.Errorf("create database client operation duration metric: %w", err))
	}
	t.connectionCreateTime, err = dbconv.NewClientConnectionCreateTime(meter)
	if err != nil {
		otel.Handle(fmt.Errorf("create database client connection create time metric: %w", err))
	}
	t.connectionWaitTime, err = dbconv.NewClientConnectionWaitTime(meter)
	if err != nil {
		otel.Handle(fmt.Errorf("create database client connection wait time metric: %w", err))
	}
	t.meterAttributes = append([]attribute.KeyValue(nil), attrs...)
}

func startOperation(ctx context.Context, state operationState) context.Context {
	state.started = time.Now()
	return context.WithValue(ctx, operationStateKey{}, state)
}

func operationFromContext(ctx context.Context) (operationState, bool) {
	state, ok := ctx.Value(operationStateKey{}).(operationState)
	return state, ok
}

func (t *Tracer) recordDatabaseOperation(ctx context.Context, err error) {
	state, ok := operationFromContext(ctx)
	if !ok {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(t.meterAttributes)+len(state.metricAttrs)+3)
	attrs = append(attrs, t.meterAttributes...)
	attrs = append(attrs, state.metricAttrs...)
	attrs = append(attrs, errorAttributes(err)...)
	t.operationDuration.Record(
		ctx,
		time.Since(state.started).Seconds(),
		dbconv.SystemNameCockroachDB,
		attrs...,
	)
}

func (t *Tracer) recordConnectionCreateTime(ctx context.Context) {
	state, ok := operationFromContext(ctx)
	if !ok || state.poolName == "" {
		return
	}
	t.connectionCreateTime.Record(
		ctx,
		time.Since(state.started).Seconds(),
		state.poolName,
		t.meterAttributes...,
	)
}

func (t *Tracer) recordConnectionWaitTime(ctx context.Context) {
	state, ok := operationFromContext(ctx)
	if !ok || state.poolName == "" {
		return
	}
	t.connectionWaitTime.Record(
		ctx,
		time.Since(state.started).Seconds(),
		state.poolName,
		t.meterAttributes...,
	)
}

func (t *Tracer) startClientSpan(
	ctx context.Context,
	name string,
	attrs []attribute.KeyValue,
) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	ctx, _ = t.tracer.Start(
		ctx,
		name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
	return ctx
}

func (t *Tracer) pgxSpanAttributes(conn *pgx.Conn) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(t.attributes)+3)
	attrs = append(attrs, t.attributes...)
	if t.includeConnectionDetails && conn != nil {
		attrs = append(attrs, connectionAttributes(conn.Config())...)
	}
	return attrs
}

func connectionAttributes(config *pgx.ConnConfig) []attribute.KeyValue {
	if config == nil {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, 3)
	if config.Host != "" {
		attrs = append(attrs, semconv.ServerAddress(config.Host))
		if config.Port != 0 {
			attrs = append(attrs, semconv.ServerPort(int(config.Port)))
		}
	}
	if config.Database != "" {
		attrs = append(attrs, semconv.DBNamespace(config.Database))
	}
	return attrs
}

func connectionPoolName(config *pgx.ConnConfig) string {
	if config == nil {
		return ""
	}
	name := config.Host
	if config.Host != "" && config.Port != 0 {
		name = net.JoinHostPort(config.Host, strconv.Itoa(int(config.Port)))
	}
	if config.Database != "" {
		if name == "" {
			return config.Database
		}
		name += "/" + config.Database
	}
	return name
}

func defaultSpanNameCtxFunc(_ context.Context, statement string) string {
	for word := range strings.FieldsSeq(statement) {
		return word
	}
	return ""
}

func (t *Tracer) querySummary(ctx context.Context, statement string) string {
	summary := strings.TrimSpace(t.spanNameCtxFunc(ctx, statement))
	if len(summary) > 255 {
		summary = summary[:255]
	}
	return summary
}

func sqlOperationName(statement string) string {
	return defaultSpanNameCtxFunc(context.Background(), statement)
}

func spanName(summary, operation string) string {
	if summary != "" {
		return summary
	}
	if operation != "" {
		return operation
	}
	return "cockroachdb"
}

func (t *Tracer) includeQueryText(args []any) bool {
	return t.sqlStatementMode == sqlStatementAll ||
		(t.sqlStatementMode == sqlStatementParameterized && len(args) > 0)
}

func (t *Tracer) queryAttributes(
	ctx context.Context,
	conn *pgx.Conn,
	statement string,
	args []any,
) ([]attribute.KeyValue, string, string) {
	return t.queryAttributesWithParameters(ctx, conn, statement, args, true)
}

func (t *Tracer) queryAttributesWithParameters(
	ctx context.Context,
	conn *pgx.Conn,
	statement string,
	args []any,
	includeParameters bool,
) ([]attribute.KeyValue, string, string) {
	attrs := t.pgxSpanAttributes(conn)
	operation := sqlOperationName(statement)
	if operation != "" {
		attrs = append(attrs, semconv.DBOperationName(operation))
	}
	summary := t.querySummary(ctx, statement)
	if summary != "" {
		attrs = append(attrs, semconv.DBQuerySummary(summary))
	}
	if !t.includeQueryText(args) {
		return attrs, operation, summary
	}
	attrs = append(attrs, semconv.DBQueryText(statement))
	if includeParameters && t.includeQueryParameters {
		attrs = append(attrs, queryParameterAttributes(args)...)
	}
	return attrs, operation, summary
}

func (t *Tracer) metricOperationAttributes(
	conn *pgx.Conn,
	operation string,
	summary string,
) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5)
	if t.includeConnectionDetails && conn != nil {
		attrs = append(attrs, connectionAttributes(conn.Config())...)
	}
	if operation != "" {
		attrs = append(attrs, semconv.DBOperationName(operation))
	}
	if summary != "" {
		attrs = append(attrs, semconv.DBQuerySummary(summary))
	}
	return attrs
}

func (t *Tracer) batchAttributes(
	ctx context.Context,
	conn *pgx.Conn,
	batch *pgx.Batch,
) ([]attribute.KeyValue, string, string) {
	if batch != nil && batch.Len() == 1 {
		query := batch.QueuedQueries[0]
		return t.queryAttributes(ctx, conn, query.SQL, query.Arguments)
	}

	operation := pgxOperationBatch
	summary := pgxOperationBatch
	attrs := t.pgxSpanAttributes(conn)
	size := 0
	if batch != nil {
		size = batch.Len()
	}
	attrs = append(attrs, semconv.DBOperationBatchSize(size))

	if size > 1 {
		firstOperation := sqlOperationName(batch.QueuedQueries[0].SQL)
		firstSummary := t.querySummary(ctx, batch.QueuedQueries[0].SQL)
		sameOperation := firstOperation != ""
		sameSummary := firstSummary != ""
		includeQueryText := true
		queryTexts := make([]string, 0, size)
		for _, query := range batch.QueuedQueries {
			sameOperation = sameOperation && sqlOperationName(query.SQL) == firstOperation
			sameSummary = sameSummary && t.querySummary(ctx, query.SQL) == firstSummary
			includeQueryText = includeQueryText && t.includeQueryText(query.Arguments)
			queryTexts = append(queryTexts, query.SQL)
		}
		if sameOperation {
			operation += " " + firstOperation
		}
		if sameSummary {
			summary += " " + firstSummary
		}
		if includeQueryText {
			attrs = append(attrs, semconv.DBQueryText(strings.Join(queryTexts, "; ")))
		}
	}

	attrs = append(
		attrs,
		semconv.DBOperationName(operation),
		semconv.DBQuerySummary(summary),
	)
	return attrs, operation, summary
}

func (t *Tracer) resultAttributes(operation string, rows int64) []attribute.KeyValue {
	if !t.includeRowCounts {
		return nil
	}
	if strings.EqualFold(operation, "SELECT") {
		return []attribute.KeyValue{semconv.DBResponseReturnedRows(int(rows))}
	}
	return []attribute.KeyValue{RowsAffectedKey.Int64(rows)}
}

// TraceQueryStart starts tracing and timing a pgx Query, QueryRow, or Exec call.
func (t *Tracer) TraceQueryStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	attrs, operation, summary := t.queryAttributes(ctx, conn, data.SQL, data.Args)
	ctx = startOperation(ctx, operationState{
		operation:   operation,
		metricAttrs: t.metricOperationAttributes(conn, operation, summary),
	})
	return t.startClientSpan(ctx, spanName(summary, operation), attrs)
}

// TraceQueryEnd completes tracing and timing a pgx Query, QueryRow, or Exec call.
func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	t.recordDatabaseOperation(ctx, data.Err)
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	if data.Err == nil {
		state, _ := operationFromContext(ctx)
		span.SetAttributes(t.resultAttributes(state.operation, data.CommandTag.RowsAffected())...)
	}
	recordError(span, data.Err)
	span.End()
}

// TraceCopyFromStart starts tracing and timing a pgx CopyFrom call.
func (t *Tracer) TraceCopyFromStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceCopyFromStartData,
) context.Context {
	collection := strings.Join(data.TableName, ".")
	attrs := append(
		t.pgxSpanAttributes(conn),
		semconv.DBOperationName(pgxOperationCopy),
		semconv.DBCollectionName(collection),
	)
	ctx = startOperation(ctx, operationState{
		operation: pgxOperationCopy,
		metricAttrs: append(
			t.metricOperationAttributes(conn, pgxOperationCopy, ""),
			semconv.DBCollectionName(collection),
		),
	})
	return t.startClientSpan(ctx, pgxOperationCopy+" "+collection, attrs)
}

// TraceCopyFromEnd completes tracing and timing a pgx CopyFrom call.
func (t *Tracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	t.recordDatabaseOperation(ctx, data.Err)
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	if data.Err == nil {
		span.SetAttributes(t.resultAttributes(pgxOperationCopy, data.CommandTag.RowsAffected())...)
	}
	recordError(span, data.Err)
	span.End()
}

// TraceBatchStart starts tracing and timing a pgx batch.
func (t *Tracer) TraceBatchStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceBatchStartData,
) context.Context {
	attrs, operation, summary := t.batchAttributes(ctx, conn, data.Batch)
	ctx = startOperation(ctx, operationState{
		operation:   operation,
		metricAttrs: t.metricOperationAttributes(conn, operation, summary),
	})
	return t.startClientSpan(ctx, spanName(summary, operation), attrs)
}

// TraceBatchQuery records one completed query as an event on the batch span.
func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs, operation, _ := t.queryAttributesWithParameters(
		ctx,
		conn,
		data.SQL,
		data.Args,
		false,
	)
	if data.Err == nil {
		attrs = append(attrs, t.resultAttributes(operation, data.CommandTag.RowsAffected())...)
	} else {
		attrs = append(attrs, errorAttributes(data.Err)...)
	}
	span.AddEvent("crdbpgx.batch.query", trace.WithAttributes(attrs...))
}

// TraceBatchEnd completes tracing and timing a pgx batch.
func (t *Tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	t.recordDatabaseOperation(ctx, data.Err)
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	recordError(span, data.Err)
	span.End()
}

// TraceConnectStart starts tracing and timing a pgx connection attempt.
func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	attrs := make([]attribute.KeyValue, 0, len(t.attributes)+4)
	attrs = append(attrs, t.attributes...)
	attrs = append(attrs, semconv.DBOperationName(pgxOperationConnect))
	if t.includeConnectionDetails {
		attrs = append(attrs, connectionAttributes(data.ConnConfig)...)
	}
	ctx = startOperation(ctx, operationState{poolName: connectionPoolName(data.ConnConfig)})
	return t.startClientSpan(ctx, pgxOperationConnect, attrs)
}

// TraceConnectEnd completes tracing and timing a pgx connection attempt.
func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	t.recordConnectionCreateTime(ctx)
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	recordError(span, data.Err)
	span.End()
}

// TracePrepareStart starts tracing and timing a pgx Prepare call.
func (t *Tracer) TracePrepareStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TracePrepareStartData,
) context.Context {
	attrs := t.pgxSpanAttributes(conn)
	attrs = append(attrs, semconv.DBOperationName(pgxOperationPrepare))
	if data.Name != "" {
		attrs = append(attrs, PrepareStmtNameKey.String(data.Name))
	}
	if t.sqlStatementMode == sqlStatementAll {
		attrs = append(attrs, semconv.DBQueryText(data.SQL))
	}
	summary := t.querySummary(ctx, data.SQL)
	if summary != "" {
		summary = pgxOperationPrepare + " " + summary
		attrs = append(attrs, semconv.DBQuerySummary(summary))
	}
	ctx = startOperation(ctx, operationState{
		operation:   pgxOperationPrepare,
		metricAttrs: t.metricOperationAttributes(conn, pgxOperationPrepare, summary),
	})
	return t.startClientSpan(ctx, spanName(summary, pgxOperationPrepare), attrs)
}

// TracePrepareEnd completes tracing and timing a pgx Prepare call.
func (t *Tracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareEndData) {
	t.recordDatabaseOperation(ctx, data.Err)
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	recordError(span, data.Err)
	span.End()
}

// TraceAcquireStart starts tracing and timing a pgxpool Acquire call.
func (t *Tracer) TraceAcquireStart(
	ctx context.Context,
	pool *pgxpool.Pool,
	_ pgxpool.TraceAcquireStartData,
) context.Context {
	if t.disableAcquireTracer {
		return ctx
	}
	attrs := make([]attribute.KeyValue, 0, len(t.attributes)+4)
	attrs = append(attrs, t.attributes...)
	poolName := ""
	if pool != nil && pool.Config() != nil {
		poolName = connectionPoolName(pool.Config().ConnConfig)
		attrs = append(attrs, semconv.DBClientConnectionPoolName(poolName))
		if t.includeConnectionDetails {
			attrs = append(attrs, connectionAttributes(pool.Config().ConnConfig)...)
		}
	}
	ctx = startOperation(ctx, operationState{poolName: poolName})
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	ctx, _ = t.tracer.Start(
		ctx,
		"crdbpgx.pool.acquire",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrs...),
	)
	return ctx
}

// TraceAcquireEnd completes tracing and timing a pgxpool Acquire call.
func (t *Tracer) TraceAcquireEnd(
	ctx context.Context,
	_ *pgxpool.Pool,
	data pgxpool.TraceAcquireEndData,
) {
	if t.disableAcquireTracer {
		return
	}
	t.recordConnectionWaitTime(ctx)
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	recordError(span, data.Err)
	span.End()
}

func queryParameterAttributes(args []any) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(args))
	for index, arg := range args {
		attrs = append(attrs, semconv.DBQueryParameter(strconv.Itoa(index), fmt.Sprintf("%+v", arg)))
	}
	return attrs
}
