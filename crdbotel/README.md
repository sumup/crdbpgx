# crdbotel

Optional OpenTelemetry metrics, query spans, and connection failover events for [`crdbpgx`](https://github.com/sumup/crdbpgx).

```sh
go get github.com/sumup/crdbpgx/crdbotel
```

Register a meter, a tracer, or both with `crdbpgx.WithHooks`:

```go
pool, err := crdbpgx.NewPool(ctx, cfg,
  crdbpgx.WithName("orders"),
  crdbpgx.WithHooks(
    crdbotel.NewMeter(crdbotel.WithMeterProvider(meterProvider)),
    crdbotel.NewTracer(crdbotel.WithTracerProvider(tracerProvider)),
  ),
)
if err != nil {
  return err
}
defer pool.Close()
```

Both constructors use the global OpenTelemetry provider when no provider is
specified or when nil is passed. The application owns provider flushing and
shutdown.

| Metric | Type | Attributes |
| --- | --- | --- |
| `crdbpgx.resets` | Histogram | `pool.id` |
| `crdbpgx.balancer.acquired` | Histogram | `pool.id` |
| `crdbpgx.balancer.pruned` | Histogram | `pool.id` |
| `crdb_healthy_nodes` | Gauge | none |

The resets histogram includes serialization retries and records zero for operations that succeed without retries. When the retry limit is exhausted, it records the total number of attempts.

The pruned histogram counts healthy connections removed for balance, excluding connections removed because their nodes are unhealthy.

The healthy-node gauge reflects the latest tracker snapshot. For a caller-owned or shared tracker, configure `crdbpgx.WithHealthTrackerHooks(meter)` explicitly.

`WithMeterAttributes(attrs...)` adds attributes to every metric. For example, use a cluster name to distinguish independent databases. Repeated calls append attributes, and the configured pool name takes precedence for `pool.id` on pool metrics.

`NewTracer()` creates a client span for each `Exec`, `Query`, and `QueryRow` attempt, including retries and queries on directly acquired connections.

| Tracer option | Behavior |
| --- | --- |
| `WithTracerAttributes(attrs...)` | Adds attributes to query spans |
| `WithSpanNameFunc(fn)` | Generates the summary from the query context and SQL |
| `WithDisableConnectionDetailsInAttributes()` | Omits the namespace, server endpoints, and network peer; default names use the DBMS name |
| `WithDisableSQLStatementInAttributes()` | Omits query text and parameter values |
| `WithIncludeQueryParameters(filter...)` | Enables parameter values, optionally restricted by a SQL predicate |

Name callbacks should return low-cardinality summaries, such as `FindOrder`. An empty result uses default naming, and nil restores default naming. Summaries are limited to 255 bytes. Callbacks must support concurrent use. Naming remains available when SQL text collection is disabled.

For context-based naming, use an application-specific context key:

```go
type queryNameKey struct{}

tracer := crdbotel.NewTracer(
  crdbotel.WithSpanNameFunc(func(ctx context.Context, _ string) string {
    name, _ := ctx.Value(queryNameKey{}).(string)
    return name
  }),
  crdbotel.WithTracerAttributes(attribute.String("cluster", "primary")),
)

// Register tracer with WithHooks, then pass queryCtx to a database operation.
queryCtx := context.WithValue(ctx, queryNameKey{}, "FindOrder")
```

Parameterized query templates are recorded as `db.query.text`, limited to 16 KiB. Positional parameters, `pgx.NamedArgs`, and `pgx.StrictNamedArgs` are supported. Non-parameterized text and text supplied through opaque query rewriters are not collected.

Parameter values are disabled by default. `WithIncludeQueryParameters()` enables `db.query.parameter.<key>` string attributes, using zero-based indices for positional parameters and names for named parameters. Unused named arguments are omitted, and each value is limited to 16 KiB. A filter can restrict capture:

```go
crdbotel.WithIncludeQueryParameters(func(sql string) bool {
  return sql == "SELECT id FROM catalog WHERE id = $1"
})
```

Only enable parameter capture for values that may be recorded in telemetry. Disabling SQL text collection also disables parameter capture, regardless of option order. Additional tracer attributes apply to query spans; collected attributes take precedence over additional attributes with the same key.

Database errors record their SQLSTATE as `error.type` and `db.response.status_code`; other failures use their unwrapped Go error type. Successful calls leave the span status unset. Empty results are not failures. Raw error messages and exception events are not recorded; applications can log errors where they handle them.

Connection failover events are added to the caller's span. An existing `cfg.ConnConfig.Tracer` is preserved and runs after the registered hooks. Batch, `CopyFrom`, preparation, and connection spans are left to any separately configured pgx tracer.

See the [instrumentation guide](../README.md#instrumentation) for shared tracker configuration and custom hooks.
