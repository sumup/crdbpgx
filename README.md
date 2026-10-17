<div align="center">

# crdbpgx

[![Go Reference](https://pkg.go.dev/badge/github.com/sumup/crdbpgx.svg)](https://pkg.go.dev/github.com/sumup/crdbpgx)
[![CI Status](https://github.com/sumup/crdbpgx/workflows/CI/badge.svg)](https://github.com/sumup/crdbpgx/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/sumup/crdbpgx)](./LICENSE)

</div>

A [pgx](https://github.com/jackc/pgx)-compatible driver for [CockroachDB](https://www.cockroachlabs.com/). `crdbpgx` supports tracking of CockroachDB gateway nodes health and automatic rebalancing of connections for failing nodes or nodes draining during upgrades. The driver also transparently handles retryable errors.

Inspired by AuthZed's [Maximizing CockroachDB Performance: Our Journey to 1 Million QPS](https://authzed.com/blog/maximizing-cockroachdb-performance) and their implementation [authzed/crdbpool](https://github.com/authzed/crdbpool). Further enhanced with configurable context propagation and improved load-balancing. Distributed under the same license.

> [!IMPORTANT]
> `crdbpgx` is not a drop-in replacement for `pgx`. It is a CockroachDB-focused wrapper around `pgxpool` that adds node health tracking, connection rebalancing, retry handling, and configurable context propagation. Those behaviors intentionally change how connections are acquired, retried, and managed so applications can better tolerate CockroachDB node failures, draining nodes during upgrades, and gateway imbalance.

## Example

```go
package main

import (
  "context"
  "fmt"
  "os"
  "os/signal"
  "time"

  "github.com/jackc/pgx/v5/pgxpool"
  "github.com/sumup/crdbpgx"
)

func main() {
  // your application context
  ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
  defer cancel()

  cfg, err := pgxpool.ParseConfig(os.Getenv("DSN"))
  if err != nil {
    panic(fmt.Errorf("parse dsn: %w", err))
  }

  pool, err := crdbpgx.NewPool(
    ctx, // context used only while the pool is created
    cfg,
    crdbpgx.WithContextPropagationBehaviour(crdbpgx.ContextPropagationOnAcquire),
    crdbpgx.WithBalancer(crdbpgx.NewNodeConnectionBalancer(
      crdbpgx.WithBalancingInterval(30*time.Second),
      crdbpgx.WithBalancingTimeout(10*time.Second),
    )),
  )
  if err != nil {
    panic(fmt.Errorf("create read pool: %w", err))
  }
  defer pool.Close()

  // use your pool
}
```

## Typed queries

`Queries` provides generic `Query` and `QueryRow` methods on Go 1.27+. Wrap a
pool or transaction with `NewQueries` to share the same query code. The embedded
`Runner` retains the callback API and transaction behavior:

```go
// db := crdbpgx.NewQueries(pool)
// Inside a transaction: db := crdbpgx.NewQueries(tx)

func listIDs(ctx context.Context, db crdbpgx.Queries) ([]int64, error) {
  return db.Query(ctx, pgx.RowTo[int64], "SELECT id FROM users")
}

func getID(ctx context.Context, db crdbpgx.Queries, name string) (int64, error) {
  return db.QueryRow(ctx, pgx.RowTo[int64],
    "SELECT id FROM users WHERE name = $1", name)
}
```

Both helpers close their rows. `QueryRow` returns the first row, ignores additional
rows, and returns `pgx.ErrNoRows` for an empty result. On error, `Query` returns
nil and `QueryRow` returns the zero value of the result type.

Pool operations retry the query and collection together. Inside a transaction,
errors propagate to the outer transaction retry loop. Row collectors can run
again during retries and should avoid external side effects.

## Components

There are three main components of `crdbpgx`:

### Pool

The database connection pool is a thin wrapper around `pgxpool` adjusted to CockroachDB that transparently retries retryable errors. Optional features, such as the node connection balancer, are configured with `NewPool` options.

### Node Health Tracker

Tracks health of nodes in the cluster. This is done in two ways - when executing queries and in a periodical polling loop. In queries, nodes are marked as unhealthy if they fail with a resettable error (i.e. errors that are not a result of a database operation but rather of the underlying connection). The polling mechanism then periodically pings a random node in the cluster and if a node comes back to live or a new node is added their health is tracked as healthy.

After a topology change, the built-in health tracker temporarily uses its topology-change polling interval for 5 minutes. By default this interval is 5 seconds. This helps discover newly added or rejoined nodes faster during rolling upgrades and scale-up events when configured with a lower interval. The accelerated interval and window can be configured with `WithTopologyChangePollInterval`.

The health tracker is owned by the pool as they are coupled closely together but users of this package are free to provide their own implementation of the node health tracker.

### Node Connection Balancer

Node connection balancer periodically acquires idle connections from the pool and prunes connections that make the distribution across healthy nodes uneven. Configure it with `WithBalancer(crdbpgx.NewNodeConnectionBalancer(...))` when creating a pool. The pool starts the balancer loop with pool-owned lifecycle context and stops it on `Close()`.

When used with the built-in health tracker, the balancer also reacts to topology changes: it rebalances immediately, then temporarily lowers its interval from 30 seconds to 5 seconds for 5 minutes. The accelerated interval and window can be configured with `WithTopologyChangeBalancingInterval`.

If you need manual lifecycle control, create a balancer with `NewNodeConnectionBalancer`, call `Run(ctx, pool)` in your own goroutine for the periodic loop, or call `Rebalance(ctx, pool)` to perform a single balancing operation.

## Instrumentation

Instrumentation is optional. Choose the adapters you need:

| Module | Provides |
| --- | --- |
| [crdbotel](./crdbotel) | [OpenTelemetry](https://opentelemetry.io/) metrics, query spans, and connection failover events |
| [crdbprom](./crdbprom) | Native [Prometheus](https://prometheus.io/) metrics |

Register instrumentation with `WithHooks` when creating a pool:

```go
pool, err := crdbpgx.NewPool(ctx, cfg,
  crdbpgx.WithName("orders"),
  crdbpgx.WithHooks(crdbotel.NewMeter(), crdbotel.NewTracer()),
)
if err != nil {
  return err
}
defer pool.Close()
```

Metrics and tracing can be enabled independently or combined with other observers. For example, `WithHooks(prometheusMeter, crdbotel.NewTracer())` combines native Prometheus metrics with OpenTelemetry query spans and failover events. Query hooks are automatically composed with any existing `cfg.ConnConfig.Tracer`.

`crdbotel.NewTracer(crdbotel.WithTracerProvider(provider))` selects a provider for query spans. Without an explicit provider, the global OpenTelemetry tracer provider is used.

Pool hooks also cover the built-in balancer and automatically created health tracker. For a caller-created or shared tracker, configure instrumentation with `WithHealthTrackerHooks` and pass the tracker to each pool with `WithHealthTracker`:

```go
tracker, err := crdbpgx.NewNodeHealthTracker(cfg.ConnConfig,
  crdbpgx.WithHealthTrackerHooks(meter),
)
```

### Custom Hooks

Embed `crdbpgx.HooksDefaults` by value and implement the callbacks you need:

```go
type retryLogger struct {
  crdbpgx.HooksDefaults
}

var _ crdbpgx.Hook = retryLogger{}

func (retryLogger) OnOperationEnd(ctx context.Context, data crdbpgx.OperationEndData) {
  if data.Retries > 0 {
    slog.DebugContext(ctx, "database operation retried",
      "pool", data.PoolName, "retries", data.Retries)
  }
}

// Pass this option when creating a pool.
option := crdbpgx.WithHooks(meter, retryLogger{})
```

`Hook` defines all lifecycle callbacks; `HooksDefaults` supplies no-op implementations, including for callbacks added in future versions. Repeated `WithHooks` and `WithHealthTrackerHooks` calls append observers. Hooks run synchronously in registration order and may run concurrently across operations. They must return promptly and support concurrent use.

Pool hooks also implement pgx's `TraceQueryStart` and `TraceQueryEnd`. These callbacks observe each `Exec`, `Query`, and `QueryRow` attempt, including queries on directly acquired connections. `TraceQueryStart` must return a context, which is passed to subsequent hooks and pgx. `TraceQueryEnd` runs when pgx completes the query or closes its rows. `OnOperationEnd` reports the full retry-managed operation.

## Maintainers

- [Johannes Gräger](mailto:johannes.graeger@sumup.com)
- [Matouš Dzivjak](mailto:matous.dzivjak@sumup.com)

## Integration Testing

There is an opt-in Docker-backed integration test that starts a 3-node CockroachDB cluster, stops one node, restarts it, and asserts that `crdbpgx` recovers and re-discovers the node.

Requirements:

- Docker with the local daemon running
- free host ports `26257-26259` and `8080-8082`

Run it with:

```sh
make test-integration
```

The cluster definition lives in [`testdata/crdb-cluster/docker-compose.yml`](./testdata/crdb-cluster/docker-compose.yml). By default it uses `cockroachdb/cockroach:v26.2.0`, but you can override that for local experiments:

```sh
CRDB_IMAGE=cockroachdb/cockroach:v25.1.10 make test-integration
```

### Typed query integration tests

To test typed queries, retries, and nested transactions against an existing
CockroachDB test database, set `CRDBPGX_TEST_DATABASE_URL`:

```sh
CRDBPGX_TEST_DATABASE_URL='postgres://root@localhost:26257/defaultdb?sslmode=disable' \
  go test -race -run TestQueryIntegration -v ./...
```

The test creates and removes its own table. Without this variable, the database
integration test is skipped; typed query unit tests still run.
