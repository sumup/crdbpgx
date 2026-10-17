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

## Typed Queries and Transactions

`Pool` embeds a concrete `Runner`, so typed queries and commands are available
as `pool.Query`, `pool.QueryRow`, and `pool.Exec` on Go 1.27+:

```go
users, err := pool.Query(ctx, pgx.RowToStructByName[User], "SELECT id, name FROM users")

err = pool.InTx(ctx, pgx.TxOptions{}, func(tx crdbpgx.Runner) error {
  user, err := tx.QueryRow(ctx, pgx.RowToStructByName[User],
    "SELECT id, name FROM users WHERE id = $1", userID)
  if err != nil {
    return err
  }
  _, err = tx.Exec(ctx, "UPDATE users SET name = $1 WHERE id = $2", name, user.ID)
  return err
})
```

Store `pool.Runner` in repository code to use the same concrete type inside and
outside transactions:

```go
type Repository struct {
  db crdbpgx.Runner
}

repo := Repository{db: pool.Runner}
```

`Runner.InTx` supplies a transaction-bound `Runner` automatically. Calling it on
that runner creates a savepoint. Only the outer pool retries the entire
transaction. Callbacks can execute again during retries and should avoid
external side effects. Do not retain a transaction runner after its callback.

Both query methods close their rows. `QueryRow` returns the first row, ignores
additional rows, and returns `pgx.ErrNoRows` for an empty result. On error,
`Query` returns nil, `QueryRow` returns the zero value, and `Exec` returns an
empty command tag. Typed transaction operations honor the pool's configured
context propagation behavior.

`Executor` is the underlying callback interface for adapters and test doubles.
`NewRunner(executor)` wraps these implementations. Existing callback methods
remain available. `InTxFunc` still supplies a `Tx` for callers that need the raw
`pgx.Tx` methods.

This is a breaking API change: the former `Runner` interface is now `Executor`,
`Runner` is concrete, and `InTx` callbacks receive `func(Runner) error`.

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

`crdbotel.NewMeter()` automatically registers pool statistics through `WithHooks` and unregisters them on `pool.Close()`. Use a unique `WithName` for each pool. Statistics include idle and acquired connections (`db.client.connection.count`, with `idle` and `used` states), the pool limit (`db.client.connection.max`), and `crdbpgx.pool.*` metrics for connections being constructed, successful/canceled acquisitions, acquisitions that waited, new connections, and connections destroyed due to lifetime or idle limits. All carry `db.client.connection.pool.name` and any `WithMeterAttributes` attributes. Acquisition duration and wait duration are cumulative seconds for successful acquisitions. pgx does not expose the number of pending acquisitions or separate timeouts from other cancellations, so those metrics are not emitted.

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
