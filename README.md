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
  defer func() {
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    _ = pool.Close(shutdownCtx)
  }()

  // use your pool
}
```

## Components

There are three main components of `crdbpgx`:

### Pool

The database connection pool is a thin wrapper around `pgxpool` adjusted to CockroachDB that transparently retries retryable errors. Optional features, such as the node connection balancer, are configured with `NewPool` options.

### Node Health Tracker

Tracks health of nodes in the cluster. This is done in two ways - when executing queries and in a periodical polling loop. In queries, nodes are marked as unhealthy if they fail with a resettable error (i.e. errors that are not a result of a database operation but rather of the underlying connection). The polling mechanism then periodically pings a random node in the cluster and if a node comes back to live or a new node is added their health is tracked as healthy.

The health tracker is owned by the pool as they are coupled closely together but users of this package are free to provide their own implementation of the node health tracker.

### Node Connection Balancer

Node connection balancer periodically acquires idle connections from the pool and prunes connections that make the distribution across healthy nodes uneven. Configure it with `WithBalancer(crdbpgx.NewNodeConnectionBalancer(...))` when creating a pool. The pool starts the balancer loop with pool-owned lifecycle context and stops it on `Close(ctx)`.

If you need manual lifecycle control, create a balancer with `NewNodeConnectionBalancer`, call `Run(ctx, pool)` in your own goroutine for the periodic loop, or call `Rebalance(ctx, pool)` to perform a single balancing operation.

## Credits

- [jackc/pgx](https://github.com/jackc/pgx)
- [authzed/crdbpool](https://github.com/authzed/crdbpool)

### Maintainers

- [Johannes Gräger](mailto:johannes.graeger@sumup.com)

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
