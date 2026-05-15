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
  "os"
  "os/signal"
  "fmt"

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

  pool, err = crdbpgx.NewPool(
    ctx, // application context that the pool life-time is tied to
    cfg,
    crdbpgx.WithContextPropagationBehaviour(crdbpgx.ContextPropagationOnAcquire),
  )
  if err != nil {
    panic(fmt.Errorf("create read pool: %w", err))
  }

  // use your pool
}
```

## Components

There are free main components of `crdbpgx`:

### Pool

The database connection pool which is a thin wrapper around `pgxpool` adjusted to CockroachDB that transparently retries retryable errors.

### Node health tracker

Tracks health of nodes in the cluster. This is done in two ways - when executing queries and in a periodical polling loop. In queries, nodes are marked as unhealthy if they fail with a resettable error (i.e. errors that are not a result of a database operation but rather of the underlying connection). The polling mechanism then periodically pings a random node in the cluster and if a node comes back to live or a new node is added their health is tracked as healthy.

The health tracker is owned by the pool as they are coupled closely together but users of this package are free to provide their own implementation of the node health tracker.

### Node connection balancer

Node connection balancer wraps the connection pool with rebalancing functionality. At a specified period the balancer acquires idle connections from the pool and prunes those, that are making the connection distribution among nodes uneven. The balancer functionality ensures that the load is evenly distributed among the nodes in the cluster even during upgrades where nodes drain and come back online or when the cluster is scaled.

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
