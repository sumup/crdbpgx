<div align="center">

# crdbpgx

[![Go Reference](https://pkg.go.dev/badge/github.com/sumup/crdbpgx.svg)](https://pkg.go.dev/github.com/sumup/crdbpgx)
[![CI Status](https://github.com/sumup/crdbpgx/workflows/CI/badge.svg)](https://github.com/sumup/crdbpgx/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/sumup/crdbpgx)](./LICENSE)

</div>

A [pgx](https://github.com/jackc/pgx)-compatible driver for [CockroachDB](https://www.cockroachlabs.com/). `crdbpgx` supports tracking of CockroachDB gateway nodes health and automatic rebalancing of connections for failing nodes or nodes draining during upgrades. The driver also transparently handles retryable errors.

Inspired by AuthZed's [Maximizing CockroachDB Performance: Our Journey to 1 Million QPS](https://authzed.com/blog/maximizing-cockroachdb-performance) and their implementation [authzed/crdbpool](https://github.com/authzed/crdbpool). Further enhanced with configurable context propagation and improved load-balancing. Distributed under the same license.

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
