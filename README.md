<div align="center">

# crdbpgx

[![Go Reference](https://pkg.go.dev/badge/github.com/sumup/crdbpgx.svg)](https://pkg.go.dev/github.com/sumup/crdbpgx)
[![CI Status](https://github.com/sumup/crdbpgx/workflows/CI/badge.svg)](https://github.com/sumup/crdbpgx/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/sumup/crdbpgx)](./LICENSE)

</div>

A [pgx](https://github.com/jackc/pgx)-compatible driver for [CockroachDB](https://www.cockroachlabs.com/). `crdbpgx` detects CockroachDB gateway nodes health and automatically rebalances connections to ensure optimal performance, even when nodes are failing or draining during upgrades. The driver also transparently handles retryable errors, even for unavailable or draining nodes.

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

  healthChecker, err := crdbpgx.NewNodeHealthChecker(cfg.ConnString())
  if err != nil {
    panic(fmt.Errorf("create node health checker: %w", err))
  }

  pool, err = crdbpgx.NewPool(
    ctx, // application context that the pool life-time is tied to
    cfg,
    healthChecker,
    crdbpgx.WithContextPropagationBehaviour(crdbpgx.ContextPropagationOnAcquire),
  )
  if err != nil {
    panic(fmt.Errorf("create read pool: %w", err))
  }

  // use your pool
}
```

## Credits

- [jackc/pgx](https://github.com/jackc/pgx)
- [authzed/crdbpool](https://github.com/authzed/crdbpool)

### Maintainers

- [Johannes Gräger](mailto:johannes.graeger@sumup.com)
