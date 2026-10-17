# crdbprom

Optional native Prometheus metrics for [`crdbpgx`](https://github.com/sumup/crdbpgx).

```sh
go get github.com/sumup/crdbpgx/crdbprom
```

Create one meter for your registry and pass it to the pool's hooks:

```go
import (
  "github.com/prometheus/client_golang/prometheus"
  "github.com/prometheus/client_golang/prometheus/promhttp"
  "github.com/sumup/crdbpgx"
  "github.com/sumup/crdbpgx/crdbprom"
)

registry := prometheus.NewRegistry()
meter, err := crdbprom.NewMeter(registry)
if err != nil {
  return err
}
pool, err := crdbpgx.NewPool(ctx, cfg,
  crdbpgx.WithName("orders"),
  crdbpgx.WithHooks(meter),
)
if err != nil {
  return err
}
defer pool.Close()

// Mount on your application's HTTP server.
mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
```

`NewMeter` requires a `prometheus.Registerer` and returns registration conflicts as errors. Reuse the same meter across pools sharing a registry.

| Metric | Type | Labels | Buckets |
| --- | --- | --- | --- |
| `crdbpgx_resets` | Histogram | `pool_id` | 0, 1, 2, 5, 10, 20, 50 |
| `crdbpgx_balancer_acquired` | Histogram | `pool_id` | 0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50 |
| `crdbpgx_balancer_pruned` | Histogram | `pool_id` | 0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50 |
| `crdb_healthy_nodes` | Gauge | none | n/a |

The resets histogram includes serialization retries and records zero for operations that succeed without retries. When the retry limit is exhausted, it records the total number of attempts. Initial acquisition failures are excluded.

The pruned histogram counts healthy connections removed for balance, excluding connections removed because their nodes are unhealthy.

The healthy-node gauge belongs to the health tracker and starts at zero. For a caller-owned or shared tracker, configure its hooks explicitly:

```go
tracker, err := crdbpgx.NewNodeHealthTracker(cfg.ConnConfig,
  crdbpgx.WithHealthTrackerHooks(meter),
)
```

Pass that tracker to each pool with `crdbpgx.WithHealthTracker(tracker)`. The caller owns its polling and cancellation. For independent clusters, create separate meters with registerers wrapped by `prometheus.WrapRegistererWith` using distinct constant cluster labels, or use separate registries. Prefixes can be added with `prometheus.WrapRegistererWithPrefix`.

The meter can be collected concurrently with pool activity. Closing a pool does not unregister it, since other pools or trackers may still use it. After all users have stopped, `registerer.Unregister(meter)` removes its metrics.

Other observers can be registered alongside the meter. For example, `crdbpgx.WithHooks(meter, crdbotel.NewTracer())` combines native Prometheus metrics with OTEL query spans and failover events without enabling OTEL metrics.
