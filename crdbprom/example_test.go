package crdbprom_test

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sumup/crdbpgx"
	"github.com/sumup/crdbpgx/crdbprom"
)

func ExampleNewMeter() {
	registry := prometheus.NewRegistry()
	meter, err := crdbprom.NewMeter(registry)
	if err != nil {
		panic(err)
	}
	cfg, err := pgxpool.ParseConfig("postgres://localhost/orders")
	if err != nil {
		panic(err)
	}
	pool, err := crdbpgx.NewPool(context.Background(), cfg,
		crdbpgx.WithName("orders"),
		crdbpgx.WithHooks(meter),
	)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	// Mount promhttp.HandlerFor(registry, promhttp.HandlerOpts{}) at /metrics
	// in the application's HTTP server.
}
