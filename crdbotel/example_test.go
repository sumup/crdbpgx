package crdbotel_test

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumup/crdbpgx"
	"github.com/sumup/crdbpgx/crdbotel"
)

func ExampleNewMeter() {
	cfg, err := pgxpool.ParseConfig("postgres://localhost/orders")
	if err != nil {
		panic(err)
	}
	pool, err := crdbpgx.NewPool(context.Background(), cfg,
		crdbpgx.WithName("orders"),
		crdbpgx.WithHooks(crdbotel.NewMeter(), crdbotel.NewTracer()),
	)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
}

func ExampleWithSpanNameFunc() {
	type queryNameKey struct{}
	tracer := crdbotel.NewTracer(crdbotel.WithSpanNameFunc(func(ctx context.Context, _ string) string {
		name, _ := ctx.Value(queryNameKey{}).(string)
		return name
	}))
	cfg, err := pgxpool.ParseConfig("postgres://localhost/orders")
	if err != nil {
		panic(err)
	}
	pool, err := crdbpgx.NewPool(context.Background(), cfg, crdbpgx.WithHooks(tracer))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	ctx := context.WithValue(context.Background(), queryNameKey{}, "FindOrder")
	err = pool.QueryRowFunc(ctx, func(_ context.Context, row pgx.Row) error {
		var id int
		return row.Scan(&id)
	}, "SELECT $1", 42)
	if err != nil {
		panic(err)
	}
}
