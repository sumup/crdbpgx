package crdbpgx_test

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumup/crdbpgx"
)

func ExampleNewPool() {
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	pool, err := crdbpgx.NewPool(
		context.Background(),
		cfg,
		crdbpgx.WithName("orders"),
		crdbpgx.WithContextPropagationBehaviour(crdbpgx.ContextPropagationOnAcquire),
		crdbpgx.WithBalancer(crdbpgx.NewNodeConnectionBalancer(
			crdbpgx.WithBalancingInterval(30*time.Second),
			crdbpgx.WithBalancingTimeout(10*time.Second),
		)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
}

func ExamplePool_InTx() {
	transferFunds := func(ctx context.Context, pool *crdbpgx.Pool, accountID string, amount int64) error {
		return pool.InTx(ctx, pgx.TxOptions{}, func(ctx context.Context, db crdbpgx.Tx) error {
			return db.ExecFunc(
				ctx,
				func(_ context.Context, _ pgconn.CommandTag, err error) error {
					return err
				},
				"UPDATE accounts SET balance = balance - $1 WHERE id = $2",
				amount,
				accountID,
			)
		})
	}

	_ = transferFunds
}
