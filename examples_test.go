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
		return pool.InTx(ctx, pgx.TxOptions{}, func(ctx context.Context, db crdbpgx.Runner) error {
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

// The same helper accepts a pool or a transaction through Runner.
func ExampleQueries_Query() {
	listIDs := func(ctx context.Context, db crdbpgx.Queries) ([]int64, error) {
		return db.Query(ctx, pgx.RowTo[int64],
			"SELECT id FROM users WHERE active = @active", pgx.NamedArgs{"active": true})
	}
	_ = listIDs
}

func ExampleQueries_QueryRow() {
	type User struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	getUser := func(ctx context.Context, db crdbpgx.Queries, id int64) (User, error) {
		return db.QueryRow(ctx, pgx.RowToStructByName[User],
			"SELECT id, name FROM users WHERE id = $1", id)
	}
	_ = getUser
}
