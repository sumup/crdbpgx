package crdbpgx_test

import (
	"context"
	"fmt"
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

func ExamplePool_InTx_queryAndUpdate() {
	type User struct {
		ID        string    `db:"id"`
		Name      string    `db:"name"`
		Email     string    `db:"email"`
		UpdatedAt time.Time `db:"updated_at"`
	}

	updateUser := func(ctx context.Context, pool *crdbpgx.Pool, userID, name, email string) error {
		return pool.InTx(ctx, pgx.TxOptions{}, func(ctx context.Context, tx crdbpgx.Tx) error {
			user, err := tx.QueryRowFunc(
				ctx,
				pgx.RowToStructByName[User],
				`SELECT id, name, email, updated_at FROM users WHERE id = $1 FOR UPDATE`,
				userID,
			)
			if err != nil {
				return fmt.Errorf("query user: %w", err)
			}

			user.Name = name
			user.Email = email
			user.UpdatedAt = time.Now()

			tag, err := tx.Exec(
				ctx,
				`UPDATE users SET name = $2, email = $3, updated_at = $4 WHERE id = $1`,
				user.ID,
				user.Name,
				user.Email,
				user.UpdatedAt,
			)
			if err != nil {
				return fmt.Errorf("update user: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("update user: expected one affected row, got %d", tag.RowsAffected())
			}

			return nil
		})
	}

	_ = updateUser
}
