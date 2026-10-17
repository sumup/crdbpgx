package crdbpgx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Runner executes typed queries and transactions through an Executor.
// Pool embeds Runner, so its methods can be called directly on the pool.
// A transaction callback receives another Runner bound to that transaction.
type Runner struct {
	Executor
}

var _ Executor = Runner{}

// NewRunner binds typed operations to db. It does not own or close db.
// Pools initialize their embedded Runner automatically.
func NewRunner(db Executor) Runner {
	return Runner{Executor: db}
}

// Exec executes a SQL command and returns its command tag.
// A pool retries retryable errors. Transaction errors propagate to the outer
// transaction retry loop. On error Exec returns an empty command tag.
func (r Runner) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	var tag pgconn.CommandTag
	err := r.ExecFunc(ctx, func(_ context.Context, result pgconn.CommandTag, err error) error {
		tag = result
		return err
	}, sql, args...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return tag, nil
}

// InTx runs fn with a Runner bound to a transaction and commits on success.
// On retryable errors a pool retries the whole transaction, including fn.
// When already inside a transaction, InTx creates a savepoint and propagates
// errors to the outer retry loop. Nested transactions ignore opts and inherit
// the ambient isolation level. fn must not retain the transaction Runner.
func (r Runner) InTx(ctx context.Context, opts pgx.TxOptions, fn func(Runner) error) error {
	return r.InTxFunc(ctx, opts, func(tx Tx) error { return fn(NewRunner(tx)) })
}

// Query executes a query and collects its rows with rowTo.
// It closes the rows before returning and returns nil on error.
// A pool retries the entire query, including collection, on retryable errors.
// Within a transaction, errors propagate to the enclosing transaction's retry loop.
// rowTo may be called again on retry and should avoid external side effects.
func (r Runner) Query[T any](ctx context.Context, rowTo pgx.RowToFunc[T], sql string, args ...any) ([]T, error) {
	var result []T
	err := r.QueryFunc(ctx, func(_ context.Context, rows pgx.Rows) error {
		var err error
		result, err = pgx.CollectRows(rows, rowTo)
		return err
	}, sql, args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// QueryRow executes a query and collects its first row with rowTo.
// It closes the rows before returning, ignores additional rows, and returns
// pgx.ErrNoRows if the query returns no rows. On error it returns the zero value of T.
// It has the same retry behavior and rowTo requirements as Query.
func (r Runner) QueryRow[T any](ctx context.Context, rowTo pgx.RowToFunc[T], sql string, args ...any) (T, error) {
	var result T
	err := r.QueryFunc(ctx, func(_ context.Context, rows pgx.Rows) error {
		var err error
		result, err = pgx.CollectOneRow(rows, rowTo)
		return err
	}, sql, args...)
	if err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}
