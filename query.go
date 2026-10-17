package crdbpgx

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Queries provides typed query methods for a pool or transaction.
// Its embedded Runner preserves the callback API and transaction retry behavior.
// Use NewQueries to bind it to a pool, transaction, or another Runner.
type Queries struct {
	Runner
}

var _ Runner = Queries{}

// NewQueries binds typed query methods to db. It does not own or close db.
func NewQueries(db Runner) Queries {
	return Queries{Runner: db}
}

// Query executes a query and collects its rows with rowTo.
// It closes the rows before returning and returns nil on error.
// A pool retries the entire query, including collection, on retryable errors.
// Within a transaction, errors propagate to the enclosing transaction's retry loop.
// rowTo may be called again on retry and should avoid external side effects.
func (q Queries) Query[T any](ctx context.Context, rowTo pgx.RowToFunc[T], sql string, args ...any) ([]T, error) {
	var result []T
	err := q.QueryFunc(ctx, func(_ context.Context, rows pgx.Rows) error {
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
func (q Queries) QueryRow[T any](ctx context.Context, rowTo pgx.RowToFunc[T], sql string, args ...any) (T, error) {
	var result T
	err := q.QueryFunc(ctx, func(_ context.Context, rows pgx.Rows) error {
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
