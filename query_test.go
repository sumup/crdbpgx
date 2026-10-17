package crdbpgx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

// Embed unused methods so these fixtures only implement the collection surface.
type queryRunner struct {
	crdbpgx.Executor
	query func(context.Context, func(context.Context, pgx.Rows) error, string, ...any) error
}

func (r queryRunner) QueryFunc(ctx context.Context, fn func(context.Context, pgx.Rows) error, sql string, args ...any) error {
	return r.query(ctx, fn, sql, args...)
}

type queryRows struct {
	pgx.Rows
	values []int
	next   int
	closed bool
	err    error
}

func (r *queryRows) Next() bool {
	if r.next == len(r.values) {
		return false
	}
	r.next++
	return true
}
func (r *queryRows) Scan(dest ...any) error { *dest[0].(*int) = r.values[r.next-1]; return nil }
func (r *queryRows) Close()                 { r.closed = true }
func (r *queryRows) Err() error             { return r.err }

func runnerForRows(rows *queryRows) crdbpgx.Executor {
	return queryRunner{query: func(ctx context.Context, fn func(context.Context, pgx.Rows) error, _ string, _ ...any) error {
		return fn(ctx, rows)
	}}
}

func TestQuery(t *testing.T) {
	t.Parallel()

	t.Run("forwards arguments and closes rows", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		rows := &queryRows{values: []int{1, 2}}
		db := queryRunner{query: func(gotCtx context.Context, fn func(context.Context, pgx.Rows) error, sql string, args ...any) error {
			require.Equal(t, ctx, gotCtx)
			require.Equal(t, "SELECT @value", sql)
			require.Equal(t, []any{pgx.NamedArgs{"value": 42}}, args)
			return fn(gotCtx, rows)
		}}
		got, err := crdbpgx.NewRunner(db).Query(ctx, pgx.RowTo[int], "SELECT @value", pgx.NamedArgs{"value": 42})
		require.NoError(t, err)
		require.Equal(t, []int{1, 2}, got)
		require.True(t, rows.closed)
	})

	t.Run("empty rows", func(t *testing.T) {
		t.Parallel()

		rows := &queryRows{}
		got, err := crdbpgx.NewRunner(runnerForRows(rows)).Query(t.Context(), pgx.RowTo[int], "query")
		require.NoError(t, err)
		require.Empty(t, got)
		require.True(t, rows.closed)
	})

	t.Run("row stream error discards collected values", func(t *testing.T) {
		t.Parallel()

		marker := errors.New("row stream failed")
		rows := &queryRows{values: []int{1}, err: marker}
		got, err := crdbpgx.NewRunner(runnerForRows(rows)).Query(t.Context(), pgx.RowTo[int], "query")
		require.ErrorIs(t, err, marker)
		require.Nil(t, got)
		require.True(t, rows.closed)
	})

	t.Run("retry replaces partial results", func(t *testing.T) {
		t.Parallel()

		marker := errors.New("retry collection")
		first := &queryRows{values: []int{1, 2}}
		second := &queryRows{values: []int{3}}
		db := queryRunner{query: func(ctx context.Context, fn func(context.Context, pgx.Rows) error, _ string, _ ...any) error {
			require.ErrorIs(t, fn(ctx, first), marker)
			require.True(t, first.closed)
			return fn(ctx, second)
		}}
		got, err := crdbpgx.NewRunner(db).Query(t.Context(), func(row pgx.CollectableRow) (int, error) {
			n, err := pgx.RowTo[int](row)
			if n == 2 {
				return n, marker
			}
			return n, err
		}, "query")
		require.NoError(t, err)
		require.Equal(t, []int{3}, got)
		require.True(t, second.closed)
	})
}

func TestQueryRow(t *testing.T) {
	t.Parallel()

	t.Run("first row and closes rows", func(t *testing.T) {
		t.Parallel()

		rows := &queryRows{values: []int{1, 2}}
		got, err := crdbpgx.NewRunner(runnerForRows(rows)).QueryRow(t.Context(), pgx.RowTo[int], "query")
		require.NoError(t, err)
		require.Equal(t, 1, got)
		require.True(t, rows.closed)
	})

	t.Run("no rows", func(t *testing.T) {
		t.Parallel()

		rows := &queryRows{}
		got, err := crdbpgx.NewRunner(runnerForRows(rows)).QueryRow(t.Context(), pgx.RowTo[int], "query")
		require.ErrorIs(t, err, pgx.ErrNoRows)
		require.Zero(t, got)
		require.True(t, rows.closed)
	})

	t.Run("mapper error returns zero value", func(t *testing.T) {
		t.Parallel()

		marker := errors.New("mapper failed")
		rows := &queryRows{values: []int{1}}
		got, err := crdbpgx.NewRunner(runnerForRows(rows)).QueryRow(t.Context(), func(pgx.CollectableRow) (int, error) { return 99, marker }, "query")
		require.ErrorIs(t, err, marker)
		require.Zero(t, got)
		require.True(t, rows.closed)
	})

	t.Run("failed retry discards earlier value", func(t *testing.T) {
		t.Parallel()

		marker := errors.New("retry failed")
		db := queryRunner{query: func(ctx context.Context, fn func(context.Context, pgx.Rows) error, _ string, _ ...any) error {
			require.NoError(t, fn(ctx, &queryRows{values: []int{99}}))
			return marker
		}}
		got, err := crdbpgx.NewRunner(db).QueryRow(t.Context(), pgx.RowTo[int], "query")
		require.ErrorIs(t, err, marker)
		require.Zero(t, got)
	})
}
