//go:build integration

package crdbpgx

import (
	"context"
	"crypto/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newIntegrationPool(t *testing.T, maxConns int32, opts ...Option) *Pool {
	t.Helper()

	url := os.Getenv("CRDB_TEST_DATABASE_URL")
	require.NotEmpty(t, url, "set CRDB_TEST_DATABASE_URL to a disposable CockroachDB database")
	cfg, err := pgxpool.ParseConfig(url)
	// Do not print connection errors or configuration: the URL may contain credentials.
	require.True(t, err == nil, "parse integration database configuration")
	cfg.MaxConns = maxConns
	cfg.MinConns = 0
	cfg.MinIdleConns = 0
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	opts = append([]Option{WithContextPropagationBehaviour(ContextPropagationPassthrough)}, opts...)
	pool, err := NewPool(t.Context(), cfg, opts...)
	require.True(t, err == nil, "create integration pool")
	t.Cleanup(pool.Close)
	return pool
}

func TestIntegrationPool_InTx(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		disableRetries bool
		alwaysRetry    bool
		wantAttempts   int
		wantRows       int
	}{
		{name: "retries the entire transaction", wantAttempts: 2, wantRows: 1},
		{name: "stops at the retry limit", alwaysRetry: true, wantAttempts: 3},
		{name: "honors disabled retries", disableRetries: true, wantAttempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool := newIntegrationPool(t, 1, WithMaxRetries(2))
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			table := pgx.Identifier{"crdbpgx_" + strings.ToLower(rand.Text())}.Sanitize()
			require.NoError(t, pool.ExecFunc(ctx, func(_ context.Context, _ pgconn.CommandTag, err error) error {
				return err
			}, "CREATE TABLE "+table+" (value INT)"))
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				assert.NoError(t, pool.ExecFunc(cleanupCtx, func(_ context.Context, _ pgconn.CommandTag, err error) error {
					return err
				}, "DROP TABLE "+table))
			})

			if tc.disableRetries {
				ctx = WithDisableRetries(ctx)
			}
			var attempts int
			err := pool.InTx(ctx, pgx.TxOptions{}, func(ctx context.Context, db Runner) error {
				attempts++
				if err := db.ExecFunc(ctx, func(_ context.Context, _ pgconn.CommandTag, err error) error {
					return err
				}, "INSERT INTO "+table+" VALUES (1)"); err != nil {
					return err
				}
				if attempts == 1 || tc.alwaysRetry {
					// Returning a real serialization failure also aborts the server-side transaction.
					return db.ExecFunc(ctx, func(_ context.Context, _ pgconn.CommandTag, err error) error {
						return err
					}, "SELECT crdb_internal.force_retry('1s'::INTERVAL)")
				}
				return nil
			})
			if tc.wantRows == 1 {
				require.NoError(t, err)
			} else {
				var maxRetry *MaxRetryError
				require.ErrorAs(t, err, &maxRetry)
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, CrdbRetryErrCode, pgErr.Code)
			}
			assert.Equal(t, tc.wantAttempts, attempts)

			var rows int
			require.NoError(t, pool.QueryRowFunc(ctx, func(_ context.Context, row pgx.Row) error {
				return row.Scan(&rows)
			}, "SELECT count(*) FROM "+table))
			assert.Equal(t, tc.wantRows, rows, "failed attempts must roll back their writes")
		})
	}
}

func TestIntegrationNodeConnectionBalancer_Rebalance(t *testing.T) {
	t.Parallel()

	tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{})
	require.NoError(t, err)
	pool := newIntegrationPool(t, 4, WithHealthTracker(tracker))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	conns := make([]*pgxpool.Conn, 0, 4)
	original := make([]*pgx.Conn, 0, 4)
	for range 4 {
		conn, err := pool.Acquire(ctx)
		require.NoError(t, err)
		t.Cleanup(conn.Release)
		conns = append(conns, conn)
		original = append(original, conn.Conn())
	}
	node := pool.NodeID(original[0])
	require.NotZero(t, node)
	for _, conn := range original {
		require.Equal(t, node, pool.NodeID(conn), "run this test against a single gateway")
	}
	for _, conn := range conns {
		conn.Release()
	}
	// pgx runs AfterRelease asynchronously; this waits for real connection lifecycle work.
	require.Eventually(t, func() bool {
		return pool.Stat().IdleConns() == 4
	}, 5*time.Second, 10*time.Millisecond)

	for range 3 {
		tracker.SetStatus(node, false)
	}
	require.False(t, tracker.IsHealthy(node))
	balancer := NewNodeConnectionBalancer().(*nodeConnectionBalancer)
	t.Cleanup(balancer.ticker.Stop)
	balancer.Rebalance(ctx, pool)
	require.Eventually(t, func() bool {
		return pool.Stat().TotalConns() == 0
	}, 5*time.Second, 10*time.Millisecond)

	var value int
	require.NoError(t, pool.QueryRowFunc(ctx, func(_ context.Context, row pgx.Row) error {
		return row.Scan(&value)
	}, "SELECT 1"))
	assert.Equal(t, 1, value)
	assert.True(t, tracker.IsHealthy(node), "a replacement connection restores node health")
}
