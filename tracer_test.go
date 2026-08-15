package crdbpgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

type queryTracer struct {
	crdbpgx.NopTracer
}

func (*queryTracer) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryStartData,
) context.Context {
	return ctx
}

func (*queryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestWithTracer(t *testing.T) {
	t.Parallel()

	t.Run("installs compatible pgx query tracer", func(t *testing.T) {
		t.Parallel()

		cfg, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:1/db?pool_min_conns=0&pool_min_idle_conns=0")
		require.NoError(t, err)

		tracer := new(queryTracer)
		pool, err := crdbpgx.NewPool(
			t.Context(),
			cfg,
			crdbpgx.WithHealthTracker(fakeHealthTracker{}),
			crdbpgx.WithTracer(tracer),
		)
		require.NoError(t, err)
		t.Cleanup(pool.Close)

		require.Same(t, tracer, pool.Config().ConnConfig.Tracer)
	})

	t.Run("preserves an existing pgx query tracer", func(t *testing.T) {
		t.Parallel()

		cfg, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:1/db?pool_min_conns=0&pool_min_idle_conns=0")
		require.NoError(t, err)

		existingTracer := new(queryTracer)
		cfg.ConnConfig.Tracer = existingTracer
		pool, err := crdbpgx.NewPool(
			t.Context(),
			cfg,
			crdbpgx.WithHealthTracker(fakeHealthTracker{}),
			crdbpgx.WithTracer(new(queryTracer)),
		)
		require.NoError(t, err)
		t.Cleanup(pool.Close)

		require.Same(t, existingTracer, pool.Config().ConnConfig.Tracer)
	})
}
