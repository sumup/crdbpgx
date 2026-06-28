package crdbpgx_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

type fakeHealthTracker struct{}

func (fakeHealthTracker) SetStatus(uint32, bool) {}
func (fakeHealthTracker) IsHealthy(uint32) bool  { return true }
func (fakeHealthTracker) NumHealthy() int        { return 1 }

type lifecycleBalancer struct {
	started chan struct{}
	stopped chan struct{}
	release chan struct{}
}

func newLifecycleBalancer() *lifecycleBalancer {
	return &lifecycleBalancer{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *lifecycleBalancer) Run(ctx context.Context, _ *crdbpgx.Pool) {
	close(b.started)
	<-ctx.Done()
	<-b.release
	close(b.stopped)
}

func (b *lifecycleBalancer) Rebalance(context.Context, *crdbpgx.Pool) {}

func TestPool_Close(t *testing.T) {
	t.Parallel()

	t.Run("shuts down dependencies", func(t *testing.T) {
		t.Parallel()

		cfg, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:1/db?pool_min_conns=0&pool_min_idle_conns=0")
		require.NoError(t, err)

		parentCtx, cancelParent := context.WithCancel(context.Background())
		balancer := newLifecycleBalancer()

		pool, err := crdbpgx.NewPool(
			parentCtx,
			cfg,
			crdbpgx.WithHealthTracker(fakeHealthTracker{}),
			crdbpgx.WithBalancer(balancer),
		)
		require.NoError(t, err)

		<-balancer.started
		cancelParent()

		select {
		case <-balancer.stopped:
			t.Fatal("balancer stopped when NewPool parent context was canceled")
		case <-time.After(25 * time.Millisecond):
		}

		close(balancer.release)
		pool.Close()

		require.Eventually(t, func() bool {
			select {
			case <-balancer.stopped:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("idempotent", func(t *testing.T) {
		t.Parallel()

		cfg, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:1/db?pool_min_conns=0&pool_min_idle_conns=0")
		require.NoError(t, err)

		balancer := newLifecycleBalancer()
		pool, err := crdbpgx.NewPool(
			context.Background(),
			cfg,
			crdbpgx.WithHealthTracker(fakeHealthTracker{}),
			crdbpgx.WithBalancer(balancer),
		)
		require.NoError(t, err)
		<-balancer.started

		close(balancer.release)
		pool.Close()
		pool.Close()
	})
}
