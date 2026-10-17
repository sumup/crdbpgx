package crdbpgx_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sumup/crdbpgx"
)

type fakeHealthTracker struct{}

func (fakeHealthTracker) SetStatus(uint32, bool) {}
func (fakeHealthTracker) IsHealthy(uint32) bool  { return true }
func (fakeHealthTracker) NumHealthy() int        { return 1 }

type lifecycleBalancer struct {
	started  chan struct{}
	canceled chan struct{}
	stopped  chan struct{}
	release  chan struct{}
}

func (b *lifecycleBalancer) Run(ctx context.Context, _ *crdbpgx.Pool) {
	close(b.started)
	<-ctx.Done()
	close(b.canceled)
	<-b.release
	close(b.stopped)
}

func (b *lifecycleBalancer) Rebalance(context.Context, *crdbpgx.Pool) {}

func TestPool_Close(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		cfg, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:1/db?pool_min_conns=0&pool_min_idle_conns=0")
		require.NoError(t, err)

		parentCtx, cancelParent := context.WithCancel(t.Context())
		defer cancelParent()
		balancer := &lifecycleBalancer{
			started:  make(chan struct{}),
			canceled: make(chan struct{}),
			stopped:  make(chan struct{}),
			release:  make(chan struct{}),
		}
		release := sync.OnceFunc(func() { close(balancer.release) })
		pool, err := crdbpgx.NewPool(parentCtx, cfg,
			crdbpgx.WithHealthTracker(fakeHealthTracker{}),
			crdbpgx.WithBalancer(balancer),
		)
		require.NoError(t, err)
		t.Cleanup(pool.Close)
		t.Cleanup(release)

		synctest.Wait()
		select {
		case <-balancer.started:
		default:
			t.Fatal("balancer did not start")
		}
		cancelParent()
		synctest.Wait()
		select {
		case <-balancer.canceled:
			t.Fatal("balancer inherited the initialization context cancellation")
		default:
		}

		closed := make(chan struct{})
		go func() {
			pool.Close()
			close(closed)
		}()
		synctest.Wait()
		select {
		case <-balancer.canceled:
		default:
			t.Fatal("Close did not cancel the balancer")
		}
		select {
		case <-closed:
			t.Fatal("Close returned before the balancer stopped")
		default:
		}

		release()
		synctest.Wait()
		select {
		case <-closed:
		default:
			t.Fatal("Close did not return after the balancer stopped")
		}
		select {
		case <-balancer.stopped:
		default:
			t.Fatal("balancer did not stop")
		}
		assert.NotPanics(t, pool.Close, "Close is idempotent")
	})
}
