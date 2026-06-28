package crdbpgx

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestNodeConnectionBalancer_AcceleratesAfterNodeLeaves(t *testing.T) {
	t.Parallel()

	t.Run("topology changes", func(t *testing.T) {
		t.Parallel()

		t.Run("node left", func(t *testing.T) {
			t.Parallel()

			tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{})
			require.NoError(t, err)

			balancer := NewNodeConnectionBalancer(
				WithBalancingInterval(time.Hour),
				WithTopologyChangeBalancingInterval(10*time.Millisecond, 75*time.Millisecond),
			).(*nodeConnectionBalancer)

			rebalanced := make(chan struct{}, 10)
			balancer.rebalance = func(context.Context, *Pool) {
				select {
				case rebalanced <- struct{}{}:
				default:
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan struct{})
			go func() {
				defer close(done)
				balancer.Run(ctx, &Pool{health: tracker})
			}()

			select {
			case <-rebalanced:
				t.Fatal("unexpected rebalance before topology change")
			case <-time.After(25 * time.Millisecond):
			}

			tracker.SetStatus(1, true)
			tracker.SetStatus(1, false)
			tracker.SetStatus(1, false)
			tracker.SetStatus(1, false)

			require.Eventually(t, func() bool {
				return len(rebalanced) >= 3
			}, time.Second, 10*time.Millisecond)

			cancel()
			require.Eventually(t, func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, time.Second, 10*time.Millisecond)
		})

		t.Run("node joined", func(t *testing.T) {
			t.Parallel()

			tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{})
			require.NoError(t, err)

			balancer := NewNodeConnectionBalancer(
				WithBalancingInterval(time.Hour),
				WithTopologyChangeBalancingInterval(10*time.Millisecond, 75*time.Millisecond),
			).(*nodeConnectionBalancer)

			rebalanced := make(chan struct{}, 10)
			balancer.rebalance = func(context.Context, *Pool) {
				select {
				case rebalanced <- struct{}{}:
				default:
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan struct{})
			go func() {
				defer close(done)
				balancer.Run(ctx, &Pool{health: tracker})
			}()

			tracker.SetStatus(1, true)
			select {
			case <-rebalanced:
				t.Fatal("unexpected rebalance before topology change")
			case <-time.After(25 * time.Millisecond):
			}

			tracker.SetStatus(2, true)

			require.Eventually(t, func() bool {
				return len(rebalanced) >= 3
			}, time.Second, 10*time.Millisecond)

			cancel()
			require.Eventually(t, func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, time.Second, 10*time.Millisecond)
		})
	})
}
