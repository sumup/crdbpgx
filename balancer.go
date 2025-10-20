package crdbpgx

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	defaultBalancingTimeout = 10 * time.Second
)

type balancerMetrics struct {
	acquireHist metric.Int64Histogram
	pruneHist   metric.Int64Histogram
}

func newBalancerMetrics() (b *balancerMetrics, err error) {
	b = &balancerMetrics{}

	b.acquireHist, err = meter.Int64Histogram(
		"crdbpgx.balancer.acquired",
		metric.WithUnit("{connection}"),
		metric.WithDescription("connections acquired for pruning"),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize crdb balancer acquire histogram: %w", err)
	}

	b.pruneHist, err = meter.Int64Histogram(
		"crdbpgx.balancer.pruned",
		metric.WithUnit("{connection}"),
		metric.WithDescription("connections pruned per balancing run"),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize crdb balancer prune histogram: %w", err)
	}

	return b, nil
}

// nodeConnectionBalancer distributes database connections across CockroachDB nodes,
// rebalancing periodically to maintain even distribution and handle node health changes.
type nodeConnectionBalancer struct {
	ticker         *time.Ticker
	mu             *sync.Mutex
	pool           *Pool
	healthTracker  *NodeHealthTracker
	balanceTimeout time.Duration
	rnd            *rand.Rand
}

// NewNodeConnectionBalancer creates a connection balancer that rebalances connections
// across CockroachDB nodes at the specified interval.
//
// The balancer ensures an even distribution of connections across healthy nodes,
// pruning excess connections from over-utilized nodes and marking unhealthy
// node connections for garbage collection.
//
// Parameters:
//   - pool: The connection pool to balance
//   - healthTracker: Node health tracker for determining node availability
//   - interval: How often to perform rebalancing
//   - opts: Optional configuration (see [NodeConnectionBalancerOpt])
func NewNodeConnectionBalancer(
	pool *Pool,
	healthTracker *NodeHealthTracker,
	interval time.Duration,
	opts ...NodeConnectionBalancerOpt,
) *nodeConnectionBalancer {
	nb := &nodeConnectionBalancer{
		ticker:         time.NewTicker(interval),
		mu:             &sync.Mutex{},
		pool:           pool,
		healthTracker:  healthTracker,
		balanceTimeout: defaultBalancingTimeout,
		//nolint:gosec // weak random is fine here
		rnd: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}

	for _, opt := range opts {
		opt(nb)
	}

	return nb
}

// NodeConnectionBalancerOpt is a functional option for configuring a connection balancer.
type NodeConnectionBalancerOpt func(*nodeConnectionBalancer)

// WithBalancingTimeout sets the timeout for an individual balancing operation.
// Default: 10 seconds.
func WithBalancingTimeout(timeout time.Duration) NodeConnectionBalancerOpt {
	return func(b *nodeConnectionBalancer) {
		b.balanceTimeout = timeout
	}
}

// Rebalance continuously rebalances connections at the configured interval.
//
// This should be run in a goroutine. It will run until the context is canceled.
// During each rebalancing cycle, it acquires all idle connections, calculates
// the optimal distribution across healthy nodes, and marks excess connections
// for garbage collection.
func (p *nodeConnectionBalancer) Rebalance(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.ticker.Stop()
		case <-p.ticker.C:
			if p.mu.TryLock() {
				ctx, cancel := context.WithTimeout(ctx, p.balanceTimeout)
				p.rebalance(ctx)
				cancel()
				p.mu.Unlock()
			}
		}
	}
}

func (p *nodeConnectionBalancer) rebalance(ctx context.Context) {
	conns := p.pool.AcquireAllIdle(ctx)
	defer func() {
		// Release all acquired idle connections
		for _, c := range conns {
			c.Release()
		}
	}()
	balancerMeter.acquireHist.Record(ctx, int64(len(conns)),
		metric.WithAttributes(
			attribute.String("pool.id", p.pool.Name()),
		),
	)

	nodeCount := max(p.healthTracker.NumHealthyNodes(), 1)

	healthyConns := make(map[uint32][]*pgxpool.Conn, nodeCount)
	for _, c := range conns {
		nid := nodeID(c.Conn())
		if p.healthTracker.IsHealthy(nid) {
			if healthyConns[nid] == nil {
				healthyConns[nid] = make([]*pgxpool.Conn, 0, min(1, len(conns)/nodeCount))
			}
			healthyConns[nid] = append(healthyConns[nid], c)
		} else {
			p.pool.GCMark(c.Conn())
		}
	}

	// Shuffle node pruning order
	connsPerNode := p.pool.connsPerNode()
	nodes := slices.Collect(maps.Keys(connsPerNode))
	p.rnd.Shuffle(len(nodes), func(i, j int) {
		nodes[j], nodes[i] = nodes[i], nodes[j]
	})

	maxConns := p.pool.MaxConns()
	perNodeMax := maxConns / nodeCount

	var prunedTotal int64

	for i, nodeID := range nodes {
		cnt := connsPerNode[nodeID]
		nodeMax := perNodeMax

		if p.healthTracker.NumHealthyNodes() == 0 || i < maxConns%p.healthTracker.NumHealthyNodes() {
			perNodeMax++
		}

		toPrune := cnt - nodeMax
		if toPrune <= 0 {
			continue
		}

		// Only prune half of the connections to avoid excessive connection teardown
		toPrune = min(toPrune>>1, len(healthyConns[nodeID]))
		if toPrune == 0 {
			continue
		}

		for _, c := range healthyConns[nodeID][:toPrune] {
			p.pool.GCMark(c.Conn())
		}

		prunedTotal += int64(toPrune)
		slog.DebugContext(
			ctx,
			"balancer: pruned connections",
			slog.String("library", "github.com/sumup/crdbpgx"),
			slog.String("pool", p.pool.Name()),
			slog.Uint64("node_id", uint64(nodeID)),
			slog.Int64("pool_max_conns", int64(p.pool.Config().MaxConns)),
			slog.Int64("node_conns", int64(cnt)),
			slog.Int64("node_max_conns", int64(nodeMax)),
			slog.Int("node_pruned_conns", toPrune),
		)
	}

	balancerMeter.pruneHist.Record(ctx, prunedTotal,
		metric.WithAttributes(
			attribute.String("pool.id", p.pool.Name()),
		),
	)
}
