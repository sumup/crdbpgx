package crdbpgx

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// defaultBalancingInterval is the default interval for how often the balancer prunes excess idle connections.
	defaultBalancingInterval = 30 * time.Second

	// defaultTopologyChangeBalancingInterval is the default for the balancing interval after a topology
	// change in the cluster. By default, this interval if faster then when the cluster is stable
	// to ensure quicker rebalancing of connections.
	defaultTopologyChangeBalancingInterval = 5 * time.Second

	// defaultTopologyChangeBalancingIntervalWindow is the default for topology change balancing interval window
	// which controls how long accelerated balancing lasts.
	defaultTopologyChangeBalancingIntervalWindow = 5 * time.Minute

	// defaultBalancingTimeout is the default for balancing timeouts which bounds a single balancing operation.
	defaultBalancingTimeout = 10 * time.Second
)

var (
	newNodeConnectionBalancerMetricsOnce = sync.OnceValue(newNodeConnectionBalancerMetrics)
)

type balancerMetrics struct {
	// acquiredConnectionsHistogram records idle connections inspected per run.
	acquiredConnectionsHistogram metric.Int64Histogram

	// prunedConnectionsHistogram records connections marked for pruning per run.
	prunedConnectionsHistogram metric.Int64Histogram
}

func newNodeConnectionBalancerMetrics() *balancerMetrics {
	acquireHist, err := meter.Int64Histogram(
		"crdbpgx.balancer.acquired",
		metric.WithUnit("{connection}"),
		metric.WithDescription("connections acquired for pruning"),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50),
	)
	if err != nil {
		panic(fmt.Errorf("initialize crdb balancer acquire histogram: %w", err))
	}

	pruneHist, err := meter.Int64Histogram(
		"crdbpgx.balancer.pruned",
		metric.WithUnit("{connection}"),
		metric.WithDescription("connections pruned per balancing run"),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50),
	)
	if err != nil {
		panic(fmt.Errorf("initialize crdb balancer prune histogram: %w", err))
	}

	return &balancerMetrics{
		acquiredConnectionsHistogram: acquireHist,
		prunedConnectionsHistogram:   pruneHist,
	}
}

// NodeBalancer balances pooled connections across CockroachDB nodes.
type NodeBalancer interface {
	// Run starts the periodic balancing loop and blocks until ctx is canceled.
	Run(ctx context.Context, pool *Pool)
	// Rebalance performs one balancing operation.
	Rebalance(ctx context.Context, pool *Pool)
}

// nodeConnectionBalancer distributes database connections across CockroachDB nodes,
// rebalancing periodically to maintain even distribution and handle node health changes.
type nodeConnectionBalancer struct {
	// mu serializes balancing operations.
	mu *sync.Mutex

	// ticker schedules periodic balancing runs.
	ticker *time.Ticker

	// balancingInterval is the regular balancing interval.
	balancingInterval time.Duration

	// topologyChangeBalanceInterval is the accelerated balancing interval used when
	// the balancer detects changes in the cluster's topology.
	topologyChangeBalanceInterval time.Duration

	// topologyChangeBalanceWindow is how long accelerated balancing remains active
	// after a topology change inside the cluster.
	topologyChangeBalanceWindow time.Duration

	// acceleratedUntil is the time when accelerated balancing ends.
	acceleratedUntil time.Time

	// balanceTimeout bounds a single balancing operation.
	balanceTimeout time.Duration

	// logger emits optional debug logs.
	logger *slog.Logger

	// metrics contains shared balancer metric instruments.
	metrics *balancerMetrics

	// rebalance performs the balancing operation.
	//
	// IMPORTANT: sole purpose of this field is for testing.
	rebalance func(context.Context, *Pool)
}

// NewNodeConnectionBalancer creates a connection balancer that distributes
// connections across CockroachDB nodes.
//
// The balancer ensures an even distribution of connections across healthy nodes,
// pruning excess connections from over-utilized nodes and marking unhealthy
// node connections for garbage collection.
//
// Pass the returned balancer to [WithBalancer] to run it with a [Pool], or call
// [NodeBalancer.Run] yourself if you need to manage its lifecycle directly.
//
// If no interval is specified via [WithBalancingInterval], the default is 30 seconds.
func NewNodeConnectionBalancer(opts ...NodeConnectionBalancerOption) NodeBalancer {
	nb := &nodeConnectionBalancer{
		mu:                            new(sync.Mutex),
		ticker:                        time.NewTicker(defaultBalancingInterval),
		balanceTimeout:                defaultBalancingTimeout,
		logger:                        slog.Default(),
		balancingInterval:             defaultBalancingInterval,
		metrics:                       newNodeConnectionBalancerMetricsOnce(),
		topologyChangeBalanceInterval: defaultTopologyChangeBalancingInterval,
		topologyChangeBalanceWindow:   defaultTopologyChangeBalancingIntervalWindow,
	}
	nb.rebalance = nb.rebalancePool

	for _, opt := range opts {
		opt(nb)
	}

	return nb
}

// NodeConnectionBalancerOption is a configuration option for the connection balancer.
type NodeConnectionBalancerOption func(*nodeConnectionBalancer)

// WithBalancingInterval sets how often to perform connection rebalancing.
// Default: 30 seconds.
func WithBalancingInterval(interval time.Duration) NodeConnectionBalancerOption {
	return func(b *nodeConnectionBalancer) {
		b.balancingInterval = interval
		b.ticker.Reset(interval)
	}
}

// WithTopologyChangeBalancingInterval configures a temporary faster balancing
// interval used after the tracker detects a topology change.
//
// The faster interval is active for window. Defaults: 5 seconds for 5 minutes.
func WithTopologyChangeBalancingInterval(interval, window time.Duration) NodeConnectionBalancerOption {
	return func(b *nodeConnectionBalancer) {
		if interval > 0 {
			b.topologyChangeBalanceInterval = interval
		}
		if window > 0 {
			b.topologyChangeBalanceWindow = window
		}
	}
}

// WithBalancingTimeout sets the timeout for an individual balancing operation.
// Default: 10 seconds.
func WithBalancingTimeout(timeout time.Duration) NodeConnectionBalancerOption {
	return func(b *nodeConnectionBalancer) {
		b.balanceTimeout = timeout
	}
}

// Run continuously rebalances connections at the configured interval.
//
// This should be run in a goroutine. It will run until the context is canceled.
// During each rebalancing cycle, it acquires all idle connections, calculates
// the optimal distribution across healthy nodes, and marks excess connections
// for garbage collection.
func (p *nodeConnectionBalancer) Run(ctx context.Context, pool *Pool) {
	defer p.ticker.Stop()

	topologyChanges, unsubscribe := topologyChangeEvents(pool.health)
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return
		case <-topologyChanges:
			p.acceleratedUntil = time.Now().Add(p.topologyChangeBalanceWindow)
			p.ticker.Reset(p.currentBalancingInterval())
			p.rebalance(ctx, pool)
		case <-p.ticker.C:
			p.rebalance(ctx, pool)
			p.ticker.Reset(p.currentBalancingInterval())
		}
	}
}

// Rebalance performs one balancing operation.
func (p *nodeConnectionBalancer) Rebalance(ctx context.Context, pool *Pool) {
	p.rebalance(ctx, pool)
}

func (p *nodeConnectionBalancer) rebalancePool(ctx context.Context, pool *Pool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, p.balanceTimeout)
	defer cancel()

	conns := pool.AcquireAllIdle(ctx)
	defer func() {
		// Release all acquired idle connections
		for _, c := range conns {
			c.Release()
		}
	}()
	p.metrics.acquiredConnectionsHistogram.Record(ctx, int64(len(conns)),
		metric.WithAttributes(
			attribute.String("pool.id", pool.Name()),
		),
	)

	nodeCount := max(pool.health.NumHealthy(), 1)

	healthyConns := make(map[uint32][]*pgxpool.Conn, nodeCount)
	for _, c := range conns {
		nid := nodeID(c.Conn())
		if pool.health.IsHealthy(nid) {
			if healthyConns[nid] == nil {
				healthyConns[nid] = make([]*pgxpool.Conn, 0, min(1, len(conns)/nodeCount))
			}
			healthyConns[nid] = append(healthyConns[nid], c)
		} else {
			pool.GCMark(c.Conn())
		}
	}

	connsPerNode := pool.connsPerNode()

	maxConns := pool.MaxConns()
	nodeMax := maxConnsPerNode(maxConns, nodeCount)

	var prunedTotal int64

	for nodeID, cnt := range connsPerNode {
		// Prune only connections above the per-node maximum. When there is
		// more than one excess connection, prune half of the excess so the
		// balancer converges gradually instead of tearing down too much at once.
		toPrune := cnt - nodeMax
		if toPrune <= 0 {
			continue
		}
		if toPrune > 1 {
			toPrune /= 2
		}
		toPrune = min(toPrune, len(healthyConns[nodeID]))
		if toPrune == 0 {
			continue
		}

		for _, c := range healthyConns[nodeID][:toPrune] {
			pool.GCMark(c.Conn())
		}

		prunedTotal += int64(toPrune)
		p.logger.DebugContext(
			ctx,
			"balancer: pruned connections",
			slog.String("library", "github.com/sumup/crdbpgx"),
			slog.String("pool", pool.Name()),
			slog.Uint64("node_id", uint64(nodeID)),
			slog.Int64("pool_max_conns", int64(pool.Config().MaxConns)),
			slog.Int64("node_conns", int64(cnt)),
			slog.Int64("node_max_conns", int64(nodeMax)),
			slog.Int("node_pruned_conns", toPrune),
		)
	}

	p.metrics.prunedConnectionsHistogram.Record(ctx, prunedTotal,
		metric.WithAttributes(
			attribute.String("pool.id", pool.Name()),
		),
	)
}

// maxConnsPerNode returns the maximum number of connections per node
// given the size of the pool and number of nodes.
//
// maxConnsPerNode rounds up to ensure we always saturate the pool.
// E.g. a pool with size 10 and a cluster with 3 nodes results in
// the maximum number of connections per node to be 4.
func maxConnsPerNode(maxConns, nodeCount int) int {
	if nodeCount <= 0 {
		return maxConns
	}

	nodeMax := maxConns / nodeCount
	if maxConns%nodeCount > 0 {
		nodeMax++
	}

	return nodeMax
}

func (p *nodeConnectionBalancer) currentBalancingInterval() time.Duration {
	if p.topologyChangeBalanceWindow > 0 && time.Now().Before(p.acceleratedUntil) {
		return p.topologyChangeBalanceInterval
	}
	return p.balancingInterval
}

type topologyChangeSubscriber interface {
	SubscribeTopologyChanges() (<-chan struct{}, func())
}

func topologyChangeEvents(health NodeHealthTracker) (<-chan struct{}, func()) {
	subscriber, ok := health.(topologyChangeSubscriber)
	if !ok {
		return nil, func() {}
	}
	return subscriber.SubscribeTopologyChanges()
}
