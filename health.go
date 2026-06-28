package crdbpgx

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"
)

const (
	defaultNodeHealthPollInterval = 5 * time.Second
	defaultNodeHealthCloseTimeout = 5 * time.Second
)

var (
	healthyCRDBNodeCountGauge metric.Int64Gauge
)

// NodeHealthTracker is responsible for tracking health of a nodes
// in a CockroachDB cluster.
type NodeHealthTracker interface {
	SetStatus(nodeID uint32, healthy bool)
	IsHealthy(nodeID uint32) bool
	NumHealthy() int
}

// nodeHealthTracker monitors the health status of CockroachDB nodes in a cluster.
//
// It periodically attempts connections to discover and track node availability,
// and maintains a registry of healthy nodes for use by connection rebalancing
// and error recovery mechanisms.
type nodeHealthTracker struct {
	mu           sync.RWMutex
	connConfig   *pgx.ConnConfig
	healthyNodes map[uint32]struct{}
	nodesSeen    map[uint32]*rate.Limiter
	newLimiter   func() *rate.Limiter
	pollInterval time.Duration
}

type nodeHealthTrackerConfig struct {
	pollInterval time.Duration
}

// NodeHealthTrackerOption is a configuration option for the [nodeHealthTracker].
type NodeHealthTrackerOption func(*nodeHealthTrackerConfig)

// WithPollInterval configures the interval at which the node health tracker
// attempts to connect to a random node in the cluster and assert its health.
func WithPollInterval(d time.Duration) NodeHealthTrackerOption {
	return func(tracker *nodeHealthTrackerConfig) {
		tracker.pollInterval = d
	}
}

// NewNodeHealthTracker creates a new [nodeHealthTracker].
//
// The tracker periodically probes for healthy nodes in the CockroachDB cluster
// and tracks their health.
func NewNodeHealthTracker(cfg *pgx.ConnConfig, opts ...NodeHealthTrackerOption) (_ *nodeHealthTracker, err error) {
	if healthyCRDBNodeCountGauge == nil {
		healthyCRDBNodeCountGauge, err = meter.Int64Gauge(
			"crdb_healthy_nodes",
			metric.WithDescription("Number of health CRDB nodes detected"),
		)
		if err != nil {
			return nil, fmt.Errorf("initialize crdb health nodes gauge: %w", err)
		}
	}

	tCfg := &nodeHealthTrackerConfig{
		pollInterval: defaultNodeHealthPollInterval,
	}
	for _, o := range opts {
		o(tCfg)
	}

	return &nodeHealthTracker{
		connConfig:   cfg,
		pollInterval: tCfg.pollInterval,
		healthyNodes: make(map[uint32]struct{}),
		nodesSeen:    make(map[uint32]*rate.Limiter),
		newLimiter: func() *rate.Limiter {
			return rate.NewLimiter(rate.Every(1*time.Minute), 2)
		},
	}, nil
}

// Poll continuously checks health of the nodes in the cluster.
// On each run, it attempts to connect to a random node in the cluster
// and tracks its health. This functionality aids with discovery of
// new nodes in the cluster when scaling up or taking nodes back
// online after upgrade.
//
// This should be run in a goroutine. It will run until the context is canceled.
func (t *nodeHealthTracker) Poll(ctx context.Context) {
	ticker := NewJitteredTicker(t.pollInterval, &UniformJitter{
		//nolint:gosec // We use unsecure rand here in an acceptable way.
		Source: rand.New(rand.NewSource(time.Now().Unix())),
		// To avoid checking all nodes at the same time with concurrent startup, we apply a jitter.
		// This should be 20% of the interval, but not longer than a second.
		Min: min(t.pollInterval/5, 1*time.Second),
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.tryConnection(ctx, t.pollInterval)
		}
	}
}

func (t *nodeHealthTracker) tryConnection(ctx context.Context, interval time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, interval)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, t.connConfig)
	if err != nil {
		return
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultNodeHealthCloseTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if err := conn.Ping(ctx); err != nil {
		return
	}

	slog.DebugContext(ctx,
		"crdbpgx: health tracker connected to node",
		slog.Uint64("node_id", uint64(nodeID(conn))),
	)

	// Connection successful --> mark node health
	t.SetStatus(nodeID(conn), true)
}

// SetStatus updates the health status of a node.
//
// If healthy is true, the node is marked as healthy. If false, the node is marked
// as unhealthy only if it has failed more than twice in the last minute (rate limited
// to prevent marking nodes as unhealthy due to transient errors).
func (t *nodeHealthTracker) SetStatus(nodeID uint32, healthy bool) {
	t.mu.Lock()
	defer func() {
		numHealthy := len(t.healthyNodes)
		t.mu.Unlock()
		healthyCRDBNodeCountGauge.Record(context.Background(), int64(numHealthy))
	}()

	nodeLimiter, nodeHasBeenSeen := t.nodesSeen[nodeID]
	if !nodeHasBeenSeen {
		nodeLimiter = t.newLimiter()
		t.nodesSeen[nodeID] = nodeLimiter
	}

	if healthy {
		t.healthyNodes[nodeID] = struct{}{}
		t.nodesSeen[nodeID] = t.newLimiter()
		return
	}

	// If the limiter allows the request, we haven't seen more than 2 failures in the past minute.
	// Keep the node health
	if nodeLimiter.Allow() {
		return
	}

	delete(t.healthyNodes, nodeID)
}

// IsHealthy returns true if the specified node is currently marked as healthy.
func (t *nodeHealthTracker) IsHealthy(nodeID uint32) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.healthyNodes[nodeID]
	return ok
}

// NumHealthy returns the current count of healthy nodes in the cluster.
func (t *nodeHealthTracker) NumHealthy() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.healthyNodes)
}
