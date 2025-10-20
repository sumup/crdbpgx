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

var (
	healthyCRDBNodeCountGauge metric.Int64Gauge
)

// NodeHealthTracker monitors the health status of CockroachDB nodes in a cluster.
//
// It periodically attempts connections to discover and track node availability,
// and maintains a registry of healthy nodes for use by connection rebalancing
// and error recovery mechanisms.
type NodeHealthTracker struct {
	mu           sync.RWMutex
	connConfig   *pgx.ConnConfig
	healthyNodes map[uint32]struct{}
	nodesSeen    map[uint32]*rate.Limiter
	newLimiter   func() *rate.Limiter
}

// NewNodeHealthChecker creates a new [NodeHealthTracker].
//
// The tracker uses the provided DSN to periodically probe for healthy nodes
// in the CockroachDB cluster. The DSN should point to any node in the cluster;
// the tracker will discover other nodes through successful connections.
func NewNodeHealthChecker(dsn string) (_ *NodeHealthTracker, err error) {
	if healthyCRDBNodeCountGauge == nil {
		healthyCRDBNodeCountGauge, err = meter.Int64Gauge(
			"crdb_healthy_nodes",
			metric.WithDescription("Number of health CRDB nodes detected"),
		)
		if err != nil {
			return nil, fmt.Errorf("initialize crdb health nodes gauge: %w", err)
		}
	}

	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	return &NodeHealthTracker{
		connConfig:   connConfig,
		healthyNodes: make(map[uint32]struct{}),
		nodesSeen:    make(map[uint32]*rate.Limiter),
		newLimiter: func() *rate.Limiter {
			return rate.NewLimiter(rate.Every(1*time.Minute), 2)
		},
	}, nil
}

// Poll continuously checks node health at the specified interval.
//
// This should be run in a goroutine. It will run until the context is canceled.
// The interval specifies how often to attempt a connection to discover and verify
// node health.
func (t *NodeHealthTracker) Poll(ctx context.Context, interval time.Duration) {
	ticker := NewJitteredTicker(interval, &UniformJitter{
		//nolint:gosec // We use unsecure rand here in an acceptable way.
		Source: rand.New(rand.NewSource(time.Now().Unix())),
		// To avoid checking all nodes at the same time with concurrent startup, we apply a jitter.
		// This should be 20% of the interval, but not longer than a second.
		Min: min(interval/5, 1*time.Second),
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
		case <-ticker.C:
			t.tryConnection(interval)
		}
	}
}

func (t *NodeHealthTracker) tryConnection(interval time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), interval)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, t.connConfig)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	if err := conn.Ping(ctx); err != nil {
		return
	}

	slog.DebugContext(ctx,
		"crdbpgx: health tracker connected to node",
		slog.Uint64("node_id", uint64(nodeID(conn))),
	)

	// Connection successful --> mark node health
	t.SetNodeStatus(nodeID(conn), true)
}

// SetNodeStatus updates the health status of a node.
//
// If healthy is true, the node is marked as healthy. If false, the node is marked
// as unhealthy only if it has failed more than twice in the last minute (rate limited
// to prevent marking nodes as unhealthy due to transient errors).
func (t *NodeHealthTracker) SetNodeStatus(nodeID uint32, healthy bool) {
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
func (t *NodeHealthTracker) IsHealthy(nodeID uint32) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.healthyNodes[nodeID]
	return ok
}

// NumHealthyNodes returns the current count of healthy nodes in the cluster.
func (t *NodeHealthTracker) NumHealthyNodes() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.healthyNodes)
}
