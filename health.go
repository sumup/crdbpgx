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
	// defaultNodeHealthPollInterval is how often the tracker probes for node health.
	defaultNodeHealthPollInterval = 5 * time.Second

	// defaultTopologyChangeNodeHealthPollInterval is the faster probe interval used after topology changes.
	defaultTopologyChangeNodeHealthPollInterval = 5 * time.Second

	// defaultTopologyChangeNodeHealthPollIntervalWindow controls how long accelerated health probing lasts.
	defaultTopologyChangeNodeHealthPollIntervalWindow = 5 * time.Minute

	// defaultNodeHealthCloseTimeout bounds cleanup of probe connections.
	defaultNodeHealthCloseTimeout = 5 * time.Second

	// defaultNodeStaleTimeout is how long an observed node without live pooled
	// connections remains part of the healthy topology.
	defaultNodeStaleTimeout = 10 * time.Minute
)

var (
	newNodeHealthTrackerMetricsOnce = sync.OnceValue(newNodeHealthTrackerMetrics)
)

type healthTrackerMetrics struct {
	// healthyNodesGauge records the current healthy node count.
	healthyNodesGauge metric.Int64Gauge
}

func newNodeHealthTrackerMetrics() *healthTrackerMetrics {
	healthyCRDBNodeCountGauge, err := meter.Int64Gauge(
		"crdb_healthy_nodes",
		metric.WithDescription("Number of healthy CRDB nodes detected"),
	)
	if err != nil {
		panic(fmt.Errorf("initialize crdb health nodes gauge: %w", err))
	}

	return &healthTrackerMetrics{
		healthyNodesGauge: healthyCRDBNodeCountGauge,
	}
}

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
	// mu protects health state and topology subscribers.
	mu sync.RWMutex

	// connConfig is used when probing CockroachDB nodes.
	connConfig *pgx.ConnConfig

	// healthyNodes contains node IDs currently considered healthy.
	healthyNodes map[uint32]struct{}

	// nodesSeen tracks recent failures per known node.
	nodesSeen map[uint32]*rate.Limiter

	// lastSeen tracks the last successful connection or health probe per node.
	lastSeen map[uint32]time.Time

	// liveConnections tracks pooled connections per node across pools sharing
	// this health tracker.
	liveConnections map[uint32]int

	// newLimiter creates failure limiters for node health transitions.
	newLimiter func() *rate.Limiter

	// pollInterval is the regular health probe interval.
	pollInterval time.Duration

	// metrics contains shared health tracker metric instruments.
	metrics *healthTrackerMetrics

	// topologyChangePollInterval is the accelerated probe interval.
	topologyChangePollInterval time.Duration

	// topologyChangePollWindow is how long accelerated probing remains active.
	topologyChangePollWindow time.Duration

	// nodeStaleTimeout bounds how long nodes without live pooled connections
	// remain healthy without another successful observation.
	nodeStaleTimeout time.Duration

	// topologyChangeSubscribers receive non-blocking topology change signals.
	topologyChangeSubscribers []chan struct{}
}

type nodeHealthTrackerConfig struct {
	// pollInterval configures regular health probes.
	pollInterval time.Duration
	// topologyChangePollInterval configures accelerated health probes.
	topologyChangePollInterval time.Duration
	// topologyChangePollWindow configures the accelerated probing duration.
	topologyChangePollWindow time.Duration
	// nodeStaleTimeout configures expiry of unobserved nodes without live connections.
	nodeStaleTimeout time.Duration
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

// WithTopologyChangePollInterval configures a temporary faster polling interval
// used after the tracker detects a topology change.
//
// The faster interval is active for window. Defaults: 5 seconds for 5 minutes.
func WithTopologyChangePollInterval(interval, window time.Duration) NodeHealthTrackerOption {
	return func(tracker *nodeHealthTrackerConfig) {
		if interval > 0 {
			tracker.topologyChangePollInterval = interval
		}
		if window > 0 {
			tracker.topologyChangePollWindow = window
		}
	}
}

// WithNodeStaleTimeout configures how long an observed node without live pooled
// connections remains healthy without another successful health probe.
// A zero timeout disables stale-node expiry. The default is 10 minutes.
func WithNodeStaleTimeout(timeout time.Duration) NodeHealthTrackerOption {
	return func(tracker *nodeHealthTrackerConfig) {
		if timeout >= 0 {
			tracker.nodeStaleTimeout = timeout
		}
	}
}

// NewNodeHealthTracker creates a new [nodeHealthTracker].
//
// The tracker periodically probes for healthy nodes in the CockroachDB cluster
// and tracks their health.
func NewNodeHealthTracker(cfg *pgx.ConnConfig, opts ...NodeHealthTrackerOption) (_ *nodeHealthTracker, err error) {
	tCfg := &nodeHealthTrackerConfig{
		pollInterval:               defaultNodeHealthPollInterval,
		topologyChangePollInterval: defaultTopologyChangeNodeHealthPollInterval,
		topologyChangePollWindow:   defaultTopologyChangeNodeHealthPollIntervalWindow,
		nodeStaleTimeout:           defaultNodeStaleTimeout,
	}
	for _, o := range opts {
		o(tCfg)
	}

	return &nodeHealthTracker{
		connConfig:                 cfg,
		pollInterval:               tCfg.pollInterval,
		topologyChangePollInterval: tCfg.topologyChangePollInterval,
		topologyChangePollWindow:   tCfg.topologyChangePollWindow,
		nodeStaleTimeout:           tCfg.nodeStaleTimeout,
		healthyNodes:               make(map[uint32]struct{}),
		nodesSeen:                  make(map[uint32]*rate.Limiter),
		lastSeen:                   make(map[uint32]time.Time),
		liveConnections:            make(map[uint32]int),
		metrics:                    newNodeHealthTrackerMetricsOnce(),
		topologyChangeSubscribers:  make([]chan struct{}, 0),
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
	topologyChanges, unsubscribe := t.SubscribeTopologyChanges()
	defer unsubscribe()

	jitter := UniformJitter{
		//nolint:gosec // We use unsecure rand here in an acceptable way.
		Source: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	var acceleratedUntil time.Time

	for {
		interval := t.currentPollInterval(time.Now(), acceleratedUntil)
		timer := time.NewTimer(jitteredPollInterval(interval, jitter))

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-topologyChanges:
			timer.Stop()
			acceleratedUntil = time.Now().Add(t.topologyChangePollWindow)
		case <-timer.C:
			t.tryConnection(ctx, interval)
			t.expireStaleNodes(time.Now())
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
	if healthy {
		t.setHealthy(nodeID, false)
		return
	}

	t.mu.Lock()
	if _, isHealthy := t.healthyNodes[nodeID]; !isHealthy {
		t.mu.Unlock()
		return
	}

	nodeLimiter := t.nodesSeen[nodeID]
	if nodeLimiter == nil {
		nodeLimiter = t.newLimiter()
		t.nodesSeen[nodeID] = nodeLimiter
	}

	// If the limiter allows the request, we haven't seen more than 2 failures in the past minute.
	// Keep the node health
	if nodeLimiter.Allow() {
		numHealthy := len(t.healthyNodes)
		t.mu.Unlock()
		t.metrics.healthyNodesGauge.Record(context.Background(), int64(numHealthy))
		return
	}

	_, wasHealthy := t.healthyNodes[nodeID]
	delete(t.healthyNodes, nodeID)
	delete(t.nodesSeen, nodeID)
	delete(t.lastSeen, nodeID)
	numHealthy := len(t.healthyNodes)
	t.mu.Unlock()

	t.metrics.healthyNodesGauge.Record(context.Background(), int64(numHealthy))
	if wasHealthy {
		t.notifyTopologyChange()
	}
}

func (t *nodeHealthTracker) connectionOpened(nodeID uint32) {
	t.setHealthy(nodeID, true)
}

func (t *nodeHealthTracker) setHealthy(nodeID uint32, connectionOpened bool) {
	t.mu.Lock()
	_, wasHealthy := t.healthyNodes[nodeID]
	hadHealthyNodes := len(t.healthyNodes) > 0
	t.healthyNodes[nodeID] = struct{}{}
	t.nodesSeen[nodeID] = t.newLimiter()
	t.lastSeen[nodeID] = time.Now()
	if connectionOpened {
		t.liveConnections[nodeID]++
	}
	numHealthy := len(t.healthyNodes)
	t.mu.Unlock()

	t.metrics.healthyNodesGauge.Record(context.Background(), int64(numHealthy))
	if !wasHealthy && hadHealthyNodes {
		t.notifyTopologyChange()
	}
}

func (t *nodeHealthTracker) connectionClosed(nodeID uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.liveConnections[nodeID] <= 1 {
		delete(t.liveConnections, nodeID)
		return
	}
	t.liveConnections[nodeID]--
}

func (t *nodeHealthTracker) expireStaleNodes(now time.Time) {
	if t.nodeStaleTimeout <= 0 {
		return
	}

	t.mu.Lock()
	topologyChanged := false
	for nodeID := range t.healthyNodes {
		if t.liveConnections[nodeID] > 0 {
			continue
		}

		lastSeen, ok := t.lastSeen[nodeID]
		if ok && now.Sub(lastSeen) < t.nodeStaleTimeout {
			continue
		}

		delete(t.healthyNodes, nodeID)
		delete(t.nodesSeen, nodeID)
		delete(t.lastSeen, nodeID)
		topologyChanged = true
	}
	numHealthy := len(t.healthyNodes)
	t.mu.Unlock()

	if !topologyChanged {
		return
	}

	t.metrics.healthyNodesGauge.Record(context.Background(), int64(numHealthy))
	t.notifyTopologyChange()
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

func (t *nodeHealthTracker) SubscribeTopologyChanges() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)

	t.mu.Lock()
	t.topologyChangeSubscribers = append(t.topologyChangeSubscribers, ch)
	t.mu.Unlock()

	var unsubscribeOnce sync.Once
	unsubscribe := func() {
		unsubscribeOnce.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()

			for i, subscriber := range t.topologyChangeSubscribers {
				if subscriber != ch {
					continue
				}

				copy(t.topologyChangeSubscribers[i:], t.topologyChangeSubscribers[i+1:])
				t.topologyChangeSubscribers[len(t.topologyChangeSubscribers)-1] = nil
				t.topologyChangeSubscribers = t.topologyChangeSubscribers[:len(t.topologyChangeSubscribers)-1]
				return
			}
		})
	}

	return ch, unsubscribe
}

func (t *nodeHealthTracker) notifyTopologyChange() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, ch := range t.topologyChangeSubscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (t *nodeHealthTracker) currentPollInterval(now, acceleratedUntil time.Time) time.Duration {
	if t.topologyChangePollWindow > 0 && now.Before(acceleratedUntil) {
		return t.topologyChangePollInterval
	}
	return t.pollInterval
}

func jitteredPollInterval(interval time.Duration, jitter UniformJitter) time.Duration {
	// To avoid checking all nodes at the same time with concurrent startup, we apply a jitter.
	// This should be 20% of the interval, but not longer than a second.
	minJitter := min(interval/5, time.Second)
	if minJitter <= 0 {
		minJitter = time.Nanosecond
	}
	if interval <= minJitter {
		return interval
	}

	jitter.Min = minJitter
	return jitter.Jitter(interval)
}
