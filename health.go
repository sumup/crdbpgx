package crdbpgx

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
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
)

// NodeHealthTracker tracks the health of nodes in a CockroachDB cluster.
// Implementations must be safe for concurrent use by pools and balancers.
type NodeHealthTracker interface {
	// SetStatus records a health observation. Implementations may apply a failure
	// threshold before marking a node unhealthy.
	SetStatus(nodeID uint32, healthy bool)
	// IsHealthy reports whether the node is currently considered healthy.
	IsHealthy(nodeID uint32) bool
	// NumHealthy returns the number of nodes currently considered healthy.
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

	// newLimiter creates failure limiters for node health transitions.
	newLimiter func() *rate.Limiter

	// pollInterval is the regular health probe interval.
	pollInterval time.Duration

	// hooks observes health updates.
	hooks hooks

	// topologyChangePollInterval is the accelerated probe interval.
	topologyChangePollInterval time.Duration

	// topologyChangePollWindow is how long accelerated probing remains active.
	topologyChangePollWindow time.Duration

	// topologyChangeSubscribers receive non-blocking topology change signals.
	topologyChangeSubscribers []chan struct{}
}

type nodeHealthTrackerConfig struct {
	hooks hooks
	// pollInterval configures regular health probes.
	pollInterval time.Duration
	// topologyChangePollInterval configures accelerated health probes.
	topologyChangePollInterval time.Duration
	// topologyChangePollWindow configures the accelerated probing duration.
	topologyChangePollWindow time.Duration
}

// NodeHealthTrackerOption configures a tracker created by [NewNodeHealthTracker].
type NodeHealthTrackerOption func(*nodeHealthTrackerConfig)

// WithHealthTrackerHooks configures instrumentation on a caller-owned health
// tracker, including one shared by multiple pools. Hooks are appended in
// registration order. Nil entries are ignored. Pool hooks do not replace them.
func WithHealthTrackerHooks(hooks ...Hook) NodeHealthTrackerOption {
	return func(cfg *nodeHealthTrackerConfig) {
		for _, hook := range hooks {
			if hook != nil {
				cfg.hooks = append(cfg.hooks, hook)
			}
		}
	}
}

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

// NewNodeHealthTracker creates a node health tracker. Call Poll in a goroutine
// to discover healthy nodes until its context is canceled. When sharing the
// tracker through [WithHealthTracker], the caller owns its polling lifecycle.
func NewNodeHealthTracker(cfg *pgx.ConnConfig, opts ...NodeHealthTrackerOption) (_ *nodeHealthTracker, err error) {
	tCfg := &nodeHealthTrackerConfig{
		pollInterval:               defaultNodeHealthPollInterval,
		topologyChangePollInterval: defaultTopologyChangeNodeHealthPollInterval,
		topologyChangePollWindow:   defaultTopologyChangeNodeHealthPollIntervalWindow,
	}
	for _, o := range opts {
		o(tCfg)
	}

	return &nodeHealthTracker{
		connConfig:                 cfg,
		pollInterval:               tCfg.pollInterval,
		topologyChangePollInterval: tCfg.topologyChangePollInterval,
		topologyChangePollWindow:   tCfg.topologyChangePollWindow,
		healthyNodes:               make(map[uint32]struct{}),
		nodesSeen:                  make(map[uint32]*rate.Limiter),
		hooks:                      tCfg.hooks,
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
// A healthy observation marks the node healthy and resets its failure allowance.
// The tracker tolerates two failures, with the allowance replenished at one
// failure per minute. Further failures mark the node unhealthy until it recovers.
func (t *nodeHealthTracker) SetStatus(nodeID uint32, healthy bool) {
	t.mu.Lock()

	nodeLimiter, nodeHasBeenSeen := t.nodesSeen[nodeID]
	if !nodeHasBeenSeen {
		nodeLimiter = t.newLimiter()
		t.nodesSeen[nodeID] = nodeLimiter
	}

	if healthy {
		_, wasHealthy := t.healthyNodes[nodeID]
		hadHealthyNodes := len(t.healthyNodes) > 0
		t.healthyNodes[nodeID] = struct{}{}
		t.nodesSeen[nodeID] = t.newLimiter()
		numHealthy := len(t.healthyNodes)
		t.mu.Unlock()
		t.recordHealthUpdate(nodeID, numHealthy, true)
		if !wasHealthy && hadHealthyNodes {
			t.notifyTopologyChange()
		}
		return
	}

	// If the limiter allows the request, we haven't seen more than 2 failures in the past minute.
	// Keep the node health
	if nodeLimiter.Allow() {
		_, isHealthy := t.healthyNodes[nodeID]
		numHealthy := len(t.healthyNodes)
		t.mu.Unlock()
		t.recordHealthUpdate(nodeID, numHealthy, isHealthy)
		return
	}

	_, wasHealthy := t.healthyNodes[nodeID]
	delete(t.healthyNodes, nodeID)
	numHealthy := len(t.healthyNodes)
	t.mu.Unlock()

	t.recordHealthUpdate(nodeID, numHealthy, false)
	if wasHealthy {
		t.notifyTopologyChange()
	}
}

func (t *nodeHealthTracker) recordHealthUpdate(nodeID uint32, count int, healthy bool) {
	t.hooks.onHealthUpdate(HealthUpdateData{
		NodeID:       nodeID,
		Healthy:      healthy,
		HealthyNodes: count,
	})
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

// SubscribeTopologyChanges returns a channel signaled when a known healthy node
// becomes unhealthy or another healthy node joins an already discovered cluster.
// Signals may be coalesced. The caller must call the returned unsubscribe function
// when done; it is safe to call repeatedly and does not close the channel.
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
