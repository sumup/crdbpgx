package crdbpgx

import "context"

// AcquireFromDifferentNodeOutcome describes the outcome of a retry-driven
// connection reacquisition attempt.
type AcquireFromDifferentNodeOutcome string

const (
	AcquireFromDifferentNodeStarted           AcquireFromDifferentNodeOutcome = "started"
	AcquireFromDifferentNodeOnlyOneHealthy    AcquireFromDifferentNodeOutcome = "only_one_healthy"
	AcquireFromDifferentNodeDifferentAcquired AcquireFromDifferentNodeOutcome = "different_acquired"
)

// AcquireFromDifferentNodeEvent is emitted while attempting to reacquire a
// connection from a different CockroachDB node.
type AcquireFromDifferentNodeEvent struct {
	PoolName       string
	CurrentNodeID  uint32
	AcquiredNodeID uint32
	Outcome        AcquireFromDifferentNodeOutcome
}

// RetriesCompleteEvent is emitted after a retrying database operation finishes.
type RetriesCompleteEvent struct {
	PoolName string
	Retries  uint
}

// BalancerRunEvent is emitted after a connection rebalancing run completes.
type BalancerRunEvent struct {
	PoolName          string
	AcquiredIdleConns int
	PrunedConns       int
}

// NodeHealthChangeEvent is emitted after the tracked health state changes for a node.
type NodeHealthChangeEvent struct {
	NodeID       uint32
	Healthy      bool
	HealthyNodes int
}

// Hooks contains optional lifecycle callbacks that plugins can use to attach
// cross-cutting behavior such as telemetry.
type Hooks struct {
	DetachContext              func(context.Context) context.Context
	OnAcquireFromDifferentNode func(context.Context, AcquireFromDifferentNodeEvent)
	OnRetriesComplete          func(context.Context, RetriesCompleteEvent)
	OnBalancerRun              func(context.Context, BalancerRunEvent)
	OnNodeHealthChange         func(context.Context, NodeHealthChangeEvent)
}

type hookSet []Hooks

func (hs hookSet) detachContext(ctx context.Context) context.Context {
	if len(hs) == 0 {
		return context.Background()
	}

	for _, h := range hs {
		if h.DetachContext != nil {
			ctx = h.DetachContext(ctx)
		}
	}
	return ctx
}

func (hs hookSet) acquireFromDifferentNode(ctx context.Context, event AcquireFromDifferentNodeEvent) {
	for _, h := range hs {
		if h.OnAcquireFromDifferentNode != nil {
			h.OnAcquireFromDifferentNode(ctx, event)
		}
	}
}

func (hs hookSet) retriesComplete(ctx context.Context, event RetriesCompleteEvent) {
	for _, h := range hs {
		if h.OnRetriesComplete != nil {
			h.OnRetriesComplete(ctx, event)
		}
	}
}

func (hs hookSet) balancerRun(ctx context.Context, event BalancerRunEvent) {
	for _, h := range hs {
		if h.OnBalancerRun != nil {
			h.OnBalancerRun(ctx, event)
		}
	}
}

func (hs hookSet) nodeHealthChange(ctx context.Context, event NodeHealthChangeEvent) {
	for _, h := range hs {
		if h.OnNodeHealthChange != nil {
			h.OnNodeHealthChange(ctx, event)
		}
	}
}
