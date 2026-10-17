package crdbpgx

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Hook observes pool activity. Implementations should embed [HooksDefaults] by
// value and override the callbacks they need. Embedding also supplies defaults
// for callbacks added in future versions. Nil entries are ignored.
//
// Hooks run synchronously in registration order, outside internal locks, and may
// run concurrently. They must be safe for concurrent use, return promptly, and
// must not close the pool. Contexts retain caller values when cancellation is
// detached by the pool's context propagation policy.
type Hook interface {
	// QueryTracer observes individual Exec, Query, and QueryRow attempts through
	// pgx, including operations on directly acquired connections. Query start
	// contexts flow through subsequent hooks and into query end callbacks.
	pgx.QueryTracer

	// OnOperationEnd observes retry-managed operations after their connections
	// have been released, including when initial acquisition fails.
	OnOperationEnd(context.Context, OperationEndData)

	// OnNodeSwitchStart observes attempts to acquire a replacement connection.
	OnNodeSwitchStart(context.Context, NodeSwitchStartData)

	// OnNodeSwitchEnd observes replacement acquisition results, including failures.
	OnNodeSwitchEnd(context.Context, NodeSwitchEndData)

	// OnRebalanceEnd observes built-in balancing runs after idle connections have
	// been released.
	OnRebalanceEnd(context.Context, RebalanceEndData)

	// OnHealthUpdate observes each SetStatus call on an instrumented built-in
	// tracker. Concurrent updates may be delivered out of order.
	OnHealthUpdate(HealthUpdateData)
}

// HooksDefaults implements [Hook] with no-op callbacks. Embed it by value in
// custom hook implementations and override only the callbacks needed. Its zero
// value is ready to use and safe for concurrent use.
type HooksDefaults struct{}

var _ Hook = HooksDefaults{}

// TraceQueryStart implements [pgx.QueryTracer] by returning ctx unchanged.
func (HooksDefaults) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

// TraceQueryEnd implements [pgx.QueryTracer] as a no-op.
func (HooksDefaults) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// OnOperationEnd implements [Hook] as a no-op.
func (HooksDefaults) OnOperationEnd(context.Context, OperationEndData) {}

// OnNodeSwitchStart implements [Hook] as a no-op.
func (HooksDefaults) OnNodeSwitchStart(context.Context, NodeSwitchStartData) {}

// OnNodeSwitchEnd implements [Hook] as a no-op.
func (HooksDefaults) OnNodeSwitchEnd(context.Context, NodeSwitchEndData) {}

// OnRebalanceEnd implements [Hook] as a no-op.
func (HooksDefaults) OnRebalanceEnd(context.Context, RebalanceEndData) {}

// OnHealthUpdate implements [Hook] as a no-op.
func (HooksDefaults) OnHealthUpdate(HealthUpdateData) {}

type hooks []Hook

func (hs hooks) onOperationEnd(ctx context.Context, data OperationEndData) {
	for _, h := range hs {
		h.OnOperationEnd(ctx, data)
	}
}

func (hs hooks) onNodeSwitchStart(ctx context.Context, data NodeSwitchStartData) {
	for _, h := range hs {
		h.OnNodeSwitchStart(ctx, data)
	}
}

func (hs hooks) onNodeSwitchEnd(ctx context.Context, data NodeSwitchEndData) {
	for _, h := range hs {
		h.OnNodeSwitchEnd(ctx, data)
	}
}

func (hs hooks) onRebalanceEnd(ctx context.Context, data RebalanceEndData) {
	for _, h := range hs {
		h.OnRebalanceEnd(ctx, data)
	}
}

func (hs hooks) onHealthUpdate(data HealthUpdateData) {
	for _, h := range hs {
		h.OnHealthUpdate(data)
	}
}

// OperationEndData describes the result of a retry-managed operation.
// CopyFrom, SendBatch, and operations on directly acquired connections are excluded.
type OperationEndData struct {
	// PoolName is the name returned by [Pool.Name].
	PoolName string
	// Attempts counts calls to the operation, excluding connection acquisition.
	// It is zero if the initial connection acquisition failed.
	Attempts int
	// Retries counts attempts after the first.
	Retries int
	// Resets counts connections discarded following resettable errors or closure.
	Resets int
	// RetryLimitReached reports whether the operation exhausted its retry limit.
	RetryLimitReached bool
	// Err is the final operation or connection acquisition error, or nil on success.
	Err error
}

// NodeSwitchStartData describes an attempt to acquire a replacement connection.
type NodeSwitchStartData struct {
	// PoolName is the name returned by [Pool.Name].
	PoolName string
	// FromNodeID is the node being avoided, or zero if its identity is unknown.
	FromNodeID uint32
}

// NodeSwitchEndData describes the result of acquiring a replacement connection.
type NodeSwitchEndData struct {
	// PoolName is the name returned by [Pool.Name].
	PoolName string
	// FromNodeID is the node being avoided, or zero if its identity is unknown.
	FromNodeID uint32
	// ToNodeID is the replacement node, or zero if acquisition failed or its
	// identity is unknown.
	ToNodeID uint32
	// Reused reports that acquisition allowed the original node because at most
	// one healthy node was known. The replacement may still be a different node.
	Reused bool
	// Err is the connection acquisition error, or nil on success.
	Err error
}

// RebalanceEndData describes one completed built-in balancing run.
type RebalanceEndData struct {
	// PoolName is the name returned by [Pool.Name].
	PoolName string
	// Acquired counts idle connections acquired for this balancing run.
	Acquired int
	// Pruned counts healthy connections marked for pruning to balance the pool.
	// It excludes connections marked because their nodes are unhealthy.
	Pruned int
}

// HealthUpdateData is a snapshot of health after the tracker's failure threshold
// has been applied. Healthy is the effective status, not the requested status.
// Health belongs to the tracker, which may be shared by multiple pools.
type HealthUpdateData struct {
	// NodeID identifies the node whose status was updated.
	NodeID uint32
	// Healthy is the node's effective health after applying the failure threshold.
	Healthy bool
	// HealthyNodes counts healthy nodes in the tracker after this update.
	HealthyNodes int
}
