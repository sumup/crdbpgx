package crdbpgx

import "context"

// Operation identifies a retry-enabled Pool operation.
type Operation string

const (
	OperationTransaction Operation = "transaction"
	OperationExec        Operation = "exec"
	OperationQuery       Operation = "query"
	OperationQueryRow    Operation = "query_row"
)

// RetryReason identifies why crdbpgx is retrying an operation.
type RetryReason string

const (
	RetryReasonResettable RetryReason = "resettable"
	RetryReasonRetryable  RetryReason = "retryable"
)

// TraceOperationStartData describes a retry-enabled Pool operation.
type TraceOperationStartData struct {
	PoolName  string
	Operation Operation
}

// TraceOperationEndData describes the result of a retry-enabled Pool operation.
type TraceOperationEndData struct {
	Err       error
	Retries   uint
	Resets    uint
	Exhausted bool
}

// TraceRetryData describes a retryable or resettable failure.
type TraceRetryData struct {
	PoolName string
	Reason   RetryReason
	Attempt  uint
	NodeID   uint32
	Err      error
}

// TraceAcquireFromDifferentNodeStartData describes a node-reacquisition attempt.
type TraceAcquireFromDifferentNodeStartData struct {
	PoolName      string
	CurrentNodeID uint32
}

// TraceAcquireFromDifferentNodeEndData describes a completed node-reacquisition attempt.
type TraceAcquireFromDifferentNodeEndData struct {
	NodeID         uint32
	ReusedSameNode bool
	Err            error
}

// TraceNodeHealthChangeData describes an observed CockroachDB node health transition.
type TraceNodeHealthChangeData struct {
	NodeID       uint32
	Healthy      bool
	HealthyNodes int
}

// TraceBalancerData describes a completed connection balancing run.
type TraceBalancerData struct {
	PoolName            string
	HealthyNodes        int
	AcquiredConnections int
	PrunedConnections   int
}

// Tracer observes CockroachDB-specific Pool, health tracker, and balancer lifecycle events.
// Implementations must be safe for concurrent use.
//
// Query-level tracing remains compatible with pgx's native tracer interfaces. A tracer that
// also implements [github.com/jackc/pgx/v5.QueryTracer] is installed on the pgx config by
// [NewPool] when the config does not already have a tracer.
type Tracer interface {
	TraceOperationStart(context.Context, TraceOperationStartData) context.Context
	TraceOperationEnd(context.Context, TraceOperationEndData)
	TraceRetry(context.Context, TraceRetryData)
	TraceAcquireFromDifferentNodeStart(context.Context, TraceAcquireFromDifferentNodeStartData) context.Context
	TraceAcquireFromDifferentNodeEnd(context.Context, TraceAcquireFromDifferentNodeEndData)
	TraceNodeHealthChange(context.Context, TraceNodeHealthChangeData)
	TraceBalancer(context.Context, TraceBalancerData)
}

// NopTracer is a Tracer that performs no work. It can be embedded by implementations that
// only need to observe a subset of lifecycle events.
type NopTracer struct{}

func (NopTracer) TraceOperationStart(ctx context.Context, _ TraceOperationStartData) context.Context {
	return ctx
}

func (NopTracer) TraceOperationEnd(context.Context, TraceOperationEndData) {}
func (NopTracer) TraceRetry(context.Context, TraceRetryData)               {}

func (NopTracer) TraceAcquireFromDifferentNodeStart(
	ctx context.Context,
	_ TraceAcquireFromDifferentNodeStartData,
) context.Context {
	return ctx
}

func (NopTracer) TraceAcquireFromDifferentNodeEnd(context.Context, TraceAcquireFromDifferentNodeEndData) {
}

func (NopTracer) TraceNodeHealthChange(context.Context, TraceNodeHealthChangeData) {}
func (NopTracer) TraceBalancer(context.Context, TraceBalancerData)                 {}
