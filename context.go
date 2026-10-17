package crdbpgx

import (
	"context"
)

type ctxDisableRetries struct{}

// CtxDisableRetries is a context key used to disable automatic retries.
var CtxDisableRetries = ctxDisableRetries{}

// WithDisableRetries returns a context that disables automatic retry logic
// for database operations. This is useful when you need predictable behavior
// or want to handle retries manually.
func WithDisableRetries(ctx context.Context) context.Context {
	return context.WithValue(ctx, CtxDisableRetries, true)
}

// RetriesDisabled returns true if automatic retries are disabled in the context.
func RetriesDisabled(ctx context.Context) bool {
	isDisabled, ok := ctx.Value(CtxDisableRetries).(bool)
	return ok && isDisabled
}

// ContextPropagationBehaviour controls how contexts are propagated from callers
// to database operations. This affects context cancellation behavior and is
// important for preventing unwanted connection closures during operations.
type ContextPropagationBehaviour int

const (
	// ContextPropagationPassthrough propagates the caller's context directly to
	// database operations without modification. This is the standard behavior but
	// may cause connection closures if contexts are canceled during operations.
	ContextPropagationPassthrough ContextPropagationBehaviour = iota

	// ContextPropagationOnAcquire propagates the caller's context when acquiring
	// a connection from the pool, but detaches it for the actual database operation.
	// This prevents context cancellation from affecting ongoing operations while
	// still respecting cancellation during connection acquisition.
	ContextPropagationOnAcquire

	// ContextPropagationDetach removes caller cancellation and deadlines from
	// both connection acquisition and database operations. Context values,
	// including tracing information, are preserved.
	ContextPropagationDetach
)

func (p *Pool) contextForAcquire(ctx context.Context) context.Context {
	if p.ctxPropagation > ContextPropagationOnAcquire {
		return detachContext(ctx)
	}
	return ctx
}

func (p *Pool) contextForDBOp(ctx context.Context) context.Context {
	if p.ctxPropagation > ContextPropagationPassthrough {
		return detachContext(ctx)
	}
	return ctx
}

// Used to indicate wether the context is already an detached from created in this package
type ctxDetachedKey struct{}

func isDetachedContext(ctx context.Context) bool {
	return ctx.Value(ctxDetachedKey{}) != nil
}

// detachContext creates a context that ignore cancelation of the parent context.
func detachContext(ctx context.Context) context.Context {
	if isDetachedContext(ctx) {
		return ctx
	}

	return WithDisableRetries(context.WithoutCancel(ctx))
}
