package crdbpgx

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

type ctxDisableRetries struct{}

var CtxDisableRetries = ctxDisableRetries{}

func WithDisableRetries(ctx context.Context) context.Context {
	return context.WithValue(ctx, CtxDisableRetries, true)
}

func RetriesDisabled(ctx context.Context) bool {
	isDisabled, ok := ctx.Value(CtxDisableRetries).(bool)
	return ok && isDisabled
}

type ContextPropagationBehaviour int

const (
	// ContextPropagationPassthrough propagates the context from the caller to the database as it is.
	ContextPropagationPassthrough ContextPropagationBehaviour = iota

	// ContextPropagationOnAcquire propagates the context from the caller to the database on every call to acquire a connection
	// from the pool. The context is then detached from the connection on the actual database operation.
	ContextPropagationOnAcquire

	// ContextPropagationDetach detaches the context from the caller and creates a new background context for the actual
	// database operation on every call.
	ContextPropagationDetach
)

func (p *Pool) contextForAcquire(ctx context.Context) context.Context {
	if p.ctxPropagation > ContextPropagationOnAcquire {
		ctx = detachedContextWithTelemetry(ctx)
	}
	return ctx
}

func (p *Pool) contextForDBOp(ctx context.Context) context.Context {
	if p.ctxPropagation > ContextPropagationPassthrough {
		ctx = detachedContextWithTelemetry(ctx)
	}
	return ctx
}

// Used to indicate wether the context is already an detached from created in this package
type ctxDetachedKey struct{}

func isDetachedContext(ctx context.Context) bool {
	return ctx.Value(ctxDetachedKey{}) != nil
}

// detachedContextWithTelemetry creates a new background context but copies the span from the passed parent context.
func detachedContextWithTelemetry(ctx context.Context) context.Context {
	if isDetachedContext(ctx) {
		return ctx
	}
	span := trace.SpanFromContext(ctx)

	newCtx := context.Background()
	newCtx = trace.ContextWithSpan(newCtx, span)
	newCtx = context.WithValue(newCtx, ctxDetachedKey{}, struct{}{})
	newCtx = context.WithValue(newCtx, CtxDisableRetries, ctx.Value(CtxDisableRetries))

	return newCtx
}
