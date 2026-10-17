package crdbpgx

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool_ContextPropagation(t *testing.T) {
	t.Parallel()

	type key struct{}
	newCanceledContext := func(t *testing.T) context.Context {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), key{}, "value"), time.Hour)
		cancel()
		return ctx
	}
	assertDetached := func(t *testing.T, ctx context.Context) {
		t.Helper()
		require.NoError(t, ctx.Err())
		assert.Nil(t, ctx.Done())
		_, ok := ctx.Deadline()
		assert.False(t, ok)
		assert.Equal(t, "value", ctx.Value(key{}))
		assert.True(t, RetriesDisabled(ctx))
	}

	t.Run("passthrough preserves cancellation for acquisition and database operations", func(t *testing.T) {
		t.Parallel()

		ctx := newCanceledContext(t)
		pool := &Pool{ctxPropagation: ContextPropagationPassthrough}
		assert.Same(t, ctx, pool.contextForAcquire(ctx))
		assert.Same(t, ctx, pool.contextForDBOp(ctx))
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.False(t, RetriesDisabled(ctx))
		_, ok := ctx.Deadline()
		assert.True(t, ok)
	})

	t.Run("on acquire detaches only database operations", func(t *testing.T) {
		t.Parallel()

		ctx := newCanceledContext(t)
		pool := &Pool{ctxPropagation: ContextPropagationOnAcquire}
		assert.Same(t, ctx, pool.contextForAcquire(ctx))
		assertDetached(t, pool.contextForDBOp(ctx))
	})

	t.Run("detach ignores cancellation for acquisition and database operations", func(t *testing.T) {
		t.Parallel()

		ctx := newCanceledContext(t)
		pool := &Pool{ctxPropagation: ContextPropagationDetach}
		assertDetached(t, pool.contextForAcquire(ctx))
		assertDetached(t, pool.contextForDBOp(ctx))
	})
}

func TestWithDisableRetries(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	require.False(t, RetriesDisabled(ctx))
	disabled := WithDisableRetries(ctx)
	assert.True(t, RetriesDisabled(disabled))
	assert.False(t, RetriesDisabled(ctx), "the parent context is unchanged")
}
