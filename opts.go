package crdbpgx

import (
	"log/slog"
	"time"
)

const (
	// DefaultMaxRetries gives the number of transaction retries for a single transaction.
	// This is the same value as its used in the official cockroach-go library.
	// https://github.com/cockroachdb/cockroach-go/blob/master/crdb/tx.go#L114
	DefaultMaxRetries = 50

	// DefaultConnectRate limits how often the pool opens new database connections.
	DefaultConnectRate = 100 * time.Millisecond

	// DefaultContextPropagationBehaviour is the default behavior for context propagation.
	DefaultContextPropagationBehaviour = ContextPropagationDetach
)

type poolCfg struct {
	// name identifies the pool in logs and metrics.
	name string

	// maxRetries is the retry limit for database operations.
	maxRetries int

	// connectRate limits new connection creation.
	connectRate time.Duration

	// ctxPropagation controls how caller cancellation reaches database work.
	ctxPropagation ContextPropagationBehaviour

	// logger emits optional debug logs.
	logger *slog.Logger

	// healthTracker overrides the default node health tracker.
	healthTracker NodeHealthTracker

	// balancer overrides the default node connection balancer.
	balancer NodeBalancer

	hooks hooks
}

func defaultPoolCfg() *poolCfg {
	return &poolCfg{
		name:           "crdbpgx",
		maxRetries:     DefaultMaxRetries,
		connectRate:    DefaultConnectRate,
		ctxPropagation: DefaultContextPropagationBehaviour,
	}
}

// Option is a configuration option for a [Pool].
type Option func(*poolCfg)

// WithHooks configures optional instrumentation for the pool, its built-in
// balancer, and its automatically created health tracker. Hooks are appended in
// registration order, including across multiple WithHooks calls. Nil entries are
// ignored. A custom health tracker keeps its own instrumentation; configure a
// shared built-in tracker with [WithHealthTrackerHooks].
// Query callbacks run in registration order, followed by any existing pgx tracer.
func WithHooks(hooks ...Hook) Option {
	return func(pc *poolCfg) {
		for _, hook := range hooks {
			if hook != nil {
				pc.hooks = append(pc.hooks, hook)
			}
		}
	}
}

// WithName configures the pool name used for observability (metrics and tracing).
// The default if not specified is "crdbpgx".
func WithName(name string) Option {
	return func(pc *poolCfg) {
		pc.name = name
	}
}

// WithMaxRetries configures the maximum number of retries per database operation
// when encountering retryable errors. The default is [DefaultMaxRetries].
func WithMaxRetries(retries int) Option {
	return func(pc *poolCfg) {
		pc.maxRetries = retries
	}
}

// WithConnectRate configures the rate limit for establishing new connections to the database.
// This helps prevent connection storms. The default is [DefaultConnectRate].
func WithConnectRate(rate time.Duration) Option {
	return func(pc *poolCfg) {
		pc.connectRate = rate
	}
}

// WithContextPropagationBehaviour configures how contexts are propagated from callers
// to database operations. See [ContextPropagationBehaviour] for available options.
// The default is [DefaultContextPropagationBehaviour].
func WithContextPropagationBehaviour(behavior ContextPropagationBehaviour) Option {
	return func(pc *poolCfg) {
		pc.ctxPropagation = behavior
	}
}

// WithLogger configures the logger for the [Pool].
// A nil logger uses [slog.Default], which is also the default.
func WithLogger(l *slog.Logger) Option {
	return func(pc *poolCfg) {
		pc.logger = l
	}
}

// WithHealthTracker configures the health tracker used by the [Pool].
//
// You might want to initialize and pass the health tracker to the pool
// yourself if you use multiple pools for the same database inside one
// application in which case the pools can share the health tracker.
//
// If no custom health tracker is provided a default per-pool one is used.
func WithHealthTracker(h NodeHealthTracker) Option {
	return func(pc *poolCfg) {
		pc.healthTracker = h
	}
}

// WithBalancer configures the node connection balancer used by the [Pool].
//
// The pool starts the balancer after the underlying pgx pool is created
// and stops it when [Pool.Close] is called. If no balancer is configured,
// connection rebalancing is disabled.
func WithBalancer(b NodeBalancer) Option {
	return func(pc *poolCfg) {
		pc.balancer = b
	}
}
