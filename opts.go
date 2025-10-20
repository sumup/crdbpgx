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

	DefaultConnectRate = 100 * time.Millisecond

	// DefaultContextPropagationBehaviour is the default behavior for context propagation.
	DefaultContextPropagationBehaviour = ContextPropagationDetach
)

// Opt is a functional option for configuring a [Pool].
type Opt func(*poolCfg)

func defaultPoolCfg() *poolCfg {
	return &poolCfg{
		name:           "crdbpgx",
		maxRetries:     DefaultMaxRetries,
		connectRate:    DefaultConnectRate,
		ctxPropagation: DefaultContextPropagationBehaviour,
	}
}

// WithName configures the pool name used for observability (metrics and tracing).
// The default if not specified is "crdbpgx".
func WithName(name string) Opt {
	return func(pc *poolCfg) {
		pc.name = name
	}
}

// WithMaxRetries configures the maximum number of retries per database operation
// when encountering retryable errors. The default is [DefaultMaxRetries].
func WithMaxRetries(retries int) Opt {
	return func(pc *poolCfg) {
		pc.maxRetries = retries
	}
}

// WithConnectRate configures the rate limit for establishing new connections to the database.
// This helps prevent connection storms. The default is [DefaultConnectRate].
func WithConnectRate(rate time.Duration) Opt {
	return func(pc *poolCfg) {
		pc.connectRate = rate
	}
}

// WithContextPropagationBehaviour configures how contexts are propagated from callers
// to database operations. See [ContextPropagationBehaviour] for available options.
// The default is [DefaultContextPropagationBehaviour].
func WithContextPropagationBehaviour(behavior ContextPropagationBehaviour) Opt {
	return func(pc *poolCfg) {
		pc.ctxPropagation = behavior
	}
}

// WithLogger configures the logger for the [Pool].
// By default, no logger is configured and debug logging is disabled.
func WithLogger(l *slog.Logger) Opt {
	return func(pc *poolCfg) {
		pc.logger = l
	}
}
