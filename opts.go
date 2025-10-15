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

type Opt func(*poolCfg)

func defaultPoolCfg() *poolCfg {
	return &poolCfg{
		name:           "crdbpgx",
		maxRetries:     DefaultMaxRetries,
		connectRate:    DefaultConnectRate,
		ctxPropagation: DefaultContextPropagationBehaviour,
	}
}

// WithName configures the database name of o11y.
// The default if not defined is `crdbpgx`.
func WithName(name string) Opt {
	return func(pc *poolCfg) {
		pc.name = name
	}
}

// WithMaxRetries configures the per-database-operation retry count in case of a node
// unavailability. The default is defined by [DefaultMaxRetries].
func WithMaxRetries(retries int) Opt {
	return func(pc *poolCfg) {
		pc.maxRetries = retries
	}
}

// WithConnectRate configures the rate at which the pool will try to connect to the database.
// The default is defined by [DefaultConnectRate].
func WithConnectRate(rate time.Duration) Opt {
	return func(pc *poolCfg) {
		pc.connectRate = rate
	}
}

// WithContextPropagationBehaviour configures the context propagation behavior.
// See [ContextPropagationBehaviour] for available options.
func WithContextPropagationBehaviour(behavior ContextPropagationBehaviour) Opt {
	return func(pc *poolCfg) {
		pc.ctxPropagation = behavior
	}
}

// WithLogger configures the logger [Pool].
// By default, no logger is used.
func WithLogger(l *slog.Logger) Opt {
	return func(pc *poolCfg) {
		pc.logger = l
	}
}
