package crdbpgx

import (
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

type poolStats struct {
	operations                  atomic.Uint64
	retries                     atomic.Uint64
	resets                      atomic.Uint64
	maxRetryErrors              atomic.Uint64
	balancerRuns                atomic.Uint64
	balancerAcquiredConnections atomic.Uint64
	balancerPrunedConnections   atomic.Uint64
}

// Stat is a point-in-time snapshot of Pool statistics.
//
// It embeds [pgxpool.Stat] for native connection-pool counters and adds
// CockroachDB-specific retry, reset, health, and balancer counters. All
// CockroachDB-specific counters are cumulative for the lifetime of the Pool.
type Stat struct {
	*pgxpool.Stat
	operations                  uint64
	retries                     uint64
	resets                      uint64
	maxRetryErrors              uint64
	healthyNodes                int
	balancerRuns                uint64
	balancerAcquiredConnections uint64
	balancerPrunedConnections   uint64
}

// Operations returns the number of completed retry-enabled operations.
func (s Stat) Operations() uint64 { return s.operations }

// Retries returns the cumulative number of retry attempts after retryable or resettable failures.
func (s Stat) Retries() uint64 { return s.retries }

// Resets returns the cumulative number of connections reset after resettable failures.
func (s Stat) Resets() uint64 { return s.resets }

// MaxRetryErrors returns the cumulative number of operations that exhausted their retry budget.
func (s Stat) MaxRetryErrors() uint64 { return s.maxRetryErrors }

// HealthyNodes returns the number of CockroachDB nodes currently considered healthy.
func (s Stat) HealthyNodes() int { return s.healthyNodes }

// BalancerRuns returns the cumulative number of connection balancing runs.
func (s Stat) BalancerRuns() uint64 { return s.balancerRuns }

// BalancerAcquiredConnections returns the cumulative number of idle connections inspected by the balancer.
func (s Stat) BalancerAcquiredConnections() uint64 { return s.balancerAcquiredConnections }

// BalancerPrunedConnections returns the cumulative number of connections marked for pruning by the balancer.
func (s Stat) BalancerPrunedConnections() uint64 { return s.balancerPrunedConnections }

func (s *poolStats) snapshot(pgxStat *pgxpool.Stat, healthyNodes int) Stat {
	return Stat{
		Stat:                        pgxStat,
		operations:                  s.operations.Load(),
		retries:                     s.retries.Load(),
		resets:                      s.resets.Load(),
		maxRetryErrors:              s.maxRetryErrors.Load(),
		healthyNodes:                healthyNodes,
		balancerRuns:                s.balancerRuns.Load(),
		balancerAcquiredConnections: s.balancerAcquiredConnections.Load(),
		balancerPrunedConnections:   s.balancerPrunedConnections.Load(),
	}
}
