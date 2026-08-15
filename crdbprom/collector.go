package crdbprom

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sumup/crdbpgx"
)

const defaultNamespace = "crdbpgx"

// StatsProvider provides native pgx and CockroachDB-specific pool statistics.
type StatsProvider interface {
	Name() string
	Stat() crdbpgx.Stat
}

// Option configures a Collector.
type Option func(*config)

type config struct {
	namespace string
	labels    prometheus.Labels
}

// WithNamespace configures the Prometheus metric namespace. The default is crdbpgx.
func WithNamespace(namespace string) Option {
	return func(cfg *config) {
		if namespace != "" {
			cfg.namespace = namespace
		}
	}
}

// WithLabels adds constant labels to every metric. The pool label defaults to
// StatsProvider.Name and can be overridden explicitly.
func WithLabels(labels prometheus.Labels) Option {
	return func(cfg *config) {
		for key, value := range labels {
			cfg.labels[key] = value
		}
	}
}

// Collector exposes native pgx pool statistics and CockroachDB-specific retry,
// reset, health, and balancer statistics as Prometheus metrics.
type Collector struct {
	pool StatsProvider

	poolAcquires            *prometheus.Desc
	poolAcquireSeconds      *prometheus.Desc
	poolAcquiredConnections *prometheus.Desc
	poolCanceledAcquires    *prometheus.Desc
	poolConstructingConns   *prometheus.Desc
	poolEmptyAcquires       *prometheus.Desc
	poolEmptyAcquireWait    *prometheus.Desc
	poolIdleConnections     *prometheus.Desc
	poolMaxConnections      *prometheus.Desc
	poolMaxIdleDestroys     *prometheus.Desc
	poolMaxLifetimeDestroys *prometheus.Desc
	poolNewConnections      *prometheus.Desc
	poolTotalConnections    *prometheus.Desc
	clientOperations        *prometheus.Desc
	clientRetries           *prometheus.Desc
	clientResets            *prometheus.Desc
	clientRetryExhaustions  *prometheus.Desc
	healthNodes             *prometheus.Desc
	balancerRuns            *prometheus.Desc
	balancerAcquiredConns   *prometheus.Desc
	balancerPrunedConns     *prometheus.Desc
}

var _ prometheus.Collector = (*Collector)(nil)

// NewCollector creates a Prometheus collector for pool.
func NewCollector(pool StatsProvider, opts ...Option) *Collector {
	cfg := config{
		namespace: defaultNamespace,
		labels: prometheus.Labels{
			"pool": pool.Name(),
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	desc := func(subsystem, name, help string) *prometheus.Desc {
		return prometheus.NewDesc(
			prometheus.BuildFQName(cfg.namespace, subsystem, name),
			help,
			nil,
			cfg.labels,
		)
	}

	return &Collector{
		pool: pool,
		poolAcquires: desc("pool", "acquires_total",
			"Total number of successful connection acquires from the pool."),
		poolAcquireSeconds: desc("pool", "acquire_seconds_total",
			"Total time spent successfully acquiring connections from the pool."),
		poolAcquiredConnections: desc("pool", "acquired_connections",
			"Number of currently acquired connections."),
		poolCanceledAcquires: desc("pool", "canceled_acquires_total",
			"Total number of connection acquires canceled by a context."),
		poolConstructingConns: desc("pool", "constructing_connections",
			"Number of connections currently being constructed."),
		poolEmptyAcquires: desc("pool", "empty_acquires_total",
			"Total number of successful acquires that waited because the pool was empty."),
		poolEmptyAcquireWait: desc("pool", "empty_acquire_wait_seconds_total",
			"Total time spent waiting for successful acquires while the pool was empty."),
		poolIdleConnections: desc("pool", "idle_connections",
			"Number of currently idle connections."),
		poolMaxConnections: desc("pool", "max_connections",
			"Maximum number of pool connections."),
		poolMaxIdleDestroys: desc("pool", "max_idle_destroys_total",
			"Total number of connections destroyed after exceeding their idle time."),
		poolMaxLifetimeDestroys: desc("pool", "max_lifetime_destroys_total",
			"Total number of connections destroyed after exceeding their lifetime."),
		poolNewConnections: desc("pool", "new_connections_total",
			"Total number of connections opened."),
		poolTotalConnections: desc("pool", "total_connections",
			"Number of currently open pool connections."),
		clientOperations: desc("client", "operations_total",
			"Total number of completed retry-enabled operations."),
		clientRetries: desc("client", "retries_total",
			"Total number of retry attempts after retryable or resettable failures."),
		clientResets: desc("client", "resets_total",
			"Total number of connections reset after resettable failures."),
		clientRetryExhaustions: desc("client", "retry_exhaustions_total",
			"Total number of operations that exhausted their retry budget."),
		healthNodes: desc("health", "nodes",
			"Number of CockroachDB nodes currently considered healthy."),
		balancerRuns: desc("balancer", "runs_total",
			"Total number of connection balancing runs."),
		balancerAcquiredConns: desc("balancer", "acquired_connections_total",
			"Total number of idle connections inspected by the balancer."),
		balancerPrunedConns: desc("balancer", "pruned_connections_total",
			"Total number of connections marked for pruning by the balancer."),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(descs chan<- *prometheus.Desc) {
	descs <- c.poolAcquires
	descs <- c.poolAcquireSeconds
	descs <- c.poolAcquiredConnections
	descs <- c.poolCanceledAcquires
	descs <- c.poolConstructingConns
	descs <- c.poolEmptyAcquires
	descs <- c.poolEmptyAcquireWait
	descs <- c.poolIdleConnections
	descs <- c.poolMaxConnections
	descs <- c.poolMaxIdleDestroys
	descs <- c.poolMaxLifetimeDestroys
	descs <- c.poolNewConnections
	descs <- c.poolTotalConnections
	descs <- c.clientOperations
	descs <- c.clientRetries
	descs <- c.clientResets
	descs <- c.clientRetryExhaustions
	descs <- c.healthNodes
	descs <- c.balancerRuns
	descs <- c.balancerAcquiredConns
	descs <- c.balancerPrunedConns
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(metrics chan<- prometheus.Metric) {
	stats := c.pool.Stat()

	metrics <- prometheus.MustNewConstMetric(c.poolAcquires, prometheus.CounterValue, float64(stats.AcquireCount()))
	metrics <- prometheus.MustNewConstMetric(c.poolAcquireSeconds, prometheus.CounterValue, stats.AcquireDuration().Seconds())
	metrics <- prometheus.MustNewConstMetric(c.poolAcquiredConnections, prometheus.GaugeValue, float64(stats.AcquiredConns()))
	metrics <- prometheus.MustNewConstMetric(c.poolCanceledAcquires, prometheus.CounterValue, float64(stats.CanceledAcquireCount()))
	metrics <- prometheus.MustNewConstMetric(c.poolConstructingConns, prometheus.GaugeValue, float64(stats.ConstructingConns()))
	metrics <- prometheus.MustNewConstMetric(c.poolEmptyAcquires, prometheus.CounterValue, float64(stats.EmptyAcquireCount()))
	metrics <- prometheus.MustNewConstMetric(c.poolEmptyAcquireWait, prometheus.CounterValue, stats.EmptyAcquireWaitTime().Seconds())
	metrics <- prometheus.MustNewConstMetric(c.poolIdleConnections, prometheus.GaugeValue, float64(stats.IdleConns()))
	metrics <- prometheus.MustNewConstMetric(c.poolMaxConnections, prometheus.GaugeValue, float64(stats.MaxConns()))
	metrics <- prometheus.MustNewConstMetric(c.poolMaxIdleDestroys, prometheus.CounterValue, float64(stats.MaxIdleDestroyCount()))
	metrics <- prometheus.MustNewConstMetric(c.poolMaxLifetimeDestroys, prometheus.CounterValue, float64(stats.MaxLifetimeDestroyCount()))
	metrics <- prometheus.MustNewConstMetric(c.poolNewConnections, prometheus.CounterValue, float64(stats.NewConnsCount()))
	metrics <- prometheus.MustNewConstMetric(c.poolTotalConnections, prometheus.GaugeValue, float64(stats.TotalConns()))
	metrics <- prometheus.MustNewConstMetric(c.clientOperations, prometheus.CounterValue, float64(stats.Operations()))
	metrics <- prometheus.MustNewConstMetric(c.clientRetries, prometheus.CounterValue, float64(stats.Retries()))
	metrics <- prometheus.MustNewConstMetric(c.clientResets, prometheus.CounterValue, float64(stats.Resets()))
	metrics <- prometheus.MustNewConstMetric(c.clientRetryExhaustions, prometheus.CounterValue, float64(stats.MaxRetryErrors()))
	metrics <- prometheus.MustNewConstMetric(c.healthNodes, prometheus.GaugeValue, float64(stats.HealthyNodes()))
	metrics <- prometheus.MustNewConstMetric(c.balancerRuns, prometheus.CounterValue, float64(stats.BalancerRuns()))
	metrics <- prometheus.MustNewConstMetric(c.balancerAcquiredConns, prometheus.CounterValue, float64(stats.BalancerAcquiredConnections()))
	metrics <- prometheus.MustNewConstMetric(c.balancerPrunedConns, prometheus.CounterValue, float64(stats.BalancerPrunedConnections()))
}
