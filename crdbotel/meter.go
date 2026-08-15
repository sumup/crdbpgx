package crdbotel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/sumup/crdbpgx"
)

const defaultMinimumReadDBStatsInterval = time.Second

const (
	dbClientConnectionCount = "db.client.connection.count"
	dbClientConnectionMax   = "db.client.connection.max"

	poolAcquireOperations      = "crdbpgx.pool.connection.acquire.operations"
	poolAcquireTime            = "crdbpgx.pool.connection.acquire.time"
	poolAcquireCancellations   = "crdbpgx.pool.connection.acquire.cancellations"
	poolConnectionConstructing = "crdbpgx.pool.connection.construction.count"
	poolAcquireWaits           = "crdbpgx.pool.connection.acquire.waits"
	poolDestroyOperations      = "crdbpgx.pool.connection.destroy.operations"
	poolCreateOperations       = "crdbpgx.pool.connection.create.operations"
	poolAcquireWaitTime        = "crdbpgx.pool.connection.acquire.wait_time"

	clientOperations           = "crdbpgx.client.operations"
	clientRetries              = "crdbpgx.client.retries"
	clientResets               = "crdbpgx.client.resets"
	clientRetryExhaustions     = "crdbpgx.client.retry.exhaustions"
	healthNodeCount            = "crdbpgx.health.node.count"
	balancerRuns               = "crdbpgx.balancer.runs"
	balancerConnectionAcquires = "crdbpgx.balancer.connection.acquisitions"
	balancerConnectionPrunes   = "crdbpgx.balancer.connection.prunes"
)

var poolDestroyReasonKey = attribute.Key("crdbpgx.pool.connection.destroy.reason")

// PoolStats provides native pgx and CockroachDB-specific pool statistics.
type PoolStats interface {
	Name() string
	Stat() crdbpgx.Stat
	Config() *pgxpool.Config
}

type statsConfig struct {
	meterProvider              metric.MeterProvider
	minimumReadDBStatsInterval time.Duration
	attributes                 []attribute.KeyValue
}

// RecordStats registers asynchronous OpenTelemetry instruments for native pgx
// pool statistics and CockroachDB-specific retry, reset, health, and balancer
// statistics. Standard connection metrics use the OpenTelemetry database
// conventions; implementation-specific metrics use the crdbpgx namespace.
func RecordStats(db PoolStats, opts ...StatsOption) error {
	cfg := statsConfig{
		meterProvider:              otel.GetMeterProvider(),
		minimumReadDBStatsInterval: defaultMinimumReadDBStatsInterval,
	}
	for _, opt := range opts {
		opt.applyStats(&cfg)
	}

	poolAttrs := append([]attribute.KeyValue(nil), cfg.attributes...)
	poolAttrs = append(poolAttrs, semconv.DBClientConnectionPoolName(db.Name()))
	crdbAttrs := append([]attribute.KeyValue(nil), cfg.attributes...)
	crdbAttrs = append(
		crdbAttrs,
		semconv.DBSystemNameCockroachDB,
		semconv.DBClientConnectionPoolName(db.Name()),
	)

	meter := cfg.meterProvider.Meter(
		instrumentationName,
		metric.WithInstrumentationVersion(instrumentationVersion()),
	)
	if err := recordCRDBStats(meter, db, crdbAttrs...); err != nil {
		return err
	}
	return recordPGXStats(
		meter,
		db,
		cfg.minimumReadDBStatsInterval,
		poolAttrs,
		crdbAttrs,
	)
}

func recordPGXStats(
	meter metric.Meter,
	db PoolStats,
	minimumReadInterval time.Duration,
	poolAttrs []attribute.KeyValue,
	customAttrs []attribute.KeyValue,
) error {
	connectionCount, err := meter.Int64ObservableUpDownCounter(
		dbClientConnectionCount,
		metric.WithDescription("The number of connections currently in each state."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", dbClientConnectionCount, err)
	}
	connectionMax, err := meter.Int64ObservableUpDownCounter(
		dbClientConnectionMax,
		metric.WithDescription("The maximum number of open connections allowed."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", dbClientConnectionMax, err)
	}
	acquireOperations, err := meter.Int64ObservableCounter(
		poolAcquireOperations,
		metric.WithDescription("Cumulative number of successful pool acquire operations."),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", poolAcquireOperations, err)
	}
	acquireTime, err := meter.Float64ObservableCounter(
		poolAcquireTime,
		metric.WithDescription("Cumulative time spent in successful pool acquire operations."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", poolAcquireTime, err)
	}
	acquireCancellations, err := meter.Int64ObservableCounter(
		poolAcquireCancellations,
		metric.WithDescription("Cumulative number of pool acquire operations canceled by context."),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", poolAcquireCancellations, err)
	}
	constructingConnections, err := meter.Int64ObservableUpDownCounter(
		poolConnectionConstructing,
		metric.WithDescription("The number of connections currently being constructed."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", poolConnectionConstructing, err)
	}
	acquireWaits, err := meter.Int64ObservableCounter(
		poolAcquireWaits,
		metric.WithDescription("Cumulative number of successful pool acquires that waited for a connection."),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", poolAcquireWaits, err)
	}
	destroyOperations, err := meter.Int64ObservableCounter(
		poolDestroyOperations,
		metric.WithDescription("Cumulative number of connections destroyed by the pool."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", poolDestroyOperations, err)
	}
	createOperations, err := meter.Int64ObservableCounter(
		poolCreateOperations,
		metric.WithDescription("Cumulative number of connections created by the pool."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", poolCreateOperations, err)
	}
	acquireWaitTime, err := meter.Float64ObservableCounter(
		poolAcquireWaitTime,
		metric.WithDescription("Cumulative time spent waiting for a connection when the pool was empty."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", poolAcquireWaitTime, err)
	}

	poolSet := metricAttributeSet(poolAttrs)
	customSet := metricAttributeSet(customAttrs)
	idleSet := metricAttributeSet(poolAttrs, semconv.DBClientConnectionStateIdle)
	usedSet := metricAttributeSet(poolAttrs, semconv.DBClientConnectionStateUsed)
	idleDestroySet := metricAttributeSet(
		customAttrs,
		poolDestroyReasonKey.String("idle_timeout"),
	)
	lifetimeDestroySet := metricAttributeSet(
		customAttrs,
		poolDestroyReasonKey.String("max_lifetime"),
	)
	poolOpts := []metric.ObserveOption{metric.WithAttributeSet(poolSet)}
	customOpts := []metric.ObserveOption{metric.WithAttributeSet(customSet)}
	idleOpts := []metric.ObserveOption{metric.WithAttributeSet(idleSet)}
	usedOpts := []metric.ObserveOption{metric.WithAttributeSet(usedSet)}
	idleDestroyOpts := []metric.ObserveOption{metric.WithAttributeSet(idleDestroySet)}
	lifetimeDestroyOpts := []metric.ObserveOption{metric.WithAttributeSet(lifetimeDestroySet)}

	var (
		mu       sync.Mutex
		stats    crdbpgx.Stat
		lastRead time.Time
	)
	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		mu.Lock()
		defer mu.Unlock()

		now := time.Now()
		if lastRead.IsZero() || now.Sub(lastRead) >= minimumReadInterval {
			stats = db.Stat()
			lastRead = now
		}
		observer.ObserveInt64(connectionCount, int64(stats.IdleConns()), idleOpts...)
		observer.ObserveInt64(connectionCount, int64(stats.AcquiredConns()), usedOpts...)
		observer.ObserveInt64(connectionMax, int64(stats.MaxConns()), poolOpts...)
		observer.ObserveInt64(acquireOperations, stats.AcquireCount(), customOpts...)
		observer.ObserveFloat64(acquireTime, stats.AcquireDuration().Seconds(), customOpts...)
		observer.ObserveInt64(acquireCancellations, stats.CanceledAcquireCount(), customOpts...)
		observer.ObserveInt64(constructingConnections, int64(stats.ConstructingConns()), customOpts...)
		observer.ObserveInt64(acquireWaits, stats.EmptyAcquireCount(), customOpts...)
		observer.ObserveInt64(destroyOperations, stats.MaxIdleDestroyCount(), idleDestroyOpts...)
		observer.ObserveInt64(destroyOperations, stats.MaxLifetimeDestroyCount(), lifetimeDestroyOpts...)
		observer.ObserveInt64(createOperations, stats.NewConnsCount(), customOpts...)
		observer.ObserveFloat64(acquireWaitTime, stats.EmptyAcquireWaitTime().Seconds(), customOpts...)
		return nil
	},
		connectionCount,
		connectionMax,
		acquireOperations,
		acquireTime,
		acquireCancellations,
		constructingConnections,
		acquireWaits,
		destroyOperations,
		createOperations,
		acquireWaitTime,
	)
	if err != nil {
		return fmt.Errorf("register pgx pool stats callback: %w", err)
	}
	return nil
}

func metricAttributeSet(
	attrs []attribute.KeyValue,
	extra ...attribute.KeyValue,
) attribute.Set {
	all := make([]attribute.KeyValue, 0, len(attrs)+len(extra))
	all = append(all, attrs...)
	all = append(all, extra...)
	return attribute.NewSet(all...)
}

func recordCRDBStats(meter metric.Meter, db PoolStats, attrs ...attribute.KeyValue) error {
	operations, err := meter.Int64ObservableCounter(
		clientOperations,
		metric.WithUnit("{operation}"),
		metric.WithDescription("Cumulative number of completed retry-enabled operations."),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", clientOperations, err)
	}
	retries, err := meter.Int64ObservableCounter(
		clientRetries,
		metric.WithUnit("{retry}"),
		metric.WithDescription("Cumulative number of retry attempts after retryable or resettable failures."),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", clientRetries, err)
	}
	resets, err := meter.Int64ObservableCounter(
		clientResets,
		metric.WithUnit("{reset}"),
		metric.WithDescription("Cumulative number of connections reset after resettable failures."),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", clientResets, err)
	}
	retryExhaustions, err := meter.Int64ObservableCounter(
		clientRetryExhaustions,
		metric.WithUnit("{operation}"),
		metric.WithDescription("Cumulative number of operations that exhausted their retry budget."),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", clientRetryExhaustions, err)
	}
	healthyNodes, err := meter.Int64ObservableGauge(
		healthNodeCount,
		metric.WithUnit("{node}"),
		metric.WithDescription("The number of CockroachDB nodes currently considered healthy."),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", healthNodeCount, err)
	}
	runs, err := meter.Int64ObservableCounter(
		balancerRuns,
		metric.WithUnit("{run}"),
		metric.WithDescription("Cumulative number of connection balancing runs."),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", balancerRuns, err)
	}
	acquisitions, err := meter.Int64ObservableCounter(
		balancerConnectionAcquires,
		metric.WithUnit("{connection}"),
		metric.WithDescription("Cumulative number of idle connections inspected by the balancer."),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", balancerConnectionAcquires, err)
	}
	prunes, err := meter.Int64ObservableCounter(
		balancerConnectionPrunes,
		metric.WithUnit("{connection}"),
		metric.WithDescription("Cumulative number of connections marked for pruning by the balancer."),
	)
	if err != nil {
		return fmt.Errorf("create %s metric: %w", balancerConnectionPrunes, err)
	}

	observeOpts := []metric.ObserveOption{metric.WithAttributeSet(attribute.NewSet(attrs...))}
	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		stats := db.Stat()
		observer.ObserveInt64(operations, int64(stats.Operations()), observeOpts...)
		observer.ObserveInt64(retries, int64(stats.Retries()), observeOpts...)
		observer.ObserveInt64(resets, int64(stats.Resets()), observeOpts...)
		observer.ObserveInt64(retryExhaustions, int64(stats.MaxRetryErrors()), observeOpts...)
		observer.ObserveInt64(healthyNodes, int64(stats.HealthyNodes()), observeOpts...)
		observer.ObserveInt64(runs, int64(stats.BalancerRuns()), observeOpts...)
		observer.ObserveInt64(acquisitions, int64(stats.BalancerAcquiredConnections()), observeOpts...)
		observer.ObserveInt64(prunes, int64(stats.BalancerPrunedConnections()), observeOpts...)
		return nil
	}, operations, retries, resets, retryExhaustions, healthyNodes, runs, acquisitions, prunes)
	if err != nil {
		return fmt.Errorf("register crdbpgx stats callback: %w", err)
	}
	return nil
}
