package crdbotel

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/semconv/v1.43.0/dbconv"

	"github.com/sumup/crdbpgx"
)

// OnPoolCreated registers connection statistics when the meter is passed to
// crdbpgx.WithHooks. Pool.Close calls the returned cleanup to unregister them.
// Instrumentation errors are reported through otel.Handle.
func (m *Meter) OnPoolCreated(pool *crdbpgx.Pool) func() {
	registration, err := m.registerPool(pool.Name(), pool.Stat)
	if err != nil {
		otel.Handle(err)
		return nil
	}
	return func() {
		if err := registration.Unregister(); err != nil {
			otel.Handle(fmt.Errorf("unregister pool statistics: %w", err))
		}
	}
}

func (m *Meter) registerPool(name string, stat func() *pgxpool.Stat) (metric.Registration, error) {
	connections, err := dbconv.NewClientConnectionCountObservable(m.meter)
	if err != nil {
		return nil, fmt.Errorf("initialize pool connection count: %w", err)
	}

	limit, err := dbconv.NewClientConnectionMaxObservable(m.meter)
	if err != nil {
		return nil, fmt.Errorf("initialize pool connection limit: %w", err)
	}

	constructing, err := m.meter.Int64ObservableUpDownCounter("crdbpgx.pool.connections.constructing",
		metric.WithUnit("{connection}"), metric.WithDescription("Connections currently being constructed"))
	if err != nil {
		return nil, fmt.Errorf("initialize constructing connections: %w", err)
	}

	counters := []struct {
		name, unit, description string
		value                   func(*pgxpool.Stat) int64
		instrument              metric.Int64ObservableCounter
	}{
		{
			name:        "acquires",
			unit:        "{acquisition}",
			description: "Successful pool acquisitions",
			value:       (*pgxpool.Stat).AcquireCount,
		},
		{
			name:        "acquires.canceled",
			unit:        "{acquisition}",
			description: "Pool acquisitions canceled by context",
			value:       (*pgxpool.Stat).CanceledAcquireCount,
		},
		{
			name:        "acquires.waited",
			unit:        "{acquisition}",
			description: "Successful acquisitions that waited for an available connection",
			value:       (*pgxpool.Stat).EmptyAcquireCount,
		},
		{
			name:        "connections.created",
			unit:        "{connection}",
			description: "New connections opened",
			value:       (*pgxpool.Stat).NewConnsCount,
			instrument:  nil,
		},
		{
			name:        "connections.destroyed.lifetime",
			unit:        "{connection}",
			description: "Connections destroyed after exceeding maximum lifetime",
			value:       (*pgxpool.Stat).MaxLifetimeDestroyCount,
		},
		{
			name:        "connections.destroyed.idle",
			unit:        "{connection}",
			description: "Connections destroyed after exceeding maximum idle time",
			value:       (*pgxpool.Stat).MaxIdleDestroyCount,
			instrument:  nil,
		},
	}

	observables := []metric.Observable{connections.Inst(), limit.Inst(), constructing}
	for i := range counters {
		counter := &counters[i]
		counter.instrument, err = m.meter.Int64ObservableCounter(
			"crdbpgx.pool."+counter.name,
			metric.WithUnit(counter.unit),
			metric.WithDescription(counter.description),
		)
		if err != nil {
			return nil, fmt.Errorf("initialize pool %s: %w", counter.name, err)
		}

		observables = append(observables, counter.instrument)
	}

	acquireTime, err := m.meter.Float64ObservableCounter("crdbpgx.pool.acquire.duration",
		metric.WithUnit("s"), metric.WithDescription("Cumulative duration of successful pool acquisitions"))
	if err != nil {
		return nil, fmt.Errorf("initialize pool acquisition duration: %w", err)
	}

	waitTime, err := m.meter.Float64ObservableCounter("crdbpgx.pool.acquire.wait.duration",
		metric.WithUnit("s"), metric.WithDescription("Cumulative wait time of successful acquisitions when the pool was empty"))
	if err != nil {
		return nil, fmt.Errorf("initialize pool acquisition wait duration: %w", err)
	}
	observables = append(observables, acquireTime, waitTime)

	attrs := m.attributes.ToSlice()
	attrs = append(attrs, connections.AttrClientConnectionPoolName(name))

	options := metric.WithAttributes(attrs...)
	idle := metric.WithAttributes(connections.AttrClientConnectionState(dbconv.ClientConnectionStateIdle))
	used := metric.WithAttributes(connections.AttrClientConnectionState(dbconv.ClientConnectionStateUsed))

	registration, err := m.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		s := stat()
		observer.ObserveInt64(connections.Inst(), int64(s.IdleConns()), options, idle)
		observer.ObserveInt64(connections.Inst(), int64(s.AcquiredConns()), options, used)
		observer.ObserveInt64(limit.Inst(), int64(s.MaxConns()), options)
		observer.ObserveInt64(constructing, int64(s.ConstructingConns()), options)
		for _, counter := range counters {
			observer.ObserveInt64(counter.instrument, counter.value(s), options)
		}
		observer.ObserveFloat64(acquireTime, s.AcquireDuration().Seconds(), options)
		observer.ObserveFloat64(waitTime, s.EmptyAcquireWaitTime().Seconds(), options)
		return nil
	}, observables...)
	if err != nil {
		return nil, fmt.Errorf("register pool statistics: %w", err)
	}

	return registration, nil
}
