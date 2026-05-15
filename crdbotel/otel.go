package crdbotel

import (
	"context"
	"fmt"
	"sync"

	"github.com/sumup/crdbpgx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationScope = "github.com/sumup/crdbpgx"

type plugin struct {
	meter metric.Meter

	once sync.Once

	resetHistogram           metric.Int64Histogram
	balancerAcquireHistogram metric.Int64Histogram
	balancerPruneHistogram   metric.Int64Histogram
	healthyNodeCountGauge    metric.Int64Gauge
	initErr                  error
}

// New returns lifecycle hooks that wire crdbpgx into OpenTelemetry.
func New() crdbpgx.Hooks {
	p := &plugin{
		meter: otel.Meter(instrumentationScope),
	}

	return crdbpgx.Hooks{
		DetachContext:              p.detachContext,
		OnAcquireFromDifferentNode: p.onAcquireFromDifferentNode,
		OnRetriesComplete:          p.onRetriesComplete,
		OnBalancerRun:              p.onBalancerRun,
		OnNodeHealthChange:         p.onNodeHealthChange,
	}
}

type ctxDetachedKey struct{}

func isDetachedContext(ctx context.Context) bool {
	return ctx.Value(ctxDetachedKey{}) != nil
}

func (p *plugin) detachContext(ctx context.Context) context.Context {
	if isDetachedContext(ctx) {
		return ctx
	}

	span := trace.SpanFromContext(ctx)

	newCtx := context.Background()
	newCtx = trace.ContextWithSpan(newCtx, span)
	newCtx = context.WithValue(newCtx, ctxDetachedKey{}, struct{}{})
	newCtx = context.WithValue(newCtx, crdbpgx.CtxDisableRetries, ctx.Value(crdbpgx.CtxDisableRetries))

	return newCtx
}

func (p *plugin) onAcquireFromDifferentNode(ctx context.Context, event crdbpgx.AcquireFromDifferentNodeEvent) {
	span := trace.SpanFromContext(ctx)

	switch event.Outcome {
	case crdbpgx.AcquireFromDifferentNodeStarted:
		span.AddEvent("acquiring connection from different node", trace.WithAttributes(
			attribute.Int64("current_node_id", int64(event.CurrentNodeID)),
		))
	case crdbpgx.AcquireFromDifferentNodeOnlyOneHealthy:
		span.AddEvent("only one healthy node available, reuse connection", trace.WithAttributes(
			attribute.Int64("current_node_id", int64(event.CurrentNodeID)),
		))
	case crdbpgx.AcquireFromDifferentNodeDifferentAcquired:
		span.AddEvent("acquired connection from different node", trace.WithAttributes(
			attribute.Int64("node_id", int64(event.AcquiredNodeID)),
		))
	}
}

func (p *plugin) onRetriesComplete(ctx context.Context, event crdbpgx.RetriesCompleteEvent) {
	if err := p.initInstruments(); err != nil {
		return
	}

	p.resetHistogram.Record(ctx, int64(event.Retries),
		metric.WithAttributes(attribute.String("pool.id", event.PoolName)),
	)
}

func (p *plugin) onBalancerRun(ctx context.Context, event crdbpgx.BalancerRunEvent) {
	if err := p.initInstruments(); err != nil {
		return
	}

	attrs := metric.WithAttributes(attribute.String("pool.id", event.PoolName))
	p.balancerAcquireHistogram.Record(ctx, int64(event.AcquiredIdleConns), attrs)
	p.balancerPruneHistogram.Record(ctx, int64(event.PrunedConns), attrs)
}

func (p *plugin) onNodeHealthChange(ctx context.Context, event crdbpgx.NodeHealthChangeEvent) {
	if err := p.initInstruments(); err != nil {
		return
	}

	p.healthyNodeCountGauge.Record(ctx, int64(event.HealthyNodes))
}

func (p *plugin) initInstruments() error {
	p.once.Do(func() {
		var err error

		p.resetHistogram, err = p.meter.Int64Histogram(
			"crdbpgx.resets",
			metric.WithDescription("CockroachDB client-side transaction reset distribution"),
			metric.WithExplicitBucketBoundaries(0, 1, 2, 5, 10, 20, 50),
		)
		if err != nil {
			p.initErr = fmt.Errorf("initialize crdb client resets histogram: %w", err)
			return
		}

		p.balancerAcquireHistogram, err = p.meter.Int64Histogram(
			"crdbpgx.balancer.acquired",
			metric.WithUnit("{connection}"),
			metric.WithDescription("connections acquired for pruning"),
			metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50),
		)
		if err != nil {
			p.initErr = fmt.Errorf("initialize crdb balancer acquire histogram: %w", err)
			return
		}

		p.balancerPruneHistogram, err = p.meter.Int64Histogram(
			"crdbpgx.balancer.pruned",
			metric.WithUnit("{connection}"),
			metric.WithDescription("connections pruned per balancing run"),
			metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50),
		)
		if err != nil {
			p.initErr = fmt.Errorf("initialize crdb balancer prune histogram: %w", err)
			return
		}

		p.healthyNodeCountGauge, err = p.meter.Int64Gauge(
			"crdb_healthy_nodes",
			metric.WithDescription("Number of health CRDB nodes detected"),
		)
		if err != nil {
			p.initErr = fmt.Errorf("initialize crdb health nodes gauge: %w", err)
		}
	})

	return p.initErr
}
