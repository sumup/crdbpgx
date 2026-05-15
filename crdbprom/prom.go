package crdbprom

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sumup/crdbpgx"
)

type config struct {
	registerer prometheus.Registerer
}

// Option configures the Prometheus plugin.
type Option func(*config)

// WithRegisterer sets the Prometheus registerer used for metric registration.
func WithRegisterer(registerer prometheus.Registerer) Option {
	return func(cfg *config) {
		cfg.registerer = registerer
	}
}

type plugin struct {
	registerer prometheus.Registerer

	once sync.Once

	resetHistogram           *prometheus.HistogramVec
	balancerAcquireHistogram *prometheus.HistogramVec
	balancerPruneHistogram   *prometheus.HistogramVec
	healthyNodeCountGauge    prometheus.Gauge
	initErr                  error
}

// New returns lifecycle hooks that expose crdbpgx metrics through Prometheus.
func New(opts ...Option) crdbpgx.Hooks {
	cfg := config{
		registerer: prometheus.DefaultRegisterer,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	p := &plugin{
		registerer: cfg.registerer,
	}

	return crdbpgx.Hooks{
		OnRetriesComplete:  p.onRetriesComplete,
		OnBalancerRun:      p.onBalancerRun,
		OnNodeHealthChange: p.onNodeHealthChange,
	}
}

func (p *plugin) onRetriesComplete(_ context.Context, event crdbpgx.RetriesCompleteEvent) {
	if err := p.initCollectors(); err != nil {
		return
	}

	p.resetHistogram.WithLabelValues(event.PoolName).Observe(float64(event.Retries))
}

func (p *plugin) onBalancerRun(_ context.Context, event crdbpgx.BalancerRunEvent) {
	if err := p.initCollectors(); err != nil {
		return
	}

	p.balancerAcquireHistogram.WithLabelValues(event.PoolName).Observe(float64(event.AcquiredIdleConns))
	p.balancerPruneHistogram.WithLabelValues(event.PoolName).Observe(float64(event.PrunedConns))
}

func (p *plugin) onNodeHealthChange(_ context.Context, event crdbpgx.NodeHealthChangeEvent) {
	if err := p.initCollectors(); err != nil {
		return
	}

	p.healthyNodeCountGauge.Set(float64(event.HealthyNodes))
}

func (p *plugin) initCollectors() error {
	p.once.Do(func() {
		var err error

		p.resetHistogram, err = registerHistogramVec(p.registerer, prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "crdbpgx_resets",
				Help:    "CockroachDB client-side transaction reset distribution.",
				Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
			},
			[]string{"pool_id"},
		))
		if err != nil {
			p.initErr = fmt.Errorf("register crdb client resets histogram: %w", err)
			return
		}

		p.balancerAcquireHistogram, err = registerHistogramVec(p.registerer, prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "crdbpgx_balancer_acquired",
				Help:    "Connections acquired for pruning.",
				Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50},
			},
			[]string{"pool_id"},
		))
		if err != nil {
			p.initErr = fmt.Errorf("register crdb balancer acquire histogram: %w", err)
			return
		}

		p.balancerPruneHistogram, err = registerHistogramVec(p.registerer, prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "crdbpgx_balancer_pruned",
				Help:    "Connections pruned per balancing run.",
				Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 50},
			},
			[]string{"pool_id"},
		))
		if err != nil {
			p.initErr = fmt.Errorf("register crdb balancer prune histogram: %w", err)
			return
		}

		p.healthyNodeCountGauge, err = registerGauge(p.registerer, prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "crdb_healthy_nodes",
				Help: "Number of healthy CRDB nodes detected.",
			},
		))
		if err != nil {
			p.initErr = fmt.Errorf("register crdb health nodes gauge: %w", err)
		}
	})

	return p.initErr
}

func registerHistogramVec(registerer prometheus.Registerer, collector *prometheus.HistogramVec) (*prometheus.HistogramVec, error) {
	if err := registerer.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.HistogramVec)
			if !ok {
				return nil, err
			}
			return existing, nil
		}
		return nil, err
	}

	return collector, nil
}

func registerGauge(registerer prometheus.Registerer, collector prometheus.Gauge) (prometheus.Gauge, error) {
	if err := registerer.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			existing, ok := alreadyRegistered.ExistingCollector.(prometheus.Gauge)
			if !ok {
				return nil, err
			}
			return existing, nil
		}
		return nil, err
	}

	return collector, nil
}
