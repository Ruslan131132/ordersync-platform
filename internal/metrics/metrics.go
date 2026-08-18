package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	JobsFinished     *prometheus.CounterVec
	BatchSize        prometheus.Histogram
	OrdersProcessed  *prometheus.CounterVec
	OrderSyncSeconds *prometheus.HistogramVec
	OrderLagSeconds  *prometheus.HistogramVec
	FreshOrders      prometheus.Gauge

	APICalls         *prometheus.CounterVec
	APICallSeconds   *prometheus.HistogramVec
	Retries          *prometheus.CounterVec
	RateLimitSeconds *prometheus.HistogramVec

	IsLeader           prometheus.Gauge
	LockAcquireSeconds *prometheus.HistogramVec

	OutboxPublished *prometheus.CounterVec
	OutboxBacklog   prometheus.Gauge

	HTTPRequests    *prometheus.CounterVec
	HTTPSeconds     *prometheus.HistogramVec
	MockMarketCalls *prometheus.CounterVec
}

var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		JobsFinished: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_jobs_finished_total",
			Help: "Sync jobs finished, by final status.",
		}, []string{"status"}),
		BatchSize: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "ordersync_batch_size",
			Help:    "Orders claimed per adaptive batch.",
			Buckets: []float64{1, 5, 10, 20, 40, 60, 80, 100, 150, 200, 300},
		}),
		OrdersProcessed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_orders_processed_total",
			Help: "Order sync attempts by marketplace and outcome.",
		}, []string{"marketplace", "outcome"}),
		OrderSyncSeconds: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ordersync_order_sync_duration_seconds",
			Help:    "Per-order time from claim to persisted outcome, including rate-limit waits.",
			Buckets: latencyBuckets,
		}, []string{"marketplace"}),
		OrderLagSeconds: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ordersync_order_end_to_end_lag_seconds",
			Help:    "Time from the order becoming pending to being synced.",
			Buckets: []float64{.1, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}, []string{"marketplace"}),
		FreshOrders: f.NewGauge(prometheus.GaugeOpts{
			Name: "ordersync_fresh_orders",
			Help: "Claimable orders observed when sizing the last batch.",
		}),

		APICalls: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_marketplace_api_calls_total",
			Help: "Marketplace HTTP calls by method and result class.",
		}, []string{"marketplace", "method", "result"}),
		APICallSeconds: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ordersync_marketplace_api_call_duration_seconds",
			Help:    "Marketplace HTTP call latency, excluding rate-limit waits.",
			Buckets: latencyBuckets,
		}, []string{"marketplace", "method"}),
		Retries: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_retries_total",
			Help: "Retries by component and reason.",
		}, []string{"component", "reason"}),
		RateLimitSeconds: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ordersync_rate_limit_wait_seconds",
			Help:    "Time spent waiting for a shared rate-limit token.",
			Buckets: latencyBuckets,
		}, []string{"marketplace", "method"}),

		IsLeader: f.NewGauge(prometheus.GaugeOpts{
			Name: "ordersync_leader",
			Help: "1 if this worker currently holds the scheduler leader lock.",
		}),
		LockAcquireSeconds: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ordersync_lock_acquire_duration_seconds",
			Help:    "Distributed lock acquisition round-trip, by result.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25},
		}, []string{"result"}),

		OutboxPublished: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_outbox_published_total",
			Help: "Outbox events published, by event type.",
		}, []string{"event_type"}),
		OutboxBacklog: f.NewGauge(prometheus.GaugeOpts{
			Name: "ordersync_outbox_backlog",
			Help: "Unpublished outbox events, sampled by the leader.",
		}),

		HTTPRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_http_requests_total",
			Help: "API requests by route and status code.",
		}, []string{"route", "method", "code"}),
		HTTPSeconds: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ordersync_http_request_duration_seconds",
			Help:    "API request latency by route.",
			Buckets: latencyBuckets,
		}, []string{"route", "method"}),
		MockMarketCalls: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_mock_market_requests_total",
			Help: "Requests served by the mock marketplace, by outcome.",
		}, []string{"marketplace", "outcome"}),
	}
}

// LeadershipChanged and AcquireAttempt implement lock.Observer.
func (m *Metrics) LeadershipChanged(leader bool) {
	if leader {
		m.IsLeader.Set(1)
		return
	}
	m.IsLeader.Set(0)
}

func (m *Metrics) AcquireAttempt(d time.Duration, err error) {
	result := "acquired"
	if err != nil {
		result = "not_acquired"
	}
	m.LockAcquireSeconds.WithLabelValues(result).Observe(d.Seconds())
}
