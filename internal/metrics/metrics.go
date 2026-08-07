package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	JobsProcessed   *prometheus.CounterVec
	OrdersProcessed *prometheus.CounterVec
	BatchSize       prometheus.Histogram
	APICalls        *prometheus.CounterVec
	Retries         *prometheus.CounterVec
	LockWait        prometheus.Histogram
	SyncLatency     *prometheus.HistogramVec
	OutboxPublished prometheus.Counter
}

func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)
	return &Metrics{
		JobsProcessed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_jobs_processed_total",
			Help: "Sync jobs processed by outcome",
		}, []string{"status"}),
		OrdersProcessed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_orders_processed_total",
			Help: "Orders processed by marketplace and outcome",
		}, []string{"marketplace", "status"}),
		BatchSize: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "ordersync_batch_size",
			Help:    "Adaptive batch size chosen per job",
			Buckets: []float64{1, 5, 10, 20, 40, 60, 80, 100, 150, 200},
		}),
		APICalls: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_marketplace_api_calls_total",
			Help: "Mock marketplace API calls by method and result",
		}, []string{"marketplace", "method", "result"}),
		Retries: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "ordersync_retries_total",
			Help: "Retry attempts by component",
		}, []string{"component"}),
		LockWait: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "ordersync_lock_wait_seconds",
			Help:    "Time spent waiting for distributed locks",
			Buckets: prometheus.DefBuckets,
		}),
		SyncLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ordersync_sync_latency_seconds",
			Help:    "End-to-end order sync latency",
			Buckets: prometheus.DefBuckets,
		}, []string{"marketplace"}),
		OutboxPublished: factory.NewCounter(prometheus.CounterOpts{
			Name: "ordersync_outbox_published_total",
			Help: "Outbox events successfully published by consumer",
		}),
	}
}
