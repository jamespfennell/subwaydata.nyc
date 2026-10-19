// Package metrics defines the Prometheus metrics exported by the ETL pipeline.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Values of the stage label.
const (
	StageDownload = "download"
	StageJournal  = "journal"
	StageCsv      = "csv"
	StageGtfsrt   = "gtfsrt"
	StageUpload   = "upload"
	StageMetadata = "metadata"
)

// Values of the artifact label.
const (
	ArtifactCsv      = "csv"
	ArtifactGtfsrt   = "gtfsrt"
	ArtifactMetadata = "metadata"
)

// Values of the result label.
const (
	ResultSuccess = "success"
	ResultFailure = "failure"
)

var (
	StageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "subwaydatanyc_etl_stage_duration_seconds",
		Help: "Time taken by each stage of the pipeline for a single day, including failed attempts.",
		// 1s to ~2.3h
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"stage"})

	UploadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "subwaydatanyc_etl_upload_duration_seconds",
		Help: "Time taken to upload a single object to a bucket, including failed attempts.",
		// 100ms to ~14m
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 14),
	}, []string{"bucket"})

	DaysInProgress = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "subwaydatanyc_etl_days_in_progress",
		Help: "Number of days currently being processed.",
	})

	StagesInProgress = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "subwaydatanyc_etl_stages_in_progress",
		Help: "Number of days currently in each stage of the pipeline.",
	}, []string{"stage"})

	PendingDays = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "subwaydatanyc_etl_pending_days",
		Help: "Number of days that needed processing at the start of the most recent backlog run.",
	})

	DaysProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "subwaydatanyc_etl_days_processed_total",
		Help: "Number of days processed, by result (success or failure).",
	}, []string{"result"})

	LastSuccess = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "subwaydatanyc_etl_last_success_timestamp_seconds",
		Help: "Unix time at which a day was last processed successfully.",
	})

	UploadedBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "subwaydatanyc_etl_uploaded_bytes_total",
		Help: "Number of bytes successfully uploaded, by bucket and artifact.",
	}, []string{"bucket", "artifact"})

	FeedGtfsrtFiles = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "subwaydatanyc_etl_feed_gtfsrt_files",
		Help: "Number of GTFS-RT snapshots included in a day's GTFS-RT archive, by feed.",
		// 100 to ~29k. A typical day has 300 (alerts) to 23k (1234567) snapshots per feed.
		Buckets: prometheus.ExponentialBuckets(100, 1.5, 15),
	}, []string{"feed"})

	FeedTrips = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "subwaydatanyc_etl_feed_trips",
		Help: "Number of trips in a day's journal, by feed.",
		// 10 to ~6.6k. A typical day has 120 (SIR) to 3.7k (1234567) trips per feed.
		Buckets: prometheus.ExponentialBuckets(10, 1.5, 17),
	}, []string{"feed"})
)

func init() {
	// Initialize the labeled series so that they are exported with value 0 before the first run,
	// rather than missing.
	for _, stage := range []string{StageDownload, StageJournal, StageCsv, StageGtfsrt, StageUpload, StageMetadata} {
		StagesInProgress.WithLabelValues(stage)
	}
	for _, result := range []string{ResultSuccess, ResultFailure} {
		DaysProcessed.WithLabelValues(result)
	}
}
