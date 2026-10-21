package etl

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/etl/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestStageTracker(t *testing.T) {
	journalBefore := stageCount(t, metrics.StageJournal)
	csvBefore := stageCount(t, metrics.StageCsv)

	s := &stageTracker{}
	s.start(metrics.StageJournal)
	if got := testutil.ToFloat64(metrics.StagesInProgress.WithLabelValues(metrics.StageJournal)); got != 1 {
		t.Errorf("journal stages in progress = %v, want 1", got)
	}
	s.start(metrics.StageCsv)
	if got := testutil.ToFloat64(metrics.StagesInProgress.WithLabelValues(metrics.StageJournal)); got != 0 {
		t.Errorf("journal stages in progress after next stage = %v, want 0", got)
	}
	s.finish()
	s.finish()

	if got := testutil.ToFloat64(metrics.StagesInProgress.WithLabelValues(metrics.StageCsv)); got != 0 {
		t.Errorf("csv stages in progress after finish = %v, want 0", got)
	}
	if got := stageCount(t, metrics.StageJournal) - journalBefore; got != 1 {
		t.Errorf("journal duration observations increased by %d, want 1", got)
	}
	if got := stageCount(t, metrics.StageCsv) - csvBefore; got != 1 {
		t.Errorf("csv duration observations increased by %d, want 1", got)
	}
}

func stageCount(t *testing.T, stage string) uint64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.StageDuration.WithLabelValues(stage).(prometheus.Histogram).Write(&m); err != nil {
		t.Fatalf("failed to read histogram: %s", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestRecordDayResult(t *testing.T) {
	success := metrics.DaysProcessed.WithLabelValues(metrics.ResultSuccess)
	failure := metrics.DaysProcessed.WithLabelValues(metrics.ResultFailure)
	successBefore, failureBefore := testutil.ToFloat64(success), testutil.ToFloat64(failure)

	recordDayResult(errors.New("failed"))
	if got := testutil.ToFloat64(metrics.LastSuccess); got != 0 {
		t.Errorf("last success after failure = %v, want 0", got)
	}
	recordDayResult(nil)

	if got := testutil.ToFloat64(success) - successBefore; got != 1 {
		t.Errorf("successes increased by %v, want 1", got)
	}
	if got := testutil.ToFloat64(failure) - failureBefore; got != 1 {
		t.Errorf("failures increased by %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.LastSuccess); time.Since(time.Unix(int64(got), 0)) > time.Minute {
		t.Errorf("last success = %v, want approximately now", got)
	}
}

func TestCreateGtfsrtExport_CountsFiles(t *testing.T) {
	start := time.Date(2022, time.January, 2, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	dir := t.TempDir()
	writeFile := func(feedID, name string, modTime time.Time) {
		t.Helper()
		path := filepath.Join(dir, feedID, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}
	writeFile("feed1", "a.gtfsrt", start.Add(time.Hour))
	writeFile("feed1", "b.gtfsrt", start.Add(2*time.Hour))
	writeFile("feed1", "before.gtfsrt", start.Add(-time.Hour))
	writeFile("feed1", "after.gtfsrt", end.Add(time.Hour))
	writeFile("feed2", "before.gtfsrt", start.Add(-time.Hour))

	_, numFiles, err := createGtfsrtExport(start, end, dir, []string{"feed1", "feed2"}, 3)
	if err != nil {
		t.Fatalf("createGtfsrtExport() failed: %s", err)
	}

	want := map[string]int{"feed1": 2, "feed2": 0}
	if len(numFiles) != len(want) || numFiles["feed1"] != want["feed1"] || numFiles["feed2"] != want["feed2"] {
		t.Errorf("createGtfsrtExport() file counts = %v, want %v", numFiles, want)
	}
}
