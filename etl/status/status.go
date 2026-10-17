// Package status serves an HTML page describing the state of a long-running ETL process.
package status

import (
	_ "embed"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/website/static"
)

const timeFormat = "January 2, 2006 at 3:04:05pm MST"

var americaNewYork *time.Location

func init() {
	var err error
	americaNewYork, err = time.LoadLocation("America/New_York")
	if err != nil {
		panic("failed to load America/New_York timezone")
	}
}

//go:embed status.html
var statusHtml string

var statusTemplate = template.Must(template.New("status").Parse(statusHtml))

// Status records the state of the ETL process. It is safe for concurrent use.
type Status struct {
	mu             sync.RWMutex
	processStarted time.Time
	startupErr     error
	started        bool
	buckets        []Bucket
	schedule       []string
	lastRun        *run
	lastSuccess    *time.Time
}

// Bucket describes a bucket the pipeline writes to. Credentials must not be included, as the status page is public.
type Bucket struct {
	Name    string
	Url     string
	Primary bool
}

type run struct {
	started  time.Time
	finished *time.Time
	err      error
}

func New() *Status {
	return &Status{processStarted: time.Now()}
}

// SetStartupError records that the process failed to start.
func (s *Status) SetStartupError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startupErr = err
}

// SetStarted records that the process started successfully.
func (s *Status) SetStarted(buckets []Bucket, schedule []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = true
	s.buckets = buckets
	s.schedule = schedule
}

// RunStarted records that a pipeline run has started.
func (s *Status) RunStarted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRun = &run{started: time.Now()}
}

// RunFinished records that the current pipeline run has finished with the provided error.
func (s *Status) RunFinished(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastRun == nil {
		return
	}
	now := time.Now()
	s.lastRun.finished = &now
	s.lastRun.err = err
	if err == nil {
		s.lastSuccess = &now
	}
}

// Serve serves the status page on the provided port. It blocks until the server fails.
func (s *Status) Serve(port int) error {
	log.Printf("Serving ETL status page on port %d\n", port)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), s.Handler())
}

// Handler returns an HTTP handler for the status page.
//
// The page is served with status code 503 if the process failed to start, so that automated
// monitoring can detect the failure, and 200 otherwise.
func (s *Status) Handler() http.Handler {
	mux := http.NewServeMux()
	stylesheet := static.Get().StylesheetCss
	mux.HandleFunc(stylesheet.FullPath(), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		fmt.Fprint(w, stylesheet.Content)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		html, code := s.render()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(code)
		fmt.Fprint(w, html)
	})
	return mux
}

// render returns the status page HTML and the HTTP status code to serve it with.
func (s *Status) render() (string, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type runData struct {
		Started  string
		Finished string
		Duration string
		Error    string
	}
	input := struct {
		StylesheetPath string
		State          string
		StateClass     string
		StartupError   string
		ProcessStarted string
		Schedule       string
		Buckets        []Bucket
		LastRun        *runData
		LastSuccess    string
	}{
		StylesheetPath: static.Get().StylesheetCss.FullPath(),
		ProcessStarted: formatTime(s.processStarted),
		Schedule:       strings.Join(s.schedule, ", "),
		Buckets:        s.buckets,
	}
	if s.lastSuccess != nil {
		input.LastSuccess = formatTime(*s.lastSuccess)
	}
	if s.lastRun != nil {
		input.LastRun = &runData{Started: formatTime(s.lastRun.started)}
		if s.lastRun.finished != nil {
			input.LastRun.Finished = formatTime(*s.lastRun.finished)
			input.LastRun.Duration = s.lastRun.finished.Sub(s.lastRun.started).Round(time.Second).String()
		}
		if s.lastRun.err != nil {
			input.LastRun.Error = s.lastRun.err.Error()
		}
	}
	code := http.StatusOK
	switch {
	case s.startupErr != nil:
		code = http.StatusServiceUnavailable
		input.State, input.StateClass = "Failed to start", "error"
		input.StartupError = s.startupErr.Error()
	case !s.started:
		input.State, input.StateClass = "Starting", "pending"
	case s.lastRun == nil:
		input.State, input.StateClass = "Waiting for first run", "pending"
	case s.lastRun.finished == nil:
		input.State, input.StateClass = "Running", "pending"
	case s.lastRun.err != nil:
		input.State, input.StateClass = "Last run failed", "error"
	default:
		input.State, input.StateClass = "Last run succeeded", "ok"
	}

	var b strings.Builder
	if err := statusTemplate.Execute(&b, input); err != nil {
		panic(err)
	}
	return b.String(), code
}

func formatTime(t time.Time) string {
	return t.In(americaNewYork).Format(timeFormat)
}
