package status

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var testBuckets = []Bucket{
	{Primary: true, Url: "nyc3.digitaloceanspaces.com", Name: "do-bucket"},
	{Url: "account.r2.cloudflarestorage.com", Name: "r2-bucket"},
}

func TestRender(t *testing.T) {
	testCases := []struct {
		name       string
		setup      func(s *Status)
		wantState  string
		wantCode   int
		wantText   []string
		unwantText []string
	}{
		{
			name:      "starting",
			setup:     func(s *Status) {},
			wantState: "Starting",
		},
		{
			name: "startup error",
			setup: func(s *Status) {
				s.SetStartupError(errors.New("no buckets configured"))
			},
			wantState:  "Failed to start",
			wantCode:   http.StatusServiceUnavailable,
			wantText:   []string{"no buckets configured"},
			unwantText: []string{"Buckets ("},
		},
		{
			name: "waiting for first run",
			setup: func(s *Status) {
				s.SetStarted(testBuckets, []string{"05:30:00-06:00:00"})
			},
			wantState: "Waiting for first run",
			wantText:  []string{"Buckets (2)", "do-bucket", "(primary)", "r2-bucket", "05:30:00-06:00:00"},
		},
		{
			name: "running",
			setup: func(s *Status) {
				s.SetStarted(testBuckets, nil)
				s.RunStarted()
			},
			wantState: "Running",
			wantText:  []string{"In progress"},
		},
		{
			name: "last run failed",
			setup: func(s *Status) {
				s.SetStarted(testBuckets, nil)
				s.RunStarted()
				s.RunFinished(nil)
				s.RunStarted()
				s.RunFinished(errors.New("failed to copy bytes to bucket r2-bucket"))
			},
			wantState: "Last run failed",
			wantText:  []string{"failed to copy bytes to bucket r2-bucket", "Last success"},
		},
		{
			name: "last run succeeded",
			setup: func(s *Status) {
				s.SetStarted(testBuckets, nil)
				s.RunStarted()
				s.RunFinished(nil)
			},
			wantState: "Last run succeeded",
			wantText:  []string{"Succeeded"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			tc.setup(s)
			wantCode := tc.wantCode
			if wantCode == 0 {
				wantCode = http.StatusOK
			}

			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

			if w.Code != wantCode {
				t.Errorf("status code = %d, want %d", w.Code, wantCode)
			}
			html := w.Body.String()

			for _, want := range append(tc.wantText, tc.wantState) {
				if !strings.Contains(html, want) {
					t.Errorf("Render() does not contain %q", want)
				}
			}
			for _, unwant := range tc.unwantText {
				if strings.Contains(html, unwant) {
					t.Errorf("Render() contains %q", unwant)
				}
			}
		})
	}
}

func TestHandler_Stylesheet(t *testing.T) {
	w := httptest.NewRecorder()
	New().Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/static/stylesheet.css", nil))

	if w.Code != http.StatusOK || w.Body.Len() == 0 {
		t.Errorf("stylesheet response = (%d, %d bytes), want (200, non-empty)", w.Code, w.Body.Len())
	}
}

func TestHandler_NotFound(t *testing.T) {
	w := httptest.NewRecorder()
	New().Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/other", nil))

	if w.Code != http.StatusNotFound {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusNotFound)
	}
}
