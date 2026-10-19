package html

import (
	"testing"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/metadata"
)

func TestTemplates(t *testing.T) {
	m := metadata.Metadata{
		ProcessedDays: []metadata.ProcessedDay{
			{
				Day:     metadata.NewDay(2022, time.January, 28),
				Created: time.Date(2022, time.January, 29, 5, 30, 0, 0, time.UTC),
			},
			{
				Day:     metadata.NewDay(2022, time.January, 27),
				Created: time.Date(2022, time.January, 29, 5, 31, 0, 0, time.UTC),
			},
		},
	}
	tcs := []struct {
		name string
		f    func(*metadata.Metadata) string
	}{
		{
			"Home",
			func(m *metadata.Metadata) string {
				return Home(m, nil)
			},
		},
		{
			"BrowseTheData",
			BrowseTheData,
		},
		{
			"ProgrammaticAccess",
			func(m *metadata.Metadata) string {
				return ProgrammaticAccess()
			},
		},
		{
			"DataSchema",
			func(m *metadata.Metadata) string {
				return DataSchema()
			},
		},
		{
			"HowItWorks",
			func(m *metadata.Metadata) string {
				return HowItWorks()
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc.f(&m)
		})
		t.Run(tc.name+"_nil_metadata", func(t *testing.T) {
			tc.f(nil)
		})
	}
}

func TestFormatNumDays(t *testing.T) {
	testCases := []struct {
		n    int
		want string
	}{
		{0, "0 days"},
		{1, "1 day"},
		{200, "200 days"},
		{365, "1 year"},
		{366, "1 year, 1 day"},
		{730, "2 years"},
		{2025, "5 years, 200 days"},
	}
	for _, tc := range testCases {
		if got := formatNumDays(tc.n); got != tc.want {
			t.Errorf("formatNumDays(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}
