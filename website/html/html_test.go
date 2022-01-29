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
			Home,
		},
		{
			"ExploreTheData",
			ExploreTheData,
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
	}
}
