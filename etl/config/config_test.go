package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/metadata"
)

//go:embed sample.json
var sampleConfig string

func TestConfig(t *testing.T) {
	var c Config
	if err := json.Unmarshal([]byte(sampleConfig), &c); err != nil {
		t.Fatalf("failed to parse config: %s", err)
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		t.Fatalf("failed to write config: %s", err)
	}
	if string(b) != sampleConfig {
		t.Errorf("re-written config doesn't match original. Original\n%s\nRe-Written:\n%s", sampleConfig, string(b))
	}
}

func TestParse_Valid(t *testing.T) {
	c, err := Parse([]byte(sampleConfig))
	if err != nil {
		t.Fatalf("Parse(sampleConfig) failed: %s", err)
	}
	if len(c.Buckets) != 2 || !c.Buckets[0].Primary || c.Buckets[1].Primary {
		t.Errorf("Parse(sampleConfig) buckets = %+v, want primary followed by secondary", c.Buckets)
	}
}

func TestParse_Invalid(t *testing.T) {
	bucket := func(url, name string, primary bool) Bucket {
		return Bucket{Id: url + "/" + name, Url: url, Name: name, Primary: primary}
	}
	testCases := []struct {
		name    string
		buckets []Bucket
	}{
		{
			name: "no buckets",
		},
		{
			name:    "no primary",
			buckets: []Bucket{bucket("a.com", "b1", false), bucket("a.com", "b2", false)},
		},
		{
			name:    "two primaries",
			buckets: []Bucket{bucket("a.com", "b1", true), bucket("a.com", "b2", true)},
		},
		{
			name:    "duplicate bucket",
			buckets: []Bucket{bucket("a.com", "b1", true), bucket("a.com", "b1", false)},
		},
		{
			name: "missing ID",
			buckets: []Bucket{
				{Url: "a.com", Name: "b1", Primary: true},
			},
		},
		{
			name: "duplicate ID",
			buckets: []Bucket{
				{Id: "id", Url: "a.com", Name: "b1", Primary: true},
				{Id: "id", Url: "b.com", Name: "b1"},
			},
		},
		{
			name:    "missing name",
			buckets: []Bucket{bucket("a.com", "", true)},
		},
		{
			name:    "missing url",
			buckets: []Bucket{bucket("", "b1", true)},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(Config{Buckets: tc.buckets})
			if err != nil {
				t.Fatalf("failed to marshal config: %s", err)
			}
			if _, err := Parse(b); err == nil {
				t.Errorf("Parse() succeeded, want error")
			}
		})
	}
}

func TestFeedsForDay(t *testing.T) {
	jan2 := metadata.NewDay(2022, time.January, 2)
	jan3 := metadata.NewDay(2022, time.January, 3)
	jan4 := metadata.NewDay(2022, time.January, 4)
	jan5 := metadata.NewDay(2022, time.January, 5)
	jan6 := metadata.NewDay(2022, time.January, 6)
	feeds := []Feed{
		{Id: "bounded", FirstDay: jan3, LastDay: &jan5},
		{Id: "unbounded", FirstDay: jan4},
	}
	testCases := []struct {
		day  metadata.Day
		want []string
	}{
		{day: jan2, want: nil},
		{day: jan3, want: []string{"bounded"}},
		{day: jan4, want: []string{"bounded", "unbounded"}},
		{day: jan5, want: []string{"bounded", "unbounded"}},
		{day: jan6, want: []string{"unbounded"}},
	}
	for _, tc := range testCases {
		t.Run(tc.day.String(), func(t *testing.T) {
			if got := FeedsForDay(feeds, tc.day); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("FeedsForDay(%s) = %v, want %v", tc.day, got, tc.want)
			}
		})
	}
}

func TestCalculatePendingDays(t *testing.T) {
	jan2 := metadata.NewDay(2022, time.January, 2)
	jan3 := metadata.NewDay(2022, time.January, 3)
	jan4 := metadata.NewDay(2022, time.January, 4)
	jan5 := metadata.NewDay(2022, time.January, 5)
	jan6 := metadata.NewDay(2022, time.January, 6)
	feedID1 := "feedID1"
	feedID2 := "feedID2"

	testCases := []struct {
		feeds         []Feed
		processedDays []metadata.ProcessedDay
		lastDay       metadata.Day
		wantOut       []PendingDay
	}{
		{
			feeds: []Feed{
				{
					Id:       feedID1,
					FirstDay: jan3,
					LastDay:  &jan6,
				},
			},
			processedDays: []metadata.ProcessedDay{
				{
					Day:   jan4,
					Feeds: []string{feedID1},
				},
			},
			lastDay: jan5,
			wantOut: []PendingDay{
				{
					Day:     jan5,
					FeedIDs: []string{feedID1},
				},
				{
					Day:     jan3,
					FeedIDs: []string{feedID1},
				},
			},
		},

		{
			feeds: []Feed{
				{
					Id:       feedID1,
					FirstDay: jan3,
					LastDay:  &jan6,
				},
			},
			processedDays: []metadata.ProcessedDay{
				{
					Day:   jan3,
					Feeds: []string{feedID1},
				},
				{
					Day:   jan5,
					Feeds: []string{feedID1},
				},
			},
			lastDay: jan5,
			wantOut: []PendingDay{
				{
					Day:     jan4,
					FeedIDs: []string{feedID1},
				},
			},
		},
		{
			feeds: []Feed{
				{
					Id:       feedID1,
					FirstDay: jan3,
					LastDay:  nil,
				},
			},
			processedDays: []metadata.ProcessedDay{
				{
					Day:   jan3,
					Feeds: []string{feedID1},
				},
				{
					Day:   jan5,
					Feeds: []string{feedID1},
				},
			},
			lastDay: jan5,
			wantOut: []PendingDay{
				{
					Day:     jan4,
					FeedIDs: []string{feedID1},
				},
			},
		},
		{
			feeds: []Feed{
				{
					Id:       feedID1,
					FirstDay: jan3,
					LastDay:  nil,
				},
			},
			processedDays: []metadata.ProcessedDay{
				{
					Day:   jan3,
					Feeds: []string{feedID2},
				},
				{
					Day:   jan5,
					Feeds: []string{feedID2},
				},
			},
			lastDay: jan5,
			wantOut: []PendingDay{
				{
					Day:     jan5,
					FeedIDs: []string{feedID1},
				},
				{
					Day:     jan4,
					FeedIDs: []string{feedID1},
				},
				{
					Day:     jan3,
					FeedIDs: []string{feedID1},
				},
			},
		},
		{
			feeds: []Feed{
				{
					Id:       feedID1,
					FirstDay: jan3,
					LastDay:  nil,
				},
			},
			processedDays: []metadata.ProcessedDay{},
			lastDay:       jan2,
			wantOut:       []PendingDay{},
		},
		{
			feeds: []Feed{
				{
					Id:       feedID1,
					FirstDay: jan4,
					LastDay:  nil,
				},
				{
					Id:       feedID2,
					FirstDay: jan5,
					LastDay:  nil,
				},
			},
			processedDays: []metadata.ProcessedDay{
				{
					Day:   jan4,
					Feeds: []string{feedID1},
				},
				{
					Day:   jan5,
					Feeds: []string{feedID1},
				},
			},
			lastDay: jan5,
			wantOut: []PendingDay{
				{
					Day:     jan5,
					FeedIDs: []string{feedID1, feedID2},
				},
			},
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			out := CalculatePendingDays(testCase.feeds, testCase.processedDays, testCase.lastDay, 0)
			if !reflect.DeepEqual(out, testCase.wantOut) {
				t.Errorf("Expected != actual. Expected:\n%+v\nActual:\n%+v", testCase.wantOut, out)
			}
		})
	}
}
