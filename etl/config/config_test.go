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
