package metadata

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

//go:embed nycsubway.json
var sampleConfig string

func TestSerializeRoundTrip(t *testing.T) {
	var c Metadata
	if err := json.Unmarshal([]byte(sampleConfig), &c); err != nil {
		t.Fatalf("failed to parse metadata: %s", err)
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		t.Fatalf("failed to write metadata: %s", err)
	}
	if string(b) != sampleConfig {
		t.Errorf("re-written metadata doesn't match original. Original\n%s\nRe-Written:\n%s", sampleConfig, string(b))
	}
}

func TestCalculatePendingDays(t *testing.T) {
	jan2 := Day{year: 2022, month: time.January, day: 2}
	jan3 := Day{year: 2022, month: time.January, day: 3}
	jan4 := Day{year: 2022, month: time.January, day: 4}
	jan5 := Day{year: 2022, month: time.January, day: 5}
	jan6 := Day{year: 2022, month: time.January, day: 6}
	feedID1 := "feedID1"
	feedID2 := "feedID2"

	testCases := []struct {
		metadata Metadata
		lastDay  Day
		wantOut  []PendingDay
	}{
		{
			metadata: Metadata{
				Feeds: []FeedMetadata{
					{
						Id:       feedID1,
						FirstDay: jan3,
						LastDay:  &jan6,
					},
				},
				ProcessedDays: []ProcessedDay{
					{
						Day:   jan4,
						Feeds: []string{feedID1},
					},
				},
			},
			lastDay: jan5,
			wantOut: []PendingDay{
				{
					Day:     jan3,
					FeedIDs: []string{feedID1},
				},
				{
					Day:     jan5,
					FeedIDs: []string{feedID1},
				},
			},
		},
		{
			metadata: Metadata{
				Feeds: []FeedMetadata{
					{
						Id:       feedID1,
						FirstDay: jan3,
						LastDay:  &jan6,
					},
				},
				ProcessedDays: []ProcessedDay{
					{
						Day:   jan3,
						Feeds: []string{feedID1},
					},
					{
						Day:   jan5,
						Feeds: []string{feedID1},
					},
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
			metadata: Metadata{
				Feeds: []FeedMetadata{
					{
						Id:       feedID1,
						FirstDay: jan3,
						LastDay:  nil,
					},
				},
				ProcessedDays: []ProcessedDay{
					{
						Day:   jan3,
						Feeds: []string{feedID1},
					},
					{
						Day:   jan5,
						Feeds: []string{feedID1},
					},
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
			metadata: Metadata{
				Feeds: []FeedMetadata{
					{
						Id:       feedID1,
						FirstDay: jan3,
						LastDay:  nil,
					},
				},
				ProcessedDays: []ProcessedDay{
					{
						Day:   jan3,
						Feeds: []string{feedID2},
					},
					{
						Day:   jan5,
						Feeds: []string{feedID2},
					},
				},
			},
			lastDay: jan5,
			wantOut: []PendingDay{
				{
					Day:     jan3,
					FeedIDs: []string{feedID1},
				},
				{
					Day:     jan4,
					FeedIDs: []string{feedID1},
				},
				{
					Day:     jan5,
					FeedIDs: []string{feedID1},
				},
			},
		},
		{
			metadata: Metadata{
				Feeds: []FeedMetadata{
					{
						Id:       feedID1,
						FirstDay: jan3,
						LastDay:  nil,
					},
				},
				ProcessedDays: []ProcessedDay{},
			},
			lastDay: jan2,
			wantOut: []PendingDay{},
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			out := CalculatePendingDays(&testCase.metadata, testCase.lastDay)
			if !reflect.DeepEqual(out, testCase.wantOut) {
				t.Errorf("Expected != actual. Expected:\n%+v\nActual:\n%+v", testCase.wantOut, out)
			}
		})
	}
}
