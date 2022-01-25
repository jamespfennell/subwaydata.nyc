package config

import (
	"sort"

	"github.com/jamespfennell/subwaydata.nyc/metadata"
)

// Config describes the configuration for the ETL pipeline.
type Config struct {
	// Specification of the Hoard feeds are being used as a source.
	Feeds []Feed

	// Timezone to use.
	Timezone string

	// URL of the remote object storage service hosting the bucket.
	BucketUrl string

	// Access key for the bucket.
	BucketAccessKey string

	// Secret key for the bucket.
	BucketSecretKey string

	// Name of the bucket.
	BucketName string

	// Prefix to add to the object key of all objects stored in the bucket.
	BucketPrefix string

	// Prefix to add to the file name of all data objects stored in the bucket.
	RemotePrefix string

	// Path within object storage to the JSON metadata file.
	MetadataPath string
}

type Feed struct {
	// The ID of the feed in the Hoard configuration.
	Id string

	FirstDay metadata.Day
	LastDay  *metadata.Day
}

type PendingDay struct {
	Day     metadata.Day
	FeedIDs []string
}

func CalculatePendingDays(feeds []Feed, processedDays []metadata.ProcessedDay, lastDay metadata.Day) []PendingDay {
	upperBound := lastDay.Next()
	firstDay := upperBound
	for _, feed := range feeds {
		if feed.FirstDay.Before(firstDay) {
			firstDay = feed.FirstDay
		}
	}

	dayToRequiredFeeds := map[metadata.Day][]string{}
	for _, feed := range feeds {
		firstDay := firstDay
		upperBound := upperBound
		if feed.LastDay != nil && feed.LastDay.Before(upperBound) {
			upperBound = feed.LastDay.Next()
		}
		for firstDay.Before(upperBound) {
			dayToRequiredFeeds[firstDay] = append(dayToRequiredFeeds[firstDay], feed.Id)
			firstDay = firstDay.Next()
		}
	}

	dayToProcessedFeeds := map[metadata.Day][]string{}
	for _, processedDay := range processedDays {
		dayToProcessedFeeds[processedDay.Day] = processedDay.Feeds
	}

	result := []PendingDay{}
	for day, requiredFeeds := range dayToRequiredFeeds {
		requiredFeeds := requiredFeeds
		processedFeeds := dayToProcessedFeeds[day]
		if !contains(processedFeeds, requiredFeeds) {
			result = append(result, PendingDay{
				Day:     day,
				FeedIDs: requiredFeeds,
			})
		}
	}

	sortProcessedDays(result)
	return result
}

// contains checks if every element of b is contained in a
func contains(a, b []string) bool {
	aSet := map[string]bool{}
	for _, aElem := range a {
		aSet[aElem] = true
	}
	for _, bElem := range b {
		if !aSet[bElem] {
			return false
		}
	}
	return true
}

func sortProcessedDays(in []PendingDay) {
	sort.Sort(byDay(in))
}

type byDay []PendingDay

func (b byDay) Len() int {
	return len(b)
}

func (b byDay) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byDay) Less(i, j int) bool {
	return b[i].Day.Before(b[j].Day)
}
