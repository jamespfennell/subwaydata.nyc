package config

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"sigs.k8s.io/yaml"
)

// DefaultGtfsrtCompressionLevel is the xz compression level used for the GTFS-RT archive if none is configured.
const DefaultGtfsrtCompressionLevel = 6

// Config describes the configuration for the ETL pipeline.
type Config struct {
	// Specification of the Hoard feeds are being used as a source.
	Feeds []Feed

	// Timezone to use.
	Timezone Timezone

	// Object storage buckets the pipeline writes its output to.
	//
	// Every artifact and metadata update is written to all buckets.
	// Exactly one bucket must be marked as the primary.
	Buckets []Bucket

	// Prefix to add to the file name of all data objects stored in the bucket.
	RemotePrefix string

	// Path within object storage to the JSON metadata file.
	MetadataPath string

	// xz compression level (0-9) for the GTFS-RT archive. If unset, DefaultGtfsrtCompressionLevel is used.
	//
	// The GTFS-RT archive is large (around 8GB uncompressed per day) and compression dominates its build time.
	// Levels 4 and above are much slower than level 3 on this data, for around 25% better compression.
	GtfsrtCompressionLevel *int `json:",omitempty"`
}

// GtfsrtCompressionLevelOrDefault returns the configured GTFS-RT compression level, or the default if none is set.
func (c *Config) GtfsrtCompressionLevelOrDefault() int {
	if c.GtfsrtCompressionLevel == nil {
		return DefaultGtfsrtCompressionLevel
	}
	return *c.GtfsrtCompressionLevel
}

// Bucket describes an object storage bucket the pipeline writes to.
type Bucket struct {
	// Identifier for the bucket, used in logs, metrics and the status page. Must be unique.
	//
	// This is needed because buckets with different providers can have the same name.
	Id string

	// Whether this is the primary bucket.
	//
	// The metadata in the primary bucket is the source of truth for which days have been processed.
	Primary bool

	// URL of the remote object storage service hosting the bucket.
	Url string

	// Region of the bucket. If empty, us-east-1 is used.
	Region string

	// Access key for the bucket.
	AccessKey string

	// Secret key for the bucket.
	SecretKey string

	// Name of the bucket.
	Name string

	// Prefix to add to the object key of all objects stored in the bucket.
	Prefix string

	// Canned ACL to set on objects written to the bucket, e.g. "public-read".
	// If empty, no ACL is set. This must be empty for buckets that don't support ACLs, like Cloudflare R2.
	Acl string
}

// Parse parses and validates an ETL config.
//
// The config is YAML. Because JSON is a subset of YAML, JSON configs are also accepted.
// Keys are matched to fields case-insensitively, so both `remotePrefix` and `RemotePrefix` work.
func Parse(b []byte) (*Config, error) {
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) validate() error {
	if l := c.GtfsrtCompressionLevel; l != nil && (*l < 0 || *l > 9) {
		return fmt.Errorf("GtfsrtCompressionLevel must be between 0 and 9 (got %d)", *l)
	}
	if len(c.Buckets) == 0 {
		return fmt.Errorf("no buckets configured")
	}
	numPrimaries := 0
	seen := map[[2]string]bool{}
	seenIds := map[string]bool{}
	for i, bucket := range c.Buckets {
		if bucket.Id == "" || bucket.Url == "" || bucket.Name == "" {
			return fmt.Errorf("bucket %d: Id, Url and Name must all be set", i)
		}
		if seenIds[bucket.Id] {
			return fmt.Errorf("bucket ID %q appears more than once", bucket.Id)
		}
		seenIds[bucket.Id] = true
		key := [2]string{bucket.Url, bucket.Name}
		if seen[key] {
			return fmt.Errorf("bucket %s/%s appears more than once", bucket.Url, bucket.Name)
		}
		seen[key] = true
		if bucket.Primary {
			numPrimaries++
		}
	}
	if numPrimaries != 1 {
		return fmt.Errorf("exactly one bucket must be marked as primary (got %d)", numPrimaries)
	}
	return nil
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

type Timezone struct {
	name string
	loc  *time.Location
}

func (t Timezone) AsLoc() *time.Location {
	return t.loc
}

func (t *Timezone) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &t.name); err != nil {
		return err
	}
	var err error
	t.loc, err = time.LoadLocation(t.name)
	if err != nil {
		return err
	}
	return nil
}

func (t Timezone) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.name)
}

// FeedsForDay returns the IDs of the feeds that should be processed for the provided day.
func FeedsForDay(feeds []Feed, day metadata.Day) []string {
	var result []string
	for _, feed := range feeds {
		if day.Before(feed.FirstDay) {
			continue
		}
		if feed.LastDay != nil && feed.LastDay.Before(day) {
			continue
		}
		result = append(result, feed.Id)
	}
	return result
}

func CalculatePendingDays(feeds []Feed, processedDays []metadata.ProcessedDay, lastDay metadata.Day, softwareVersion int) []PendingDay {
	upperBound := lastDay.Next()

	dayToRequiredFeeds := map[metadata.Day][]string{}
	for _, feed := range feeds {
		upperBound := upperBound
		if feed.LastDay != nil && feed.LastDay.Before(upperBound) {
			upperBound = feed.LastDay.Next()
		}
		firstDay := feed.FirstDay
		for firstDay.Before(upperBound) {
			dayToRequiredFeeds[firstDay] = append(dayToRequiredFeeds[firstDay], feed.Id)
			firstDay = firstDay.Next()
		}
	}

	dayToProcessedDay := map[metadata.Day]metadata.ProcessedDay{}
	for _, processedDay := range processedDays {
		dayToProcessedDay[processedDay.Day] = processedDay
	}

	result := []PendingDay{}
	for day, requiredFeeds := range dayToRequiredFeeds {
		requiredFeeds := requiredFeeds
		processedDay := dayToProcessedDay[day]
		if processedDay.SoftwareVersion < softwareVersion || !contains(processedDay.Feeds, requiredFeeds) {
			result = append(result, PendingDay{
				Day:     day,
				FeedIDs: requiredFeeds,
			})
		}
	}

	sortProcessedDays(result)
	return result
}

// contains checks if every element of the subset is contained in the superset
func contains(superset, subset []string) bool {
	supersetS := map[string]bool{}
	for _, supersetElem := range superset {
		supersetS[supersetElem] = true
	}
	for _, subsetElem := range subset {
		if !supersetS[subsetElem] {
			return false
		}
	}
	return true
}

func sortProcessedDays(in []PendingDay) {
	sort.Sort(sort.Reverse(byDay(in)))
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
