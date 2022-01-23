package metadata

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

// TODO: get rid of all the camelCase garbage
type Metadata struct {
	StartDay      Day            `json:"startDay"`
	LastUpdated   time.Time      `json:"lastUpdated"`
	Feeds         []FeedMetadata `json:"feeds"`
	ProcessedDays []ProcessedDay `json:"processedDays"`
}

type FeedMetadata struct {
	Id string

	// Maybe FirstDay and LastDay
	StartDay Day  `json:"startDay"`
	EndDay   *Day `json:"endDay"`
}

type Day struct {
	year  int
	month time.Month
	day   int
}

type ProcessedDay struct {
	Day             Day       `json:"day"`
	Feeds           []string  `json:"feeds"`
	Created         time.Time `json:"created"`
	SoftwareVersion int       `json:"softwareVersion"`
	Csv             Artifact  `json:"csv"`
	Gtfsrt          Artifact  `json:"gtfsrt"`
}

type Artifact struct {
	Size int64  `json:"size"`
	Url  string `json:"url"`

	// TODO: rename checksum
	Md5Checksum string `json:"md5Checksum"`
}

func ParseDay(s string) (Day, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return Day{}, fmt.Errorf("day %q not in the form YYYY-MM-DD", s)
	}
	return Day{
		year:  t.Year(),
		month: t.Month(),
		day:   t.Day(),
	}, nil
}

func (d *Day) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	dParsed, err := ParseDay(s)
	if err != nil {
		return err
	}
	*d = dParsed
	return nil
}

func (d Day) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.String())), nil
}

func (d Day) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.year, d.month, d.day)
}

func (d Day) MonthString() string {
	return fmt.Sprintf("%04d-%02d", d.year, d.month)
}

func (d Day) Start(loc *time.Location) time.Time {
	return time.Date(d.year, d.month, d.day, 0, 0, 0, 0, loc)
}

func (d Day) End(loc *time.Location) time.Time {
	return time.Date(d.year, d.month, d.day+1, 0, 0, 0, 0, loc)
}

func (d Day) Before(d2 Day) bool {
	if d.year != d2.year {
		return d.year < d2.year
	}
	if d.month != d2.month {
		return d.month < d2.month
	}
	return d.day < d2.day
}

func (d Day) Next() Day {
	t := time.Date(d.year, d.month, d.day, 12, 0, 0, 0, time.UTC).Add(24 * time.Hour)
	return Day{
		year:  t.Year(),
		month: t.Month(),
		day:   t.Day(),
	}
}

//go:embed *json
var buildTimeConfigFiles embed.FS

// TODO: destory
type Provider struct {
	configFiles map[string]*Metadata
	m           sync.RWMutex
}

func NewProvider() *Provider {
	p := Provider{configFiles: map[string]*Metadata{}}
	files, err := buildTimeConfigFiles.ReadDir(".")
	if err != nil {
		panic(fmt.Sprintf("Failed to read the build time config files: %s", err))
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(file.Name(), ".json")
		b, err := buildTimeConfigFiles.ReadFile(file.Name())
		if err != nil {
			panic(fmt.Sprintf("Failed to read build time config file %q: %s", file.Name(), err))
		}
		var c Metadata
		if err := json.Unmarshal(b, &c); err != nil {
			panic(fmt.Sprintf("Failed to parse build time config file %q: %s", file.Name(), err))
		}
		log.Printf("Read build time config file with id %q\n", id)
		p.configFiles[id] = &c
		fmt.Println(c)
	}
	return &p
}

func (p *Provider) Config(id string) *Metadata {
	p.m.RLock()
	defer p.m.RUnlock()
	return p.configFiles[id]
}

type PendingDay struct {
	Day     Day
	FeedIDs []string
}

func CalculatePendingDays(m *Metadata, lastDay Day) []PendingDay {
	upperBound := lastDay.Next()
	firstDay := upperBound
	for _, feed := range m.Feeds {
		if feed.StartDay.Before(firstDay) {
			firstDay = feed.StartDay
		}
	}

	dayToRequiredFeeds := map[Day][]string{}
	for _, feed := range m.Feeds {
		firstDay := firstDay
		upperBound := upperBound
		if feed.EndDay != nil && feed.EndDay.Before(upperBound) {
			upperBound = feed.EndDay.Next()
		}
		for firstDay.Before(upperBound) {
			dayToRequiredFeeds[firstDay] = append(dayToRequiredFeeds[firstDay], feed.Id)
			firstDay = firstDay.Next()
		}
	}

	dayToProcessedFeeds := map[Day][]string{}
	for _, processedDay := range m.ProcessedDays {
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
