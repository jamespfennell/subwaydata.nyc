package metadata

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// TODO: get rid of all the camelCase garbage
type Metadata struct {
	StartDay    Day       `json:"startDay"`
	LastUpdated time.Time `json:"lastUpdated"`
	Feeds       []struct {
		Id       string
		StartDay Day  `json:"startDay"`
		EndDay   *Day `json:"endDay"`
	} `json:"feeds"`
	ProcessedDays []ProcessedDay `json:"processedDays"`
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
		return Day{}, fmt.Errorf("day %q not in the form YYYY-MM-DD: %w", s, err)
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

func (d Day) YearString() string {
	return fmt.Sprintf("%04d", d.year)
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

// TODO:remove
func (c *Metadata) LastAvailableDay() ProcessedDay {
	return c.RecentAvailableDay(0)
}

// TODO:remove
func (c *Metadata) RecentAvailableDay(i int) ProcessedDay {
	return c.ProcessedDays[len(c.ProcessedDays)-i-1]
}

//go:embed *json
var buildTimeConfigFiles embed.FS

// TODO: better name
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
