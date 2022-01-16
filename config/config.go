package config

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type Config struct {
	StartDay    Day       `json:"startDay"`
	LastUpdated time.Time `json:"lastUpdated"`
	Feeds       []struct {
		Id       string
		StartDay Day  `json:"startDay"`
		EndDay   *Day `json:"endDay"`
	}
	ProcessedDays []ProcessedDay `json:"processedDays"`
}

type Day time.Time

type ProcessedDay struct {
	Day             Day
	Feeds           []string
	Created         time.Time
	SoftwareVersion int `json:"softwareVersion"`
	Csv             Artifact
	Gtfsrt          Artifact
}

type Artifact struct {
	Size        int
	Url         string
	Md5Checksum string
}

func (d *Day) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	location, _ := time.LoadLocation("EST")
	t, err := time.ParseInLocation("2006-01-02", s, location)
	if err != nil {
		return fmt.Errorf("day %q not in the form YYYY-MM-DD: %w", s, err)
	}
	*d = Day(t)
	return nil
}

func (d Day) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.String())), nil
}

func (d Day) AsTime() time.Time {
	return time.Time(d)
}

func (d Day) String() string {
	return d.AsTime().Format("2006-01-02")
}

func (d Day) YearString() string {
	return d.AsTime().Format("2006")
}

func (d Day) MonthString() string {
	return d.AsTime().Format("2006-01")
}

func (c *Config) LastAvailableDay() ProcessedDay {
	return c.RecentAvailableDay(0)
}

func (c *Config) RecentAvailableDay(i int) ProcessedDay {
	return c.ProcessedDays[len(c.ProcessedDays)-i-1]
}

//go:embed *json
var buildTimeConfigFiles embed.FS

// TODO: better name
type Provider struct {
	configFiles map[string]*Config
	m           sync.RWMutex
}

func NewProvider() *Provider {
	p := Provider{configFiles: map[string]*Config{}}
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
		var c Config
		if err := json.Unmarshal(b, &c); err != nil {
			panic(fmt.Sprintf("Failed to parse build time config file %q: %s", file.Name(), err))
		}
		log.Printf("Read build time config file with id %q\n", id)
		p.configFiles[id] = &c
		fmt.Println(c)
	}
	return &p
}

func (p *Provider) Config(id string) *Config {
	p.m.RLock()
	defer p.m.RUnlock()
	return p.configFiles[id]
}
