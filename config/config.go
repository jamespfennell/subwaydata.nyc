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

type Day time.Time

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

type Bytes int64

func (b Bytes) String() string {
	if b < 1000 {
		return fmt.Sprintf("%db", b)
	}
	div, exp := Bytes(1000), Bytes(0)
	for n := b / 1000; n >= 1000; n /= 1000 {
		div *= 1000
		exp++
	}
	raw := b/div + 1
	if raw >= 100 {
		return fmt.Sprintf("0.%d%cb", raw/100+1, "kmgt"[exp+1])
	}
	return fmt.Sprintf("%d%cb", b/div+1, "kmg"[exp])
}

type Config struct {
	Id          string
	Name        string
	StartDay    Day       `json:"start_day"`
	LastUpdated time.Time `json:"last_updated"`
	Feeds       []struct {
		Id       string
		StartDay Day  `json:"start_day"`
		EndDay   *Day `json:"end_day"`
	}
	AvailableDays []AvailableDay `json:"available_days"`
}

func (c *Config) LastAvailableDay() AvailableDay {
	return c.RecentAvailableDay(0)
}

func (c *Config) RecentAvailableDay(i int) AvailableDay {
	return c.AvailableDays[len(c.AvailableDays)-i-1]
}

type AvailableDay struct {
	Day         Day
	LastUpdated time.Time `json:"last_updated"`
	Sizes       struct {
		Csv    Bytes
		Sql    Bytes
		Gtfsrt Bytes
	}
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
