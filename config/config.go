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
	return []byte(d.String()), nil
}

func (d Day) String() string {
	return time.Time(d).Format("2006-01-02")
}

type Config struct {
	Id          string
	Name string
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
	return c.AvailableDays[len(c.AvailableDays)-1]
}

type AvailableDay struct {
	Day Day
	LastUpdated time.Time `json:"last_updated"`
	Sizes struct {
		Csv int
		Sql int
		Gtfsrt int
	}
}

//go:embed *json
var buildTimeConfigFiles embed.FS

// TODO: better name
type Provider struct {
	configFiles map[string]*Config
	m sync.RWMutex
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