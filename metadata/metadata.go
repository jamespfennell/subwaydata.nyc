package metadata

import (
	"encoding/json"
	"fmt"
	"time"
)

type Metadata struct {
	ProcessedDays []ProcessedDay
}

type Day struct {
	year  int
	month time.Month
	day   int
}

type ProcessedDay struct {
	Day             Day
	Feeds           []string
	Created         time.Time
	SoftwareVersion int
	Csv             Artifact
	Gtfsrt          Artifact
}

type Artifact struct {
	Size     int64
	Path     string
	Checksum string
}

func NewDay(year int, month time.Month, day int) Day {
	d, err := ParseDay(Day{year: year, month: month, day: day}.String())
	if err != nil {
		panic(err)
	}
	return d
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

func (d Day) YearString() string {
	return fmt.Sprintf("%04d", d.year)
}

func (d Day) MonthString() string {
	return fmt.Sprintf("%04d-%02d", d.year, d.month)
}

func (d Day) Format(layout string) string {
	return time.Date(d.year, d.month, d.day, 12, 0, 0, 0, time.UTC).Format(layout)
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
