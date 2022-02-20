package html

import (
	"embed"
	"fmt"
	"html/template"
	"reflect"
	"strings"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/jamespfennell/subwaydata.nyc/website/static"
)

const dataBaseUrl = "https://data.subwaydata.nyc"

var americaNewYork *time.Location

func init() {
	var err error
	americaNewYork, err = time.LoadLocation("America/New_York")
	if err != nil {
		panic("failed to load America/New_York timezone")
	}
}

func Home(m *metadata.Metadata) string {
	if m == nil {
		m = &metadata.Metadata{}
	}
	var firstDay metadata.Day
	var mostRecentDay metadata.Day
	var lastUpdated time.Time
	for i, processedDay := range m.ProcessedDays {
		if i == 0 || lastUpdated.Before(processedDay.Created) {
			lastUpdated = processedDay.Created
		}
		if i == 0 || processedDay.Day.Before(firstDay) {
			firstDay = processedDay.Day
		}
		if i == 0 || mostRecentDay.Before(processedDay.Day) {
			mostRecentDay = processedDay.Day
		}
	}
	input := struct {
		FirstDay      string
		MostRecentDay string
		NumDays       int
		LastUpdated   string
		StaticFiles   static.Files
	}{
		FirstDay:      firstDay.Format("January 2, 2006"),
		MostRecentDay: mostRecentDay.Format("January 2, 2006"),
		NumDays:       len(m.ProcessedDays),
		LastUpdated:   lastUpdated.In(americaNewYork).Format("January 2, 2006 at 15:04"),
		StaticFiles:   static.Get(),
	}
	var s strings.Builder
	if err := t.Home.Execute(&s, input); err != nil {
		panic(err)
	}
	return s.String()
}

type year struct {
	Title  string
	Months []month
}

type month struct {
	Title string
	Name  string
	Days  []dayData
}

type dayData struct {
	Title      string
	CsvUrl     string
	CsvSize    string
	GtfsrtUrl  string
	GtfsrtSize string
	Updated    string
}

func ExploreTheData(m *metadata.Metadata) string {
	if m == nil {
		m = &metadata.Metadata{}
	}
	var years []*year
	for _, p := range m.ProcessedDays {
		p := p
		if len(years) == 0 || years[len(years)-1].Title != p.Day.Format("2006") {
			years = append(years, &year{
				Title: p.Day.Format("2006"),
			})
		}
		year := years[len(years)-1]
		j := len(year.Months) - 1
		if len(year.Months) == 0 || year.Months[j].Title != p.Day.Format("January 2006") {
			year.Months = append(year.Months, month{
				Title: p.Day.Format("January 2006"),
				Name:  p.Day.Format("January"),
			})
			j += 1
		}
		year.Months[j].Days = append(year.Months[j].Days, dayData{
			Title:      p.Day.Format("January 02, 2006"),
			CsvUrl:     fmt.Sprintf("%s/%s", dataBaseUrl, p.Csv.Path),
			CsvSize:    formatBytes(p.Csv.Size),
			GtfsrtUrl:  fmt.Sprintf("%s/%s", dataBaseUrl, p.Gtfsrt.Path),
			GtfsrtSize: formatBytes(p.Gtfsrt.Size),
			Updated:    p.Created.Format("January 02, 2006"),
		})
	}
	input := struct {
		Years       []*year
		StaticFiles static.Files
	}{
		Years:       years,
		StaticFiles: static.Get(),
	}
	var s strings.Builder
	if err := t.ExploreTheData.Execute(&s, input); err != nil {
		panic(err)
	}
	return s.String()
}

func ProgrammaticAccess() string {
	return executeStaticTemplate(t.ProgrammaticAccess)
}

func DataSchema() string {
	return executeStaticTemplate(t.DataSchema)
}

func HowItWorks() string {
	return executeStaticTemplate(t.HowItWorks)
}

func executeStaticTemplate(t *template.Template) string {
	input := struct {
		StaticFiles static.Files
	}{
		StaticFiles: static.Get(),
	}
	var s strings.Builder
	if err := t.Execute(&s, input); err != nil {
		panic(err)
	}
	return s.String()
}

func formatBytes(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

//go:embed *.html
var files embed.FS

const rootTemplate = "layout.html"

type Templates struct {
	Home               *template.Template `html:"home.html"`
	ExploreTheData     *template.Template `html:"explore-the-data.html"`
	ProgrammaticAccess *template.Template `html:"programmatic-access.html"`
	HowItWorks         *template.Template `html:"how-it-works.html"`
	DataSchema         *template.Template `html:"data-schema.html"`
	PageNotFound       *template.Template `html:"404.html"`
}

var t Templates

func GetTemplates() Templates {
	return t
}

func init() {
	rootB, err := files.ReadFile(rootTemplate)
	if err != nil {
		panic(fmt.Sprintf("Could not read the root template %s", rootTemplate))
	}
	str := reflect.TypeOf(t)
	for i := 0; i < str.NumField(); i++ {
		field := str.Field(i)
		path := field.Tag.Get("html")
		if path == "" {
			panic(fmt.Sprintf("Templates.%s does not have a path specified", field.Name))
		}
		b, err := files.ReadFile(path)
		if err != nil {
			panic(fmt.Sprintf("Templates.%s references a path %s that does not exist", field.Name, path))
		}
		tmpl := template.New(field.Name)
		tmpl = template.Must(tmpl.Parse(string(rootB)))
		tmpl = template.Must(tmpl.Parse(string(b)))
		reflect.ValueOf(&t).Elem().Field(i).Set(reflect.ValueOf(tmpl))
	}
}
