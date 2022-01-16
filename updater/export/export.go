package export

import (
	"archive/tar"
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/updater/journal"
)

//go:embed trips.csv.tmpl
var tripsCsvTmpl string

//go:embed stop_times.csv.tmpl
var stopTimesCsvTmpl string

var funcMap = template.FuncMap{
	"NullableString": func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	},
	"NullableUnix": func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return fmt.Sprintf("%d", t.Unix())
	},
}

var tripsCsv *template.Template = template.Must(template.New("trips.csv.tmpl").Funcs(funcMap).Parse(tripsCsvTmpl))
var stopTimesCsv *template.Template = template.Must(template.New("stop_times.csv.tmpl").Funcs(funcMap).Parse(stopTimesCsvTmpl))

// AsCsv exports the provided trips as a tar archive of csv files.
func AsCsv(trips []journal.Trip, filePrefix string) ([]byte, error) {
	var tripsB bytes.Buffer
	err := tripsCsv.Execute(&tripsB, trips)
	if err != nil {
		return nil, err
	}

	var stopTimesB bytes.Buffer
	err = stopTimesCsv.Execute(&stopTimesB, trips)
	if err != nil {
		return nil, err
	}

	var tarB bytes.Buffer
	tw := tar.NewWriter(&tarB)
	var files = []struct {
		Name string
		Body []byte
	}{
		{"trips.csv", tripsB.Bytes()},
		{"stop_times.csv", stopTimesB.Bytes()},
		// TODO: add a readme
	}
	for _, file := range files {
		hdr := &tar.Header{
			Name: filePrefix + file.Name,
			Mode: 0600,
			Size: int64(len(file.Body)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write([]byte(file.Body)); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return tarB.Bytes(), nil
}
