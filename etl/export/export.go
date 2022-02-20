package export

import (
	"archive/tar"
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
	"time"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/subwaydata.nyc/etl/journal"
	"github.com/jamespfennell/xz"
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
	"FormatDirectionID": func(d gtfs.DirectionID) string {
		switch d {
		case gtfs.DirectionIDFalse:
			return "0"
		case gtfs.DirectionIDTrue:
			return "1"
		default:
			return ""
		}
	},
}

var tripsCsv *template.Template = template.Must(template.New("trips.csv.tmpl").Funcs(funcMap).Parse(tripsCsvTmpl))
var stopTimesCsv *template.Template = template.Must(template.New("stop_times.csv.tmpl").Funcs(funcMap).Parse(stopTimesCsvTmpl))

func AsCsv(trips []journal.Trip) ([]byte, []byte, error) {
	var tripsB bytes.Buffer
	err := tripsCsv.Execute(&tripsB, trips)
	if err != nil {
		return nil, nil, err
	}

	var stopTimesB bytes.Buffer
	err = stopTimesCsv.Execute(&stopTimesB, trips)
	if err != nil {
		return nil, nil, err
	}
	return tripsB.Bytes(), stopTimesB.Bytes(), nil
}

// AsCsvTarXz exports the provided trips as a tar.xz archive of csv files.
func AsCsvTarXz(trips []journal.Trip, filePrefix string) ([]byte, error) {
	tripsB, stopTimesB, err := AsCsv(trips)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	xw := xz.NewWriter(&out)
	tw := tar.NewWriter(xw)
	var files = []struct {
		Name string
		Body []byte
	}{
		{"trips.csv", tripsB},
		{"stop_times.csv", stopTimesB},
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
	if err := xw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
