package export

import (
	"archive/tar"
	"bytes"
	_ "embed"

	"github.com/jamespfennell/gtfs/journal"
	"github.com/jamespfennell/xz"
)

// Export exports the provided journal as a tar.xz archive of csv files.
func Export(j *journal.Journal, filePrefix string) ([]byte, error) {
	csvExport, err := j.ExportToCsv()
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
		{"trips.csv", csvExport.TripsCsv},
		{"stop_times.csv", csvExport.StopTimesCsv},
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
