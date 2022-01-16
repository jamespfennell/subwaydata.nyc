package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/updater/export"
	"github.com/jamespfennell/subwaydata.nyc/updater/gtfsrt"
	"github.com/jamespfennell/subwaydata.nyc/updater/journal"
	"github.com/jamespfennell/xz"
	"google.golang.org/protobuf/proto"
)

func main() {
	// Operations:
	//   - Run over directory, output locally
	//   - Run over date with data from Hoard, upload to object storage, update git
	//   - Run every night and process relevant days and feeds (with limit)

	err := run()
	if err != nil {
		log.Fatalf("Error: %s\n", err)
	}
	fmt.Println("Done")
}

var americaNewYorkTimezone *time.Location

func init() {
	var err error
	americaNewYorkTimezone, err = time.LoadLocation("America/New_York")
	if err != nil {
		panic(fmt.Errorf("failed to load America/New_York timezone: %w", err))
	}
}

func run() error {
	source, err := newDirectoryGtfsrtSource("../tmp/data/nycsubway_L/")
	if err != nil {
		return err
	}
	j := journal.BuildJournal(
		source,
		time.Date(2021, time.September, 16, 0, 0, 0, 0, americaNewYorkTimezone),
		time.Date(2021, time.September, 17, 0, 0, 0, 0, americaNewYorkTimezone),
	)
	fmt.Printf("%d trips", len(j.Trips))
	csvExport, err := export.AsCsv(j.Trips, "2021-09-16_")
	if err != nil {
		return err
	}
	csvExportXz, err := compress(csvExport)
	if err != nil {
		return err
	}
	if err := os.WriteFile("../tmp/2021-09-16_csv.tar.xz", csvExportXz, 0600); err != nil {
		return err
	}
	return nil
}

func compress(in []byte) ([]byte, error) {
	var out bytes.Buffer
	w := xz.NewWriter(&out)
	if _, err := w.Write(in); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

type directoryGtfsrtSource struct {
	baseDir   string
	fileNames []string
}

func newDirectoryGtfsrtSource(baseDir string) (*directoryGtfsrtSource, error) {
	files, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	source := &directoryGtfsrtSource{
		baseDir: baseDir,
	}
	for i, file := range files {
		if i > 2000 {
			break
		}
		source.fileNames = append(source.fileNames, file.Name())
	}
	sort.Strings(source.fileNames)
	return source, nil
}

func (s *directoryGtfsrtSource) Next() *gtfsrt.FeedMessage {
	for {
		if len(s.fileNames) == 0 {
			return nil
		}
		filePath := s.baseDir + s.fileNames[0]
		s.fileNames = s.fileNames[1:]
		b, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Printf("Failed to read %s: %s\n", filePath, err)
			continue
		}
		feedMessage := &gtfsrt.FeedMessage{}
		if err := proto.Unmarshal(b, feedMessage); err != nil {
			fmt.Printf("Failed to parse %s as a GTFS Realtime message: %s\n", filePath, err)
			continue
		}
		return feedMessage
	}
}
