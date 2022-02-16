package pipeline

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/jamespfennell/hoard"
	hconfig "github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/export"
	"github.com/jamespfennell/subwaydata.nyc/etl/journal"
	"github.com/jamespfennell/subwaydata.nyc/etl/storage"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/jamespfennell/xz"
)

const softwareVersion = 2

type BacklogOptions struct {
	Limit  *int
	DryRun bool
}

// Backlog runs the ETL pipeline for all days in the backlog.
func Backlog(ec *config.Config, hc *hconfig.Config, sc *storage.Client, opts BacklogOptions) error {
	now := time.Now().In(ec.Timezone.AsLoc()).Add(-29 * time.Hour).Format("2006-01-02")
	endDay, _ := metadata.ParseDay(now)

	m, err := sc.GetMetadata()
	if err != nil {
		return fmt.Errorf("failed to obtain metadata: %w", err)
	}

	pendingDays := config.CalculatePendingDays(ec.Feeds, m.ProcessedDays, endDay, softwareVersion)
	if len(pendingDays) == 0 {
		log.Println("No days in the backlog")
		return nil
	}
	log.Printf("%d days in the backlog:\n", len(pendingDays))
	for i, pendingDay := range pendingDays {
		if opts.Limit != nil && *opts.Limit <= i {
			fmt.Println("Reached limit, ending...")
			break
		}
		log.Printf("Processing backlog for %s\n", pendingDay.Day)
		if opts.DryRun {
			continue
		}
		err := Run(
			pendingDay.Day,
			pendingDay.FeedIDs,
			ec,
			hc,
			sc,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// Run runs the ETL pipeline for the provided day.
func Run(day metadata.Day, feedIDs []string, ec *config.Config, hc *hconfig.Config, sc *storage.Client) error {
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("subwaydatanyc_%s_*", day))
	if err != nil {
		return fmt.Errorf("failed to create temporary working directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	start := day.Start(ec.Timezone.AsLoc())
	end := day.End(ec.Timezone.AsLoc())

	// Stage one: download the data from Hoard
	availableFeedIDs := map[string]bool{}
	for _, feed := range hc.Feeds {
		availableFeedIDs[feed.ID] = true
	}
	var feeds []hconfig.Feed
	for _, feedID := range feedIDs {
		if !availableFeedIDs[feedID] {
			return fmt.Errorf("feed %q does not appear in the Hoard config", feedID)
		}
		feeds = append(feeds, hconfig.Feed{
			ID: feedID,
		})
	}
	err = hoard.Retrieve(
		&hconfig.Config{
			Feeds:         feeds,
			ObjectStorage: hc.ObjectStorage,
		},
		hoard.RetrieveOptions{
			Path:            tmpDir,
			KeepPacked:      false,
			FlattenTimeDirs: true,
			FlattenFeedDirs: false,
			Start:           day.Start(ec.Timezone.AsLoc()).Add(-4 * time.Hour),
			End:             day.End(ec.Timezone.AsLoc()).Add(4 * time.Hour),
		},
	)
	if err != nil {
		return err
	}

	// Stage two: run the journal code on each directory of downloaded data.
	var allTrips []journal.Trip
	for _, feedID := range feedIDs {
		source, err := journal.NewDirectoryGtfsrtSource(filepath.Join(tmpDir, feedID))
		if err != nil {
			return err
		}
		j := journal.BuildJournal(
			source,
			day.Start(ec.Timezone.AsLoc()),
			day.End(ec.Timezone.AsLoc()),
		)
		allTrips = append(allTrips, j.Trips...)
	}

	// Stage three: export all of the trips.
	log.Printf("Creating CSV export")
	csvBytes, err := export.AsCsv(allTrips, fmt.Sprintf("%s%s_", ec.RemotePrefix, day))
	if err != nil {
		return fmt.Errorf("failed to export trips to CSV: %w", err)
	}

	// Stage four: create the tar xz of GTFS files.
	log.Printf("Creating GTFS-RT export")
	gtfsrtBytes, err := createGtfsrtExport(start, end, tmpDir, feedIDs)
	if err != nil {
		return fmt.Errorf("failed to create GTFS-RT export: %w", err)
	}

	// Stage five: upload data to object storage.
	csvSha256, err := calculateSha256(csvBytes)
	if err != nil {
		return fmt.Errorf("failed to calculate SHA-256 hash of CSV upload: %w", err)
	}
	target := fmt.Sprintf("%s/%s%s_%s_%s.tar.xz", day.MonthString(), ec.RemotePrefix, day, "csv", csvSha256)
	if err := sc.Write(csvBytes, target); err != nil {
		return fmt.Errorf("failed to copy csv bytes to object storage: %w", err)
	}

	gtfsrtSha256, err := calculateSha256(gtfsrtBytes)
	if err != nil {
		return fmt.Errorf("failed to calculate SHA-256 hash of GTFS-RT upload: %w", err)
	}
	gtfsrtTarget := fmt.Sprintf("%s/%s%s_%s_%s.tar.xz", day.MonthString(), ec.RemotePrefix, day, "gtfsrt", gtfsrtSha256)
	if err := sc.Write(gtfsrtBytes, gtfsrtTarget); err != nil {
		return fmt.Errorf("failed to copy gtfsrt to object storage: %w", err)
	}

	// Stage six: update the metadata in Git.
	newProcessedDay := metadata.ProcessedDay{
		Day:             day,
		Feeds:           feedIDs,
		Created:         time.Now(),
		SoftwareVersion: softwareVersion,
		Csv: metadata.Artifact{
			Size:     int64(len(csvBytes)),
			Path:     target,
			Checksum: csvSha256,
		},
		Gtfsrt: metadata.Artifact{
			Size:     int64(len(gtfsrtBytes)),
			Path:     gtfsrtTarget,
			Checksum: gtfsrtSha256,
		},
	}
	if err := sc.UpdateMetadata(
		func(m *metadata.Metadata) bool {
			for i := range m.ProcessedDays {
				if m.ProcessedDays[i].Day == day {
					if m.ProcessedDays[i].SoftwareVersion > softwareVersion {
						log.Printf("Not updating Git metadata: existing data built with newer software")
						return false
					}
					m.ProcessedDays[i] = newProcessedDay
					return true
				}
			}
			m.ProcessedDays = append(m.ProcessedDays, newProcessedDay)
			// TODO: sort the processed days in the git module - not responsiblity here
			return true
		},
	); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}
	return nil
}

func calculateSha256(b []byte) (string, error) {
	h := sha256.New()
	h.Write(b)
	return fmt.Sprintf("%x", h.Sum(nil))[:12], nil
}

func createGtfsrtExport(start, end time.Time, sourceDir string, feedIDs []string) ([]byte, error) {
	var gtfsrtTarXz bytes.Buffer
	xw := xz.NewWriter(&gtfsrtTarXz)
	tw := tar.NewWriter(xw)
	for _, feedID := range feedIDs {
		files, err := os.ReadDir(filepath.Join(sourceDir, feedID))
		if err != nil {
			return nil, err
		}
		now := time.Now()
		for i, file := range files {
			if time.Since(now) > time.Second {
				fmt.Printf("GTFS-RT export for feed %s: processed %d/%d files\n", feedID, i, len(files))
				now = time.Now()
			}
			info, err := file.Info()
			if err != nil {
				return nil, err
			}
			if info.ModTime().Before(start) || end.Before(info.ModTime()) {
				continue
			}
			hdr := &tar.Header{
				Name:       file.Name(),
				Mode:       0600,
				Size:       info.Size(),
				ModTime:    info.ModTime(),
				AccessTime: info.ModTime(),
				ChangeTime: info.ModTime(),
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return nil, err
			}
			f, err := os.Open(filepath.Join(sourceDir, feedID, file.Name()))
			if err != nil {
				return nil, err

			}
			if _, err := io.Copy(tw, f); err != nil {
				_ = f.Close()
				return nil, err
			}
			if err := f.Close(); err != nil {
				return nil, err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := xw.Close(); err != nil {
		return nil, err
	}
	return gtfsrtTarXz.Bytes(), nil
}
