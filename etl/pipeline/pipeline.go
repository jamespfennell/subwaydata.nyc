package pipeline

import (
	"archive/tar"
	"context"
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
	"github.com/jamespfennell/subwaydata.nyc/etl/git"
	"github.com/jamespfennell/subwaydata.nyc/etl/journal"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/jamespfennell/xz"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const softwareVersion = 1

// Run runs the subwaydata.nyc ETL pipeline for the provided day.
func Run(gitSession *git.Session, day metadata.Day, feedIDs []string, config *config.Config, hoardConfig *hconfig.Config) error {
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("subwaydatanyc_%s_*", day))
	if err != nil {
		return fmt.Errorf("failed to create temporary working directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	loc, err := time.LoadLocation(config.Timezone)
	if err != nil {
		return fmt.Errorf("unable to load timezone %q: %w", config.Timezone, err)
	}
	start := day.Start(loc)
	end := day.End(loc)

	// Stage one: download the data from Hoard
	availableFeedIDs := map[string]bool{}
	for _, feed := range hoardConfig.Feeds {
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
			ObjectStorage: hoardConfig.ObjectStorage,
		},
		hoard.RetrieveOptions{
			Path:            tmpDir,
			KeepPacked:      false,
			FlattenTimeDirs: true,
			FlattenFeedDirs: false,
			Start:           day.Start(loc).Add(-4 * time.Hour),
			End:             day.End(loc).Add(4 * time.Hour),
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
			day.Start(loc),
			day.End(loc),
		)
		allTrips = append(allTrips, j.Trips...)
	}

	// Stage three: export all of the trips.
	log.Printf("Creating CSV export")
	b, err := export.AsCsv(allTrips, fmt.Sprintf("%s%s_", config.RemotePrefix, day))
	if err != nil {
		return fmt.Errorf("failed to export trips to CSV: %w", err)
	}
	csvTmpFile := filepath.Join(tmpDir, "csv.tar.xz")
	if err := os.WriteFile(csvTmpFile, b, 0666); err != nil {
		return fmt.Errorf("failed to write CSV export: %w", err)
	}

	// Stage four: create the tar xz of GTFS files.
	log.Printf("Creating GTFS-RT export")
	gtfsTmpFile := filepath.Join(tmpDir, "gtfsrt.tar.xz")
	if err := createGtfsrtExport(start, end, tmpDir, feedIDs, gtfsTmpFile); err != nil {
		return fmt.Errorf("failed to create GTFS-RT export: %w", err)
	}

	// Stage five: upload local files to object storage.
	csvSha256, err := calculateSha256(csvTmpFile)
	if err != nil {
		return fmt.Errorf("failed to calculate SHA-256 hash of CSV upload: %w", err)
	}
	csvFileSize, err := getFileSize(csvTmpFile)
	if err != nil {
		return fmt.Errorf("failed to get size of CSV upload: %w", err)
	}
	target := fmt.Sprintf("%s/%s%s_%s_%s.tar.xz", day.MonthString(), config.RemotePrefix, day, "csv", csvSha256)
	if err := copyToObjectStorage(csvTmpFile, target, config); err != nil {
		return fmt.Errorf("failed to copy %s to object storage: %w", csvTmpFile, err)
	}

	gtfsrtSha256, err := calculateSha256(gtfsTmpFile)
	if err != nil {
		return fmt.Errorf("failed to calculate SHA-256 hash of GTFS-RT upload: %w", err)
	}
	gtfsrtFileSize, err := getFileSize(gtfsTmpFile)
	if err != nil {
		return fmt.Errorf("failed to get size of GTFS-RT upload: %w", err)
	}
	gtfsrtTarget := fmt.Sprintf("%s/%s%s_%s_%s.tar.xz", day.MonthString(), config.RemotePrefix, day, "gtfsrt", gtfsrtSha256)
	if err := copyToObjectStorage(gtfsTmpFile, gtfsrtTarget, config); err != nil {
		return fmt.Errorf("failed to copy %s to object storage: %w", gtfsTmpFile, err)
	}

	// Stage six: update the metadata in Git.
	newProcessedDay := metadata.ProcessedDay{
		Day:             day,
		Feeds:           feedIDs,
		Created:         time.Now(),
		SoftwareVersion: softwareVersion,
		Csv: metadata.Artifact{
			Size:     csvFileSize,
			Path:     target,
			Checksum: csvSha256,
		},
		Gtfsrt: metadata.Artifact{
			Size:     gtfsrtFileSize,
			Path:     gtfsrtTarget,
			Checksum: gtfsrtSha256,
		},
	}
	if err := gitSession.UpdateMetadata(
		func(m *metadata.Metadata) (bool, string) {
			for i := range m.ProcessedDays {
				if m.ProcessedDays[i].Day == day {
					if m.ProcessedDays[i].SoftwareVersion > softwareVersion {
						log.Printf("Not updating Git metadata: existing data built with newer software")
						return false, ""
					}
					m.ProcessedDays[i] = newProcessedDay
					return true, fmt.Sprintf("Updated data for %s", day)
				}
			}
			m.ProcessedDays = append(m.ProcessedDays, newProcessedDay)
			// TODO: sort the processed days in the git module - not responsiblity here
			return true, fmt.Sprintf("New data for %s", day)
		},
	); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}
	return nil
}

func copyToObjectStorage(localPath string, remotePath string, ec *config.Config) error {
	client, err := minio.New(ec.BucketUrl, &minio.Options{
		Creds:  credentials.NewStaticV4(ec.BucketAccessKey, ec.BucketSecretKey, ""),
		Secure: true,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize object storage client: %w", err)
	}
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file %s: %w", localPath, err)
	}
	s, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat local file %s: %w", localPath, err)
	}
	defer file.Close()
	// TODO: accept a context in the function
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().UTC().Add(30*time.Second))
	defer cancel()
	_, err = client.PutObject(
		ctx,
		ec.BucketName,
		filepath.Join(ec.BucketPrefix, remotePath),
		file,
		s.Size(),
		minio.PutObjectOptions{
			PartSize: 1024 * 1024 * 30, // TODO: good choice here?
		},
	)
	if err != nil {
		return fmt.Errorf("failed to copy bytes to object storage: %w", err)
	}
	return nil
}

func calculateSha256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:12], nil
}

func getFileSize(filePath string) (int64, error) {
	finfo, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return finfo.Size(), nil
}

func createGtfsrtExport(start, end time.Time, sourceDir string, feedIDs []string, target string) error {
	gtfsrtTarXz, err := os.Create(target)
	if err != nil {
		return fmt.Errorf("failed to open %q for writing: %w", target, err)
	}
	defer gtfsrtTarXz.Close()
	xw := xz.NewWriter(gtfsrtTarXz)
	tw := tar.NewWriter(xw)
	for _, feedID := range feedIDs {
		files, err := os.ReadDir(filepath.Join(sourceDir, feedID))
		if err != nil {
			return err
		}
		now := time.Now()
		for i, file := range files {
			if time.Since(now) > time.Second {
				fmt.Printf("GTFS-RT export for feed %s: processed %d/%d files\n", feedID, i, len(files))
				now = time.Now()
			}
			info, err := file.Info()
			if err != nil {
				return err
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
				return err
			}
			f, err := os.Open(filepath.Join(sourceDir, feedID, file.Name()))
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, f); err != nil {
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := xw.Close(); err != nil {
		return err
	}
	return nil
}
