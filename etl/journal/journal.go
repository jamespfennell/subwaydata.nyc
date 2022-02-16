package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jamespfennell/gtfs"
)

type Journal struct {
	Trips []Trip
	// TODO: Metadata
	// TODO: filter on start_time
	// TODO: log an error if the trip passes the filter but was updated in the last update
	//if trip.NumScheduleChanges > 0 {
	//	a += 1
	//}
}

type Trip struct {
	TripUID     string
	TripID      string
	RouteID     string
	DirectionID gtfs.DirectionID
	StartTime   time.Time
	VehicleID   string

	StopTimes []StopTime

	// Metadata follows
	NumUpdates          int
	NumScheduleChanges  int
	NumScheduleRewrites int
}

type StopTime struct {
	StopID string
	// TODO: StopSequence
	ArrivalTime   *time.Time
	DepartureTime *time.Time
	Track         *string
}

type GtfrtSource interface {
	Next() *gtfs.Realtime
}

type DirectoryGtfsrtSource struct {
	baseDir   string
	fileNames []string
	startLen  int
	t         time.Time
}

func NewDirectoryGtfsrtSource(baseDir string) (*DirectoryGtfsrtSource, error) {
	files, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	source := &DirectoryGtfsrtSource{
		baseDir: baseDir,
		t:       time.Now(),
	}
	for _, file := range files {
		source.fileNames = append(source.fileNames, file.Name())
	}
	source.startLen = len(source.fileNames)
	sort.Strings(source.fileNames)
	return source, nil
}

func (s *DirectoryGtfsrtSource) Next() *gtfs.Realtime {
	for {
		if len(s.fileNames) == 0 {
			return nil
		}
		filePath := filepath.Join(s.baseDir, s.fileNames[0])
		s.fileNames = s.fileNames[1:]
		b, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Printf("Failed to read %s: %s\n", filePath, err)
			continue
		}
		result, err := gtfs.ParseRealtime(b, &gtfs.ParseRealtimeOptions{
			UseNyctExtension: true,
		})
		if err != nil {
			fmt.Printf("Failed to parse %s as a GTFS Realtime message: %s\n", filePath, err)
			continue
		}
		if time.Since(s.t) >= time.Second {
			fmt.Printf("Processed %d/%d files\n", s.startLen-len(s.fileNames), s.startLen)
			s.t = time.Now()
		}
		return result
	}
}

func BuildJournal(source GtfrtSource, startTime, endTime time.Time) *Journal {
	trips := map[string]*Trip{}
	i := 0
	for feedMessage := source.Next(); feedMessage != nil; feedMessage = source.Next() {
		for _, tripUpdate := range feedMessage.Trips {
			startTime := tripUpdate.ID.StartDate.Add(tripUpdate.ID.StartTime)
			tripUID := fmt.Sprintf("%d%s", startTime.Unix(), tripUpdate.ID.ID[6:])
			if existingTrip, ok := trips[tripUID]; ok {
				existingTrip.update(&tripUpdate)
			} else {
				trip := Trip{}
				trip.update(&tripUpdate)
				trips[tripUID] = &trip
			}
		}
		i++
	}
	var tripIDs []string
	for tripID, trip := range trips {
		if trip.StartTime.Before(startTime) || endTime.Before(trip.StartTime) {
			continue
		}
		tripIDs = append(tripIDs, tripID)
	}
	sort.Strings(tripIDs)
	j := &Journal{}
	for _, tripID := range tripIDs {
		j.Trips = append(j.Trips, *trips[tripID])
	}
	return j
}

func (trip *Trip) update(tripUpdate *gtfs.Trip) {
	startTime := tripUpdate.ID.StartDate.Add(tripUpdate.ID.StartTime)
	trip.TripUID = fmt.Sprintf("%d%s", startTime.Unix(), tripUpdate.ID.ID[6:])
	trip.TripID = tripUpdate.ID.ID
	trip.RouteID = tripUpdate.ID.RouteID
	trip.DirectionID = tripUpdate.ID.DirectionID
	trip.StartTime = tripUpdate.ID.StartDate.Add(tripUpdate.ID.StartTime)
	if tripUpdate.Vehicle != nil && tripUpdate.Vehicle.ID != nil {
		trip.VehicleID = tripUpdate.Vehicle.ID.ID
	}
	trip.NumUpdates += 1

	stopTimeUpdates := tripUpdate.StopTimeUpdates
	if len(stopTimeUpdates) == 0 {
		return
	}

	start := 0
	for i, stopTime := range trip.StopTimes {
		// TODO: doesn't this handle the case when the first stop has just been added to the schedule
		// Probably need a fancier algo for this
		if stopTime.StopID == *stopTimeUpdates[0].StopID {
			start = i
			break
		}
	}

	end := start
	for _, stopTimeUpdate := range stopTimeUpdates {
		if len(trip.StopTimes) >= end {
			break
		}
		if trip.StopTimes[end].StopID != *stopTimeUpdate.StopID {
			break
		}
		trip.StopTimes[end].update(&stopTimeUpdate)
		end++
	}

	trip.StopTimes = trip.StopTimes[:end]
	for _, stu := range stopTimeUpdates[end-start:] {
		stopTime := StopTime{}
		stopTime.update(&stu)
		trip.StopTimes = append(trip.StopTimes, stopTime)
	}
}

func (stopTime *StopTime) update(stopTimeUpdate *gtfs.StopTimeUpdate) {
	stopTime.StopID = *stopTimeUpdate.StopID
	if stopTimeUpdate.Arrival != nil {
		stopTime.ArrivalTime = stopTimeUpdate.Arrival.Time
	}
	if stopTimeUpdate.Departure != nil {
		stopTime.DepartureTime = stopTimeUpdate.Departure.Time
	}
	track := ""
	stopTime.Track = &track
}
