package export

import (
	"archive/tar"
	"bytes"
	"io"
	"log"
	"testing"
	"time"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/subwaydata.nyc/etl/journal"
	"github.com/jamespfennell/xz"
)

var trip journal.Trip = journal.Trip{
	TripUID:     "TripUID",
	TripID:      "TripID",
	RouteID:     "RouteID",
	DirectionID: gtfs.DirectionID_True,
	VehicleID:   "VehicleID",
	StartTime:   time.Unix(100, 0),
	StopTimes: []journal.StopTime{
		{
			StopID:        "StopID1",
			Track:         sptr("Track1"),
			ArrivalTime:   nil,
			DepartureTime: ptr(time.Unix(200, 0)),
			LastObserved:  time.Unix(200, 0),
			MarkedPast:    ptr(time.Unix(300, 0)),
		},
		{
			StopID:        "StopID2",
			ArrivalTime:   ptr(time.Unix(300, 0)),
			DepartureTime: ptr(time.Unix(400, 0)),
			LastObserved:  time.Unix(400, 0),
		},
		{
			StopID:        "StopID3",
			Track:         sptr("Track3"),
			ArrivalTime:   ptr(time.Unix(500, 0)),
			DepartureTime: nil,
			LastObserved:  time.Unix(400, 0),
		},
	},
	LastObserved:        time.Unix(400, 0),
	MarkedPast:          ptr(time.Unix(600, 0)),
	NumUpdates:          100,
	NumScheduleChanges:  2,
	NumScheduleRewrites: 1,
}

const expectedTripsCsv = `trip_uid,trip_id,route_id,direction_id,start_time,vehicle_id,last_observed,marked_past,num_updates,num_schedule_changes,num_schedule_rewrites
TripUID,TripID,RouteID,1,100,VehicleID,400,600,100,2,1
`

const expectedStopTimesCsv = `trip_uid,stop_id,track,arrival_time,departure_time,last_observed,marked_past
TripUID,StopID1,Track1,,200,200,300
TripUID,StopID2,,300,400,400,
TripUID,StopID3,Track3,500,,400,
`

func TestAsCsv(t *testing.T) {
	prefix := "somePrefix_"
	trips := []journal.Trip{trip}

	result, err := AsCsvTarXz(trips, prefix)
	if err != nil {
		t.Fatalf("AsCsv function failed: %s", err)
	}

	actualFiles := unTar(result)

	tripsCsv, ok := actualFiles[prefix+"trips.csv"]
	if !ok {
		t.Errorf("Did not find trips file in tar file")
	} else if tripsCsv != expectedTripsCsv {
		t.Errorf("Trips file actual:\n%s\n!= expected:\n%s\n", tripsCsv, expectedTripsCsv)
	}

	stopTimesCsv, ok := actualFiles[prefix+"stop_times.csv"]
	if !ok {
		t.Errorf("Did not find stop times file in tar file")
	} else if stopTimesCsv != expectedStopTimesCsv {
		t.Errorf("Stop times file actual:\n%s\n!= expected:\n%s\n", stopTimesCsv, expectedStopTimesCsv)
	}
}

func unTar(b []byte) map[string]string {
	result := map[string]string{}
	buf := bytes.NewBuffer(b)
	xr := xz.NewReader(buf)
	tr := tar.NewReader(xr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			log.Fatal(err)
		}
		result[hdr.Name] = string(b)
	}
	return result
}

func ptr(t time.Time) *time.Time {
	return &t
}

func sptr(s string) *string {
	return &s
}
