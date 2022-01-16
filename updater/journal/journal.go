package journal

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/updater/gtfsrt"
	"google.golang.org/protobuf/proto"
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
	DirectionID bool
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
	Next() *gtfsrt.FeedMessage
}

// TODO: start time and end time
func BuildJournal(source GtfrtSource, startTime, endTime time.Time) *Journal {
	trips := map[string]*Trip{}
	i := 0
	for feedMessage := source.Next(); feedMessage != nil; feedMessage = source.Next() {
		groupedEntities := groupEntities(feedMessage)
		for tripUID, g := range groupedEntities {
			if existingTrip, ok := trips[tripUID]; ok {
				existingTrip.update(&g)
			} else {
				trip := buildTrip(&g)
				trips[tripUID] = &trip
			}
		}
		// fmt.Printf("Processed %d\n", i)
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

type groupedEntity struct {
	TripUID       string
	StartTime     time.Time
	NumDuplicates int
	TripUpdate    *gtfsrt.TripUpdate
	Vehicle       *gtfsrt.VehiclePosition
	NyctTrip      *gtfsrt.NyctTripDescriptor
}

func groupEntities(a *gtfsrt.FeedMessage) map[string]groupedEntity {
	result := map[string]groupedEntity{}

	directionIDNorth := uint32(0)
	directionIDSouth := uint32(1)
	for _, entity := range a.Entity {
		var trip *gtfsrt.TripDescriptor
		if tripUpdate := entity.TripUpdate; tripUpdate != nil {
			trip = tripUpdate.Trip
		} else if vehicle := entity.Vehicle; vehicle != nil {
			trip = vehicle.Trip
		} else {
			continue
		}

		nyctTripID, ok := parseNyctTripID(trip.GetTripId())
		if !ok {
			fmt.Printf("failed to parse NYCT trip ID %s\n", trip.GetTripId())
			continue
		}
		startTime, ok := buildStartTime(nyctTripID, trip.GetStartDate())
		if !ok {
			fmt.Printf("failed to parse start time for start_date=%s, nyct_trip_id=%+v\n", trip.GetStartDate(), nyctTripID)
			continue
		}
		tripUID := fmt.Sprintf("%d%s", startTime.Unix(), trip.GetTripId()[6:])

		g := groupedEntity{
			TripUID:       tripUID,
			StartTime:     startTime,
			NumDuplicates: 1,
		}
		if entity.TripUpdate != nil {
			g.TripUpdate = entity.TripUpdate
		}
		if entity.Vehicle != nil {
			g.Vehicle = entity.Vehicle
		}

		// NYCT extension logic
		extendedEvent := proto.GetExtension(trip, gtfsrt.E_NyctTripDescriptor)
		g.NyctTrip, _ = extendedEvent.(*gtfsrt.NyctTripDescriptor)
		if !g.NyctTrip.GetIsAssigned() {
			continue
		}
		if g.NyctTrip.GetDirection() == gtfsrt.NyctTripDescriptor_NORTH {
			trip.DirectionId = &directionIDNorth
		} else {
			trip.DirectionId = &directionIDSouth
		}

		if otherG, ok := result[tripUID]; ok {
			g.NumDuplicates = otherG.NumDuplicates + 1
			if g.TripUpdate == nil {
				g.TripUpdate = otherG.TripUpdate
			}
			if g.Vehicle == nil {
				g.Vehicle = otherG.Vehicle
			}
		}

		result[tripUID] = g
	}

	return result
}

func getTrack(stu *gtfsrt.TripUpdate_StopTimeUpdate) *string {
	extendedEvent := proto.GetExtension(stu, gtfsrt.E_NyctStopTimeUpdate)
	nyctStopTime, ok := extendedEvent.(*gtfsrt.NyctStopTimeUpdate)
	if !ok || nyctStopTime == nil {
		fmt.Println("Failed to read NYCT extension")
		return nil
	}
	if t := nyctStopTime.ActualTrack; t != nil {
		return t
	}
	return nyctStopTime.ScheduledTrack
}

func buildTrip(g *groupedEntity) Trip {
	trip := Trip{}
	trip.update(g)
	return trip
}

func convertRawDirectionId(r uint32) bool {
	return r == 0
}

func convertOptionalTime(t int64) *time.Time {
	if t == 0 {
		return nil
	}
	u := time.Unix(t, 0)
	return &u
}

var americaNewYorkTimezone *time.Location
var startDateRegex *regexp.Regexp = regexp.MustCompile(`^([0-9]{4})([0-9]{2})([0-9]{2})$`)
var tripIDRegex *regexp.Regexp = regexp.MustCompile(`^([0-9]{6})_([[:alnum:]]{1,2})..([SN])([[:alnum:]]*)$`)

func init() {
	var err error
	americaNewYorkTimezone, err = time.LoadLocation("America/New_York")
	if err != nil {
		panic(fmt.Errorf("failed to load America/New_York timezone: %w", err))
	}
}

type nyctTripID struct {
	secondsAfterMidnight int
	routeID              string
	directionID          bool
	pathID               string
}

func parseNyctTripID(rawTripID string) (nyctTripID, bool) {
	tripIDMatch := tripIDRegex.FindStringSubmatch(rawTripID)
	if tripIDMatch == nil {
		return nyctTripID{}, false
	}
	hundrethsOfMins, _ := strconv.Atoi(tripIDMatch[1])
	return nyctTripID{
		secondsAfterMidnight: (hundrethsOfMins * 6) / 10,
		routeID:              tripIDMatch[2],
		directionID:          tripIDMatch[3] == "S",
		pathID:               tripIDMatch[4],
	}, true
}

func buildStartTime(tripID nyctTripID, startDate string) (time.Time, bool) {
	startDateMatch := startDateRegex.FindStringSubmatch(startDate)
	if startDateMatch == nil {
		return time.Time{}, false
	}
	year, _ := strconv.Atoi(startDateMatch[1])
	month, _ := strconv.Atoi(startDateMatch[2])
	day, _ := strconv.Atoi(startDateMatch[3])
	secondsAfterMidnight := time.Duration(tripID.secondsAfterMidnight) * time.Second
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, americaNewYorkTimezone).Add(secondsAfterMidnight), true
}

func (trip *Trip) update(g *groupedEntity) {
	trip.TripUID = g.TripUID
	trip.TripID = g.TripUpdate.GetTrip().GetTripId()
	trip.RouteID = g.TripUpdate.GetTrip().GetRouteId()
	trip.DirectionID = convertRawDirectionId(g.TripUpdate.GetTrip().GetDirectionId())
	trip.StartTime = g.StartTime
	if vehicleID := g.Vehicle.GetVehicle().GetId(); vehicleID != "" {
		trip.VehicleID = vehicleID
	}
	trip.NumUpdates += 1

	stus := g.TripUpdate.GetStopTimeUpdate()
	if len(stus) == 0 {
		return
	}

	start := 0
	for i, stopTime := range trip.StopTimes {
		// TODO: doesn't this handle the case when the first stop has just been added to the schedule
		// Probably need a fancier algo for this
		if stopTime.StopID == stus[0].GetStopId() {
			start = i
			break
		}
	}

	end := start
	for _, stu := range stus {
		if len(trip.StopTimes) >= end {
			break
		}
		if trip.StopTimes[end].StopID != stu.GetStopId() {
			break
		}
		trip.StopTimes[end].ArrivalTime = convertOptionalTime(stu.GetArrival().GetTime())
		trip.StopTimes[end].DepartureTime = convertOptionalTime(stu.GetDeparture().GetTime())
		trip.StopTimes[end].Track = getTrack(stu)
		end++
	}

	trip.StopTimes = trip.StopTimes[:end]
	for _, stu := range stus[end-start:] {
		trip.StopTimes = append(trip.StopTimes, StopTime{
			StopID:        stu.GetStopId(),
			ArrivalTime:   convertOptionalTime(stu.GetArrival().GetTime()),
			DepartureTime: convertOptionalTime(stu.GetDeparture().GetTime()),
			Track:         getTrack(stu),
		})
	}
}
