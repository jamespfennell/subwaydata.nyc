package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jamespfennell/gtfs/journal"
	"github.com/jamespfennell/subwaydata.nyc/etl/export"
)

var printStopTimes = flag.Bool("s", false, "Print stop times")

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		fmt.Println("Expected directory to be passed")
		os.Exit(1)
	}
	dir := args[0]
	source, err := journal.NewDirectoryGtfsrtSource(dir)
	if err != nil {
		fmt.Printf("Failed to open %s: %s", dir, err)
	}
	j := journal.BuildJournal(source, time.Unix(0, 0), time.Now())
	tripsD, stopTimesD, err := export.AsCsv(j.Trips)
	if err != nil {
		fmt.Printf("Failed to export trips: %s", err)
	}
	fmt.Println(string(tripsD))
	if *printStopTimes {
		fmt.Println(string(stopTimesD))
	}
}
