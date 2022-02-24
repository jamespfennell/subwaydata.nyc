package main

import (
	"flag"

	"github.com/jamespfennell/subwaydata.nyc/website"
)

var flagPort = flag.Int("port", 8080, "port to run the HTTP server on")
var flagMetadataUrl = flag.String("metadata-url", "", "URL for the metadata")

func main() {
	flag.Parse()
	website.Run(*flagMetadataUrl, *flagPort)
}
