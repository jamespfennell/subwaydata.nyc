# Subway Data NYC GTFS Realtime data dump

This archive file contains one day of GTFS realtime data
  for the New York City Subway.
The data was produced by the MTA/New York City Transit
  and collected by the Subway Data NYC project.
See [api.mta.info](https://api.mta.info) for information on the feeds.
See [subwaydata.nyc](https://subwaydata.nyc) to learn about the collection process
  and find other data that is available.

The file name format for the data files is:

    nycsubway_<feed_id>_<iso8610_download_time>_<hash>.gtfsrt

where:

- `feed_id` identifies which of the 8 MTA data feeds the file comes from.
  Possible values are `1234567` (includes the Grand Central shuttle),
  `ACE` (includes the Franklin Ave and Rockaways shuttles), 
  `BDFM`, `G`, `JZ`, `L`, `NQRW`, `SIR` (Staten Island Railway).

- `iso8610_download_time` is the UTC time the data was downloaded at,
  [in ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601).
  The data starts and ends at midnight NYC time.
  In general this means that there is 24 hours of data here, but on days
  where there is a daylight savings adjustment there will be 23 or 25 hours of data.

- `hash` is a hash of the contents of the file. The 
  [software used to collect the feeds](https://github.com/jamespfennell/hoard)
  removes any consecutive files with the same hash.
