# subwaydata.nyc

This is all of the source code for the [subwaydata.nyc](https://subwaydata.nyc) project.
The project has two pieces:

- An ETL pipeline that takes raw subway data collected using [Hoard](https://github.com/jamespfennell) 
    and generates per-day CSV files containing data on all of the trips that ran.

- An HTTP server that serves the website files.

# Metadata

The project maintains a metadata file that lists all days that have been processed.
This file is persisted in Digital Ocean's object/bucket storage, and [can be downloaded directly](https://data.subwaydata.nyc/subwaydatanyc_metadata.json).
When the ETL pipeline runs the metadata file is updated.
The website periodically polls for the file and updates the HTML files when it changes.

The structure of the metadata file is defined in the `metadata/metadata.go` file.

# ETL pipeline

The code is mostly in the `etl` directory.
A binary is defined in `cmd/etl`.

The ETL pipeline needs a config file that specifies the feeds to work with
and credentials for the object storage where the data is stored.
And example of this config is given in `etl/config/sample.json`.

To run the ETL pipeline for a single day:

```
go run ./cmd/etl --hoard-config $HOARD_CONFIG --etl-config $ETL_CONFIG run --day YYYY-MM-DD
```

To run the ETL pipeline over the whole backlog
(i.e. days that have yet to be processed):

```
go run ./cmd/etl --hoard-config $HOARD_CONFIG --etl-config $ETL_CONFIG backlog
```

To run a periodic job that runs the pipeline every day
at certain times:

```
go run ./cmd/etl  --hoard-config $HOARD_CONFIG --etl-config $ETL_CONFIG periodic 05:30:00-06:00:00
```

All of these commands have different options and the help text is reasonable:

```
go run ./cmd/etl help
```

# Website

The code is mostly in the `website` directory.
A binary is defined in `cmd/website`.

The website periodically polls for the metadata and
then updates the HTML files. Run it with:

```
go run ./cmd/website --metadata-url https://data.subwaydata.nyc/subwaydatanyc_metadata.json --port 8080
```
