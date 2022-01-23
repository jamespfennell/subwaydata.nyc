package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	hconfig "github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/export"
	"github.com/jamespfennell/subwaydata.nyc/etl/journal"
	"github.com/jamespfennell/subwaydata.nyc/etl/pipeline"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/jamespfennell/xz"
	"github.com/urfave/cli/v2"
)

const hoardConfig = "hoard-config"
const etlConfig = "etl-config"
const day = "day"

const descriptionMain = `
ETL pipeline for subwaydata.nyc
`

func main() {
	app := &cli.App{
		Name:        "Subway Data NYC ETL Pipeline",
		Usage:       "",
		Description: descriptionMain,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  hoardConfig,
				Usage: "path to the Hoard config file",
			},
			&cli.StringFlag{
				Name:  etlConfig,
				Usage: "path to the ETL config file",
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "run one iteration of the ETL pipeline",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  day,
						Usage: "run the pipeline for the provided day (YYYY-MM-DD) only",
					},
				},
				Action: func(c *cli.Context) error {
					hc, err := getHoardConfig(c)
					if err != nil {
						return err
					}
					ec, err := getEtlConfig(c)
					if err != nil {
						return err
					}
					if c.IsSet(day) {
						d, err := metadata.ParseDay(c.String(day))
						if err != nil {
							return fmt.Errorf("failed to read day flag: %w", err)
						}
						return pipeline.Run(
							d,
							[]string{"nycsubway_L"},
							ec,
							hc,
						)
					}
					_ = ec
					_ = hc
					return fmt.Errorf("not implemented")
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

func getHoardConfig(c *cli.Context) (*hconfig.Config, error) {
	if !c.IsSet(hoardConfig) {
		return nil, fmt.Errorf("a Hoard config must be provided")
	}
	path := c.String(hoardConfig)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the Hoard config file from disk: %w", err)
	}
	return hconfig.NewConfig(b)
}

func getEtlConfig(c *cli.Context) (*config.Config, error) {
	if !c.IsSet(etlConfig) {
		return nil, fmt.Errorf("an ETL config must be provided")
	}
	path := c.String(etlConfig)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the ETL config file from disk: %w", err)
	}
	var ec config.Config
	if err := json.Unmarshal(b, &ec); err != nil {
		return nil, fmt.Errorf("failed to parse the ETL config file: %w", err)
	}
	return &ec, nil
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
	source, err := journal.NewDirectoryGtfsrtSource("../tmp/data/nycsubway_L/")
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
