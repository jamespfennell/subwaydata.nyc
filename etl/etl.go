package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	hconfig "github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/pipeline"
	"github.com/jamespfennell/subwaydata.nyc/etl/storage"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/urfave/cli/v2"
)

const hoardConfig = "hoard-config"
const etlConfig = "etl-config"

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
				// periodic 01:00:00-04:00:00 05:00:00-06:30:00 - run the backlog process during the day
				// backlog --limit N --timeout T --list-only
				Name:        "run",
				Usage:       "run the ETL pipeline for a specific day",
				UsageText:   "etl run YYYY-MM-DD",
				Description: "Runs the pipeline for the specified day (YYYY-MM-DD).",
				Action: func(c *cli.Context) error {
					session, err := newSession(c)
					if err != nil {
						return err
					}
					args := c.Args()
					switch args.Len() {
					case 0:
						return fmt.Errorf("no day provided")
					case 1:
						d, err := metadata.ParseDay(args.Get(0))
						if err != nil {
							return err
						}
						return pipeline.Run(
							d,
							[]string{"nycsubway_L"},
							session.ec,
							session.hc,
							session.sc,
						)
					default:
						return fmt.Errorf("too many command line arguments passed")
					}
				},
			},
			{
				Name:        "backlog",
				Usage:       "run the ETL pipeline for all days that are not up-to-date",
				Description: "Runs the pipeline for days that are not up to date.",
				Flags: []cli.Flag{
					&cli.DurationFlag{
						Name:  "timeout",
						Usage: "maximum time to run for",
					},
					&cli.IntFlag{
						Name:  "limit",
						Usage: "maximum number of days to process",
					},
					&cli.BoolFlag{
						Name:  "dry-run",
						Usage: "only calculate the days that need to be updated, but don't update them",
					},
				},
				Action: func(c *cli.Context) error {
					session, err := newSession(c)
					if err != nil {
						return err
					}
					ec := session.ec
					m, err := session.sc.GetMetadata()
					if err != nil {
						return fmt.Errorf("failed to obtain metadata: %w", err)
					}

					loc, err := time.LoadLocation(ec.Timezone)
					if err != nil {
						return fmt.Errorf("unable to load timezone %q: %w", ec.Timezone, err)
					}
					now := time.Now().In(loc).Add(-5 * time.Hour).Format("2006-01-02")
					d, _ := metadata.ParseDay(now)

					pendingDays := config.CalculatePendingDays(ec.Feeds, m.ProcessedDays, d)
					if len(pendingDays) == 0 {
						fmt.Println("No days in the backlog")
						return nil
					}
					fmt.Printf("%d days in the backlog:\n", len(pendingDays))
					for i, pendingDay := range pendingDays {
						if i >= 20 {
							fmt.Printf("...and %d more days\n", len(pendingDays)-20)
							break
						}
						d := pendingDay
						fmt.Printf("- %s (feeds: %s)\n", d.Day, d.FeedIDs)
					}
					if c.Bool("dry-run") {
						return nil
					}
					fmt.Println("Running")
					for i, pendingDay := range pendingDays {
						if c.IsSet("limit") && c.Int("limit") <= i {
							fmt.Println("Reached limit, ending...")
							break
						}
						fmt.Printf("Running for %s\n", pendingDay.Day)
						err := pipeline.Run(
							pendingDay.Day,
							pendingDay.FeedIDs,
							session.ec,
							session.hc,
							session.sc,
						)
						if err != nil {
							return err
						}
					}
					return nil
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

type session struct {
	ec *config.Config
	hc *hconfig.Config
	sc *storage.Client
}

func newSession(c *cli.Context) (*session, error) {
	if !c.IsSet(hoardConfig) {
		return nil, fmt.Errorf("a Hoard config must be provided")
	}
	path := c.String(hoardConfig)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the Hoard config file from disk: %w", err)
	}
	hc, err := hconfig.NewConfig(b)
	if err != nil {
		return nil, err
	}

	if !c.IsSet(etlConfig) {
		return nil, fmt.Errorf("an ETL config must be provided")
	}
	path = c.String(etlConfig)
	b, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the ETL config file from disk: %w", err)
	}
	var ec config.Config
	if err := json.Unmarshal(b, &ec); err != nil {
		return nil, fmt.Errorf("failed to parse the ETL config file: %w", err)
	}

	sc, err := storage.NewClient(&ec)
	if err != nil {
		return nil, err
	}
	return &session{
		ec: &ec,
		hc: hc,
		sc: sc,
	}, nil
}
