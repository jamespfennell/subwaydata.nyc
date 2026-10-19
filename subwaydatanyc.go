package main

import (
	"context"
	"fmt"
	"log"
	"os"

	hconfig "github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/subwaydata.nyc/etl"
	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/periodic"
	"github.com/jamespfennell/subwaydata.nyc/etl/status"
	"github.com/jamespfennell/subwaydata.nyc/etl/storage"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/jamespfennell/subwaydata.nyc/website"
	"github.com/urfave/cli/v2"
)

const (
	etlConfig   = "etl-config"
	hoardConfig = "hoard-config"
	statusPort  = "status-port"
)

func main() {
	app := &cli.App{
		Name:     "subwaydatanyc",
		HelpName: "subwaydatanyc",
		Usage:    "tools for the subwaydata.nyc project",
		Commands: []*cli.Command{
			{
				Name:  "etl",
				Usage: "run the subwaydata.nyc ETL pipeline",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     hoardConfig,
						Usage:    "path to the Hoard config file",
						Required: true,
					},
					&cli.StringFlag{
						Name:     etlConfig,
						Usage:    "path to the ETL config file",
						Required: true,
					},
				},
				Subcommands: []*cli.Command{
					{
						Name:  "delete",
						Usage: "delete a range of processed days",
						Flags: []cli.Flag{
							&cli.StringSliceFlag{
								Name:  "day",
								Usage: "day to delete",
							},
							&cli.BoolFlag{
								Name:  "yes",
								Usage: "perform the deletions",
							},
						},
						Action: func(c *cli.Context) error {
							session, err := newSession(c)
							if err != nil {
								return err
							}
							var days []metadata.Day
							for _, rawDay := range c.StringSlice("day") {
								day, err := metadata.ParseDay(rawDay)
								if err != nil {
									return fmt.Errorf("failed to parse start date: %w", err)
								}
								days = append(days, day)
							}
							ctx := context.Background()
							return etl.DeleteDays(ctx, days, !c.Bool("yes"), session.ec, session.sc)
						},
					},
					{
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
								feedIDs := config.FeedsForDay(session.ec.Feeds, d)
								if len(feedIDs) == 0 {
									return fmt.Errorf("no feeds are configured for %s", d)
								}
								return etl.Run(
									context.Background(),
									d,
									feedIDs,
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
							&cli.IntFlag{
								Name:        "limit",
								Aliases:     []string{"l"},
								Usage:       "maximum number of days to process",
								DefaultText: "no limit",
							},
							&cli.IntFlag{
								Name:    "concurrency",
								Aliases: []string{"c"},
								Value:   1,
								Usage:   "number of days to run concurrently",
							},
							&cli.BoolFlag{
								Name:    "dry-run",
								Aliases: []string{"d"},
								Usage:   "only calculate the days that need to be updated, but don't update them",
							},
						},
						Action: func(c *cli.Context) error {
							session, err := newSession(c)
							if err != nil {
								return err
							}
							opts := etl.BacklogOptions{
								DryRun:      c.Bool("dry-run"),
								Concurrency: c.Int("concurrency"),
							}
							if c.IsSet("limit") {
								l := c.Int("limit")
								opts.Limit = &l
							}
							return etl.Backlog(context.Background(), session.ec, session.hc, session.sc, opts)
						},
					},
					{
						Name:  "periodic",
						Usage: "run the ETL pipeline periodically",
						Flags: []cli.Flag{
							&cli.IntFlag{
								Name:        statusPort,
								Usage:       "port to serve the status page on; if the process fails to start it keeps running to serve the error",
								DefaultText: "no status page",
							},
						},
						Action: func(c *cli.Context) error {
							st := status.New()
							if c.IsSet(statusPort) {
								go func() {
									log.Fatal(st.Serve(c.Int(statusPort)))
								}()
							}
							session, intervals, err := startPeriodic(c)
							if err != nil {
								if !c.IsSet(statusPort) {
									return err
								}
								// Keep running so that the error is visible on the status page.
								log.Printf("Error: %s", err)
								st.SetStartupError(err)
								select {}
							}
							var schedule []string
							for _, interval := range intervals {
								schedule = append(schedule, interval.String())
							}
							var buckets []status.Bucket
							for _, b := range session.ec.Buckets {
								buckets = append(buckets, status.Bucket{Id: b.Id, Name: b.Name, Url: b.Url, Primary: b.Primary})
							}
							st.SetStarted(buckets, schedule)
							periodic.Run(context.Background(), session.ec, session.hc, session.sc, intervals, st)
							return nil
						},
					},
				},
			},
			{
				Name:  "website",
				Usage: "serve the subwaydata.nyc website",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:        "port",
						Usage:       "port to run the HTTP server on",
						Value:       8080,
						DefaultText: "8080",
					},
					&cli.StringFlag{
						Name:     "metadata-url",
						Usage:    "URL for the metadata",
						Required: true,
					},
				},
				Action: func(ctx *cli.Context) error {
					website.Run(ctx.String("metadata-url"), ctx.Int("port"))
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

// startPeriodic performs all of the startup work for the periodic command.
func startPeriodic(c *cli.Context) (*session, []periodic.Interval, error) {
	session, err := newSession(c)
	if err != nil {
		return nil, nil, err
	}
	args := c.Args().Slice()
	if len(args) == 0 {
		return nil, nil, fmt.Errorf("no intervals provided")
	}
	var intervals []periodic.Interval
	for _, arg := range args {
		interval, err := periodic.NewInterval(arg)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse interval: %w", err)
		}
		intervals = append(intervals, interval)
	}
	if err := etl.CleanWorkspace(); err != nil {
		return nil, nil, fmt.Errorf("failed to clean workspace: %w", err)
	}
	return session, intervals, nil
}

type session struct {
	ec *config.Config
	hc *hconfig.Config
	sc *storage.Client
}

func newSession(c *cli.Context) (*session, error) {
	path := c.String(hoardConfig)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the Hoard config file from disk: %w", err)
	}
	hc, err := hconfig.NewConfig(b)
	if err != nil {
		return nil, err
	}

	path = c.String(etlConfig)
	b, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the ETL config file from disk: %w", err)
	}
	ec, err := config.Parse(b)
	if err != nil {
		return nil, fmt.Errorf("failed to parse the ETL config file: %w", err)
	}

	sc, err := storage.NewClient(ec)
	if err != nil {
		return nil, err
	}
	return &session{
		ec: ec,
		hc: hc,
		sc: sc,
	}, nil
}
