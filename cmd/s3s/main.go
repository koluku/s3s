package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/koluku/s3s"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

var (
	Name    = "s3s"
	Usage   = "Easy S3 select like searching in directories"
	Version = "current" // goreleaser sets this value
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	var state State
	var runner Runner

	app := &cli.App{
		Name:    Name,
		Version: Version,
		Usage:   Usage,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Category:    "Query:",
				Name:        "query",
				Aliases:     []string{"q"},
				Usage:       "a query for S3 Select",
				Destination: &state.Query,
			},
			&cli.StringFlag{
				Category:    "Query:",
				Name:        "where",
				Aliases:     []string{"w"},
				Usage:       "WHERE part of the query",
				Destination: &state.Where,
			},
			&cli.IntFlag{
				Category:    "Query:",
				Name:        "limit",
				Aliases:     []string{"l"},
				Usage:       "max number of results from each key to return",
				Destination: &state.Limit,
			},
			&cli.BoolFlag{
				Category:    "Query:",
				Name:        "count",
				Aliases:     []string{"c"},
				Usage:       "max number of results from each key to return",
				Destination: &state.IsCount,
			},
			&cli.BoolFlag{
				Category:    "Input Format:",
				Name:        "csv",
				Destination: &state.IsCSV,
			},
			&cli.BoolFlag{
				Category:    "Input Format:",
				Name:        "alb-logs",
				Destination: &state.IsAlbLogs,
			},
			&cli.BoolFlag{
				Category:    "Input Format:",
				Name:        "cf-logs",
				Destination: &state.IsCfLogs,
			},
			&cli.DurationFlag{
				Category:    "Time:",
				Name:        "duration",
				Usage:       `from current time if alb or cf (ex: "2h3m")`,
				Destination: &state.Duration,
			},
			&cli.TimestampFlag{
				Category: "Time:",
				Name:     "since",
				Usage:    `end at if alb or cf (ex: "2006-01-02 15:04:05")`,
				Layout:   "2006-01-02 15:04:05",
				Timezone: time.UTC,
			},
			&cli.TimestampFlag{
				Category: "Time:",
				Name:     "until",
				Usage:    `start at if alb or cf (ex: "2006-01-02 15:04:05")`,
				Layout:   "2006-01-02 15:04:05",
				Timezone: time.UTC,
			},
			&cli.StringFlag{
				Category:    "Output:",
				Name:        "output",
				Aliases:     []string{"o"},
				Destination: &state.Output,
			},
			&cli.BoolFlag{
				Category:    "Run:",
				Name:        "delve",
				Usage:       "like directory move before querying",
				Destination: &state.IsDelve,
			},
			&cli.BoolFlag{
				Category:    "Run:",
				Name:        "dry-run",
				Usage:       "pre request for s3 select",
				Destination: &state.IsDryRun,
			},
			&cli.BoolFlag{
				Name:        "intteractive",
				Aliases:     []string{"i"},
				Destination: &state.IsInteractive,
			},
			&cli.BoolFlag{
				Name:    "interactive",
				Aliases: []string{"i"},
				Usage:   "use as interactive mode",
			},
			&cli.BoolFlag{
				Name:        "debug",
				Usage:       "erorr check for developer",
				Destination: &state.IsDebug,
			},
		},
		Action: func(c *cli.Context) error {
			state.Paths = c.Args().Slice()
			state.Since = c.Timestamp("since")
			state.Until = c.Timestamp("until")

			client, err := s3s.New(ctx)
			if err != nil {
				return errors.WithStack(err)
			}

			if state.IsInteractive {
				runner = &PromptRunner{
					s3s:   client,
					state: &state,
				}
			} else {
				runner = &CommandLineRunner{
					s3s:   client,
					state: &state,
				}
			}

			return runner.Run(c.Context)
		},
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		if state.IsDebug {
			log.Fatalf("%+v\n", err)
		} else {
			log.Fatal(err)
		}
	}
}
