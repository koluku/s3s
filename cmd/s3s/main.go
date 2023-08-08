package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/koluku/s3s"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

var (
	// goreleaser
	Version = "current"

	// S3 Select Query
	queryStr string
	where    string
	limit    int
	isCount  bool

	isCSV     bool
	isALBLogs bool
	isCFLogs  bool

	duration time.Duration
	since    time.Time
	cliSince cli.Timestamp
	until    time.Time
	cliUntil cli.Timestamp

	output string

	// command option
	isDelve  bool
	isDebug  bool
	isDryRun bool
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	app := &cli.App{
		Name:    "s3s",
		Version: Version,
		Usage:   "Easy S3 select like searching in directories",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Category:    "Query:",
				Name:        "query",
				Aliases:     []string{"q"},
				Usage:       "a query for S3 Select",
				Destination: &queryStr,
			},
			&cli.StringFlag{
				Category:    "Query:",
				Name:        "where",
				Aliases:     []string{"w"},
				Usage:       "WHERE part of the query",
				Destination: &where,
			},
			&cli.IntFlag{
				Category:    "Query:",
				Name:        "limit",
				Aliases:     []string{"l"},
				Usage:       "max number of results from each key to return",
				Destination: &limit,
			},
			&cli.BoolFlag{
				Category:    "Query:",
				Name:        "count",
				Aliases:     []string{"c"},
				Usage:       "max number of results from each key to return",
				Destination: &isCount,
			},
			&cli.BoolFlag{
				Category:    "Input Format:",
				Name:        "csv",
				Destination: &isCSV,
			},
			&cli.BoolFlag{
				Category:    "Input Format:",
				Name:        "alb-logs",
				Aliases:     []string{"alb_logs"},
				Destination: &isALBLogs,
			},
			&cli.BoolFlag{
				Category:    "Input Format:",
				Name:        "cf-logs",
				Aliases:     []string{"cf_logs"},
				Destination: &isCFLogs,
			},
			&cli.DurationFlag{
				Category:    "Time:",
				Name:        "duration",
				Usage:       `from current time if alb or cf (ex: "2h3m")`,
				Destination: &duration,
			},
			&cli.TimestampFlag{
				Category:    "Time:",
				Name:        "since",
				Usage:       `end at if alb or cf (ex: "2006-01-02 15:04:05")`,
				Layout:      "2006-01-02 15:04:05",
				Timezone:    time.UTC,
				Destination: &cliSince,
			},
			&cli.TimestampFlag{
				Category:    "Time:",
				Name:        "until",
				Usage:       `start at if alb or cf (ex: "2006-01-02 15:04:05")`,
				Layout:      "2006-01-02 15:04:05",
				Timezone:    time.UTC,
				Destination: &cliUntil,
			},
			&cli.StringFlag{
				Category:    "Output:",
				Name:        "output",
				Aliases:     []string{"o"},
				Destination: &output,
			},
			&cli.BoolFlag{
				Category:    "Run:",
				Name:        "delve",
				Usage:       "like directory move before querying",
				Destination: &isDelve,
			},
			&cli.BoolFlag{
				Category:    "Run:",
				Name:        "dry-run",
				Aliases:     []string{"dry_run"},
				Usage:       "pre request for s3 select",
				Destination: &isDryRun,
			},
			&cli.BoolFlag{
				Name:    "interactive",
				Aliases: []string{"i"},
				Usage:   "use as interactive mode",
			},
			&cli.BoolFlag{
				Name:        "debug",
				Usage:       "erorr check for developer",
				Destination: &isDebug,
			},
		},
		Action: func(c *cli.Context) error {
			if cliSince.Value() != nil {
				since = *cliSince.Value()
			}
			if cliUntil.Value() != nil {
				until = *cliUntil.Value()
			}

			if c.Bool("interactive") {
				if err := prompter(c.Context); err != nil {
					return errors.WithStack(err)
				}
			} else {
				if err := cmd(c.Context, c.Args().Slice()); err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		},
	}

	err := app.RunContext(ctx, os.Args)
	if err != nil {
		if isDebug {
			log.Fatalf("%+v\n", err)
		} else {
			log.Fatal(err)
		}
	}
}

func cmd(ctx context.Context, paths []string) error {
	// Arguments and Options Check
	if err := checkArgs(paths); err != nil {
		return errors.WithStack(err)
	}
	if isALBLogs || isCFLogs {
		if err := checkTime(duration, until, since); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := checkQuery(queryStr, where, limit, isCount); err != nil {
		return errors.WithStack(err)
	}
	if err := checkFileFormat(isCSV, isALBLogs, isCFLogs); err != nil {
		return errors.WithStack(err)
	}

	// Initialize
	app, err := s3s.New(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	if isDelve {
		newPaths, err := pathDelver(ctx, app, paths)
		if err != nil {
			return errors.WithStack(err)
		}
		paths = newPaths
	}

	// Execution
	if queryStr == "" {
		queryStr = buildQuery(where, limit, isCount, isALBLogs, isCFLogs)
	}
	var outputPath string
	if output != "" {
		outputPath, err = filepath.Abs(output)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	var query *s3s.Query
	switch {
	case isCSV:
		query = &s3s.Query{
			FormatType: s3s.FormatTypeCSV,
			Query:      queryStr,
		}
	case isALBLogs:
		query = &s3s.Query{
			FormatType: s3s.FormatTypeALBLogs,
			Query:      queryStr,
		}
		if duration > 0 {
			query.Since = time.Now().UTC().Add(-duration)
			query.Until = time.Now().UTC()
		} else {
			query.Since = since
			query.Until = until
		}
	case isCFLogs:
		query = &s3s.Query{
			FormatType: s3s.FormatTypeCFLogs,
			Query:      queryStr,
		}
		if duration > 0 {
			query.Since = time.Now().UTC().Add(-duration)
			query.Until = time.Now().UTC()
		} else {
			query.Since = since
			query.Until = until
		}
	default:
		query = &s3s.Query{
			FormatType: s3s.FormatTypeJSON,
			Query:      queryStr,
		}
	}
	option := &s3s.Option{
		IsDryRun:    isDryRun,
		IsCountMode: isCount,
		Output:      outputPath,
	}

	result, err := app.Run(ctx, paths, query, option)
	if err != nil {
		return errors.WithStack(err)
	}

	// Output
	if isDryRun {
		fmt.Printf("file count: %s\n", humanize.Comma(int64(result.Count)))
		fmt.Printf("all scan byte: %s\n", humanize.Bytes(uint64(result.Bytes)))
	}
	if isDelve {
		for _, path := range paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}
