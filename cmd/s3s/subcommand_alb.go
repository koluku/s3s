package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/koluku/s3s"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

type CliALBOption struct {
	Query string
	Where string
	Limit int
	Count bool

	Duration time.Duration
	cliSince cli.Timestamp
	Since    time.Time
	cliUntil cli.Timestamp
	Until    time.Time

	Delve  bool
	DryRun bool
}

var ALBOption = &CliALBOption{}

var SubcommandALB = &cli.Command{
	Category: SUBCOMMAND_CATEGORY_FORMAT,
	Name:     "alb",
	Usage:    "Input ALB to Output JSON",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "query",
			Aliases:     []string{"q"},
			Usage:       "a query for S3 Select",
			Destination: &ALBOption.Query,
		},
		&cli.StringFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "where",
			Aliases:     []string{"w"},
			Usage:       "WHERE part of the query",
			Destination: &ALBOption.Where,
		},
		&cli.IntFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "limit",
			Aliases:     []string{"l"},
			Usage:       "max number of results from each key to return",
			Destination: &ALBOption.Limit,
		},
		&cli.BoolFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "count",
			Aliases:     []string{"c"},
			Usage:       "max number of results from each key to return",
			Destination: &ALBOption.Count,
		},
		&cli.DurationFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_TIME,
			Name:        "duration",
			Usage:       `from current time if alb or cf (ex: "2h3m")`,
			Destination: &ALBOption.Duration,
		},
		&cli.TimestampFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_TIME,
			Name:        "since",
			Usage:       `end at if alb or cf (ex: "2006-01-02 15:04:05")`,
			Layout:      "2006-01-02 15:04:05",
			Timezone:    time.UTC,
			Destination: &ALBOption.cliSince,
		},
		&cli.TimestampFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_TIME,
			Name:        "until",
			Usage:       `start at if alb or cf (ex: "2006-01-02 15:04:05")`,
			Layout:      "2006-01-02 15:04:05",
			Timezone:    time.UTC,
			Destination: &ALBOption.cliUntil,
		},
		&cli.BoolFlag{
			Name:        "delve",
			Usage:       "like directory move before querying",
			Value:       false,
			Destination: &ALBOption.Delve,
		},
		&cli.BoolFlag{
			Name:        "dry-run",
			Usage:       "pre request for s3 select",
			Value:       false,
			Destination: &ALBOption.DryRun,
		},
	},
	Action: func(c *cli.Context) error {
		if ALBOption.cliSince.Value() != nil {
			ALBOption.Since = *ALBOption.cliSince.Value()
		}
		if ALBOption.cliUntil.Value() != nil {
			ALBOption.Until = *ALBOption.cliUntil.Value()
		}

		if err := QueryForALB(c.Context, c.Args().Slice()); err != nil {
			return errors.WithStack(err)
		}
		return nil
	},
}

func QueryForALB(ctx context.Context, paths []string) error {
	// Arguments Check
	if err := CheckArgs(paths, ALBOption.Delve); err != nil {
		return errors.WithStack(err)
	}
	if err := CheckQuery(ALBOption.Query, ALBOption.Where, ALBOption.Limit, ALBOption.Count); err != nil {
		return errors.WithStack(err)
	}

	// Initialize
	app, err := s3s.NewApp(ctx, GlobalOption.Region, GlobalOption.MaxRetries, GlobalOption.ThreadCount)
	if err != nil {
		return errors.WithStack(err)
	}

	if ALBOption.Delve {
		newPaths, err := pathDelver(ctx, app, paths)
		if err != nil {
			return errors.WithStack(err)
		}
		paths = newPaths
	}

	// Execution
	if ALBOption.Query == "" {
		ALBOption.Query = BuildQueryForALB(ALBOption.Where, ALBOption.Limit, ALBOption.Count)
	}
	queryInfo := &s3s.QueryInfo{
		FormatType:      s3s.FormatTypeALBLogs,
		FieldDelimiter:  " ",
		RecordDelimiter: "\n",
		IsCountMode:     ALBOption.Count,
	}
	var keyInfo *s3s.KeyInfo
	if ALBOption.Duration > 0 {
		keyInfo = &s3s.KeyInfo{
			KeyType: s3s.KeyTypeALB,
			Since:   time.Now().UTC().Add(ALBOption.Duration * -1),
		}
	} else {
		keyInfo = &s3s.KeyInfo{
			KeyType: s3s.KeyTypeALB,
			Since:   ALBOption.Since,
			Until:   ALBOption.Until,
		}
	}

	if ALBOption.DryRun {
		scanByte, count, err := app.DryRun(ctx, paths, keyInfo, ALBOption.Query, queryInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Printf("all scan byte: %s\n", humanize.Bytes(uint64(scanByte)))
		fmt.Printf("file count: %s\n", humanize.Comma(int64(count)))
	} else {
		if err := app.Run(ctx, paths, keyInfo, ALBOption.Query, queryInfo); err != nil {
			return errors.WithStack(err)
		}
	}

	// Finalize
	if ALBOption.Delve {
		for _, path := range paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}
