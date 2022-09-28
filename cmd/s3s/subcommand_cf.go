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

type CliCFOption struct {
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

var CFOption = &CliCFOption{}

var SubcommandCF = &cli.Command{
	Category: SUBCOMMAND_CATEGORY_FORMAT,
	Name:     "cf",
	Usage:    "Input CF to Output JSON",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "query",
			Aliases:     []string{"q"},
			Usage:       "a query for S3 Select",
			Destination: &CFOption.Query,
		},
		&cli.StringFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "where",
			Aliases:     []string{"w"},
			Usage:       "WHERE part of the query",
			Destination: &CFOption.Where,
		},
		&cli.IntFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "limit",
			Aliases:     []string{"l"},
			Usage:       "max number of results from each key to return",
			Destination: &CFOption.Limit,
		},
		&cli.BoolFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "count",
			Aliases:     []string{"c"},
			Usage:       "max number of results from each key to return",
			Destination: &CFOption.Count,
		},
		&cli.DurationFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_TIME,
			Name:        "duration",
			Usage:       `from current time if alb or cf (ex: "2h3m")`,
			Destination: &CFOption.Duration,
		},
		&cli.TimestampFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_TIME,
			Name:        "since",
			Usage:       `end at if alb or cf (ex: "2006-01-02 15:04:05")`,
			Layout:      "2006-01-02 15:04:05",
			Timezone:    time.UTC,
			Destination: &CFOption.cliSince,
		},
		&cli.TimestampFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_TIME,
			Name:        "until",
			Usage:       `start at if alb or cf (ex: "2006-01-02 15:04:05")`,
			Layout:      "2006-01-02 15:04:05",
			Timezone:    time.UTC,
			Destination: &CFOption.cliUntil,
		},
		&cli.BoolFlag{
			Name:        "delve",
			Usage:       "like directory move before querying",
			Value:       false,
			Destination: &CFOption.Delve,
		},
		&cli.BoolFlag{
			Name:        "dry-run",
			Usage:       "pre request for s3 select",
			Value:       false,
			Destination: &CFOption.DryRun,
		},
	},
	Action: func(c *cli.Context) error {
		if ALBOption.cliSince.Value() != nil {
			ALBOption.Since = *ALBOption.cliSince.Value()
		}
		if ALBOption.cliUntil.Value() != nil {
			ALBOption.Until = *ALBOption.cliUntil.Value()
		}

		if err := QueryForCF(c.Context, c.Args().Slice()); err != nil {
			return errors.WithStack(err)
		}
		return nil
	},
}

func QueryForCF(ctx context.Context, paths []string) error {
	// Arguments Check
	if err := CheckArgs(paths, CFOption.Delve); err != nil {
		return errors.WithStack(err)
	}
	if err := CheckQuery(CFOption.Query, CFOption.Where, CFOption.Limit, CFOption.Count); err != nil {
		return errors.WithStack(err)
	}

	// Initialize
	app, err := s3s.NewApp(ctx, GlobalOption.Region, GlobalOption.MaxRetries, GlobalOption.ThreadCount)
	if err != nil {
		return errors.WithStack(err)
	}

	if CFOption.Delve {
		newPaths, err := pathDelver(ctx, app, paths)
		if err != nil {
			return errors.WithStack(err)
		}
		paths = newPaths
	}

	// Execution
	if CFOption.Query == "" {
		CFOption.Query = BuildQueryForCF(CFOption.Where, CFOption.Limit, CFOption.Count)
	}
	queryInfo := &s3s.QueryInfo{
		FormatType:      s3s.FormatTypeCFLogs,
		FieldDelimiter:  "\t",
		RecordDelimiter: "\n",
		IsCountMode:     CFOption.Count,
	}
	var keyInfo *s3s.KeyInfo
	if CFOption.Duration > 0 {
		keyInfo = &s3s.KeyInfo{
			KeyType: s3s.KeyTypeCF,
			Since:   time.Now().UTC().Add(CFOption.Duration * -1),
		}
	} else {
		keyInfo = &s3s.KeyInfo{
			KeyType: s3s.KeyTypeCF,
			Since:   CFOption.Since,
			Until:   CFOption.Until,
		}

	}

	if CFOption.DryRun {
		scanByte, count, err := app.DryRun(ctx, paths, keyInfo, CFOption.Query, queryInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Printf("all scan byte: %s\n", humanize.Bytes(uint64(scanByte)))
		fmt.Printf("file count: %s\n", humanize.Comma(int64(count)))
	} else {
		if err := app.Run(ctx, paths, keyInfo, CFOption.Query, queryInfo); err != nil {
			return errors.WithStack(err)
		}
	}

	// Finalize
	if CFOption.Delve {
		for _, path := range paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}
