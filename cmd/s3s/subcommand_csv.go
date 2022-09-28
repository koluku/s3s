package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dustin/go-humanize"
	"github.com/koluku/s3s"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

type CliCSVOption struct {
	Query string
	Where string
	Limit int
	Count bool

	Delve  bool
	DryRun bool
}

var CSVOption = &CliCSVOption{}

var SubcommandCSV = &cli.Command{
	Category: SUBCOMMAND_CATEGORY_FORMAT,
	Name:     "csv",
	Usage:    "Input CSV to Output JSON",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "query",
			Aliases:     []string{"q"},
			Usage:       "a query for S3 Select",
			Destination: &CSVOption.Query,
		},
		&cli.StringFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "where",
			Aliases:     []string{"w"},
			Usage:       "WHERE part of the query",
			Destination: &CSVOption.Where,
		},
		&cli.IntFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "limit",
			Aliases:     []string{"l"},
			Usage:       "max number of results from each key to return",
			Destination: &CSVOption.Limit,
		},
		&cli.BoolFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "count",
			Aliases:     []string{"c"},
			Usage:       "max number of results from each key to return",
			Destination: &CSVOption.Count,
		},
		&cli.BoolFlag{
			Name:        "delve",
			Usage:       "like directory move before querying",
			Value:       false,
			Destination: &CSVOption.Delve,
		},
		&cli.BoolFlag{
			Name:        "dry-run",
			Usage:       "pre request for s3 select",
			Value:       false,
			Destination: &CSVOption.DryRun,
		},
	},
	Action: func(c *cli.Context) error {
		if err := QueryForCSV(c.Context, c.Args().Slice()); err != nil {
			return errors.WithStack(err)
		}
		return nil
	},
}

func QueryForCSV(ctx context.Context, paths []string) error {
	// Arguments Check
	if err := CheckArgs(paths, CSVOption.Delve); err != nil {
		return errors.WithStack(err)
	}
	if err := CheckQuery(CSVOption.Query, CSVOption.Where, CSVOption.Limit, CSVOption.Count); err != nil {
		return errors.WithStack(err)
	}

	// Initialize
	app, err := s3s.NewApp(ctx, GlobalOption.Region, GlobalOption.MaxRetries, GlobalOption.ThreadCount)
	if err != nil {
		return errors.WithStack(err)
	}

	if CSVOption.Delve {
		newPaths, err := pathDelver(ctx, app, paths)
		if err != nil {
			return errors.WithStack(err)
		}
		paths = newPaths
	}

	// Execution
	if CSVOption.Query == "" {
		CSVOption.Query = BuildQuery(CSVOption.Where, CSVOption.Limit, CSVOption.Count)
	}
	queryInfo := &s3s.QueryInfo{
		FormatType:      s3s.FormatTypeCSV,
		FieldDelimiter:  ",",
		RecordDelimiter: "\n",
		IsCountMode:     CSVOption.Count,
	}
	keyInfo := &s3s.KeyInfo{
		KeyType: s3s.KeyTypeNone,
	}

	if CSVOption.DryRun {
		scanByte, count, err := app.DryRun(ctx, paths, keyInfo, JSONOption.Query, queryInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Printf("all scan byte: %s\n", humanize.Bytes(uint64(scanByte)))
		fmt.Printf("file count: %s\n", humanize.Comma(int64(count)))
	} else {
		if err := app.Run(ctx, paths, keyInfo, JSONOption.Query, queryInfo); err != nil {
			return errors.WithStack(err)
		}
	}

	// Finalize
	if CSVOption.Delve {
		for _, path := range paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}
