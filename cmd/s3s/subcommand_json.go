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

type CliJSONOption struct {
	Query string
	Where string
	Limit int
	Count bool

	Delve  bool
	DryRun bool
}

var JSONOption = &CliJSONOption{}

var SubcommandJSON = &cli.Command{
	Category: SUBCOMMAND_CATEGORY_FORMAT,
	Name:     "json",
	Usage:    "Input JSON to Output JSON",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "query",
			Aliases:     []string{"q"},
			Usage:       "a query for S3 Select",
			Destination: &JSONOption.Query,
		},
		&cli.StringFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "where",
			Aliases:     []string{"w"},
			Usage:       "WHERE part of the query",
			Destination: &JSONOption.Where,
		},
		&cli.IntFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "limit",
			Aliases:     []string{"l"},
			Usage:       "max number of results from each key to return",
			Destination: &JSONOption.Limit,
		},
		&cli.BoolFlag{
			Category:    SUBCOMMAND_FLAG_CATEGORY_QUERY,
			Name:        "count",
			Aliases:     []string{"c"},
			Usage:       "max number of results from each key to return",
			Destination: &JSONOption.Count,
		},
		&cli.BoolFlag{
			Name:        "delve",
			Usage:       "like directory move before querying",
			Value:       false,
			Destination: &JSONOption.Delve,
		},
		&cli.BoolFlag{
			Name:        "dry-run",
			Usage:       "pre request for s3 select",
			Value:       false,
			Destination: &JSONOption.DryRun,
		},
	},
	Action: func(c *cli.Context) error {
		if err := QueryForJSON(c.Context, c.Args().Slice()); err != nil {
			return errors.WithStack(err)
		}
		return nil
	},
}

func QueryForJSON(ctx context.Context, paths []string) error {
	// Arguments Check
	if err := CheckArgs(paths, JSONOption.Delve); err != nil {
		return errors.WithStack(err)
	}
	if err := CheckQuery(JSONOption.Query, JSONOption.Where, JSONOption.Limit, JSONOption.Count); err != nil {
		return errors.WithStack(err)
	}

	// Initialize
	app, err := s3s.NewApp(ctx, GlobalOption.Region, GlobalOption.MaxRetries, GlobalOption.ThreadCount)
	if err != nil {
		return errors.WithStack(err)
	}

	if JSONOption.Delve {
		newPaths, err := pathDelver(ctx, app, paths)
		if err != nil {
			return errors.WithStack(err)
		}
		paths = newPaths
	}

	// Execution
	if JSONOption.Query == "" {
		JSONOption.Query = BuildQuery(JSONOption.Where, JSONOption.Limit, JSONOption.Count)
	}
	queryInfo := &s3s.QueryInfo{
		FormatType:  s3s.FormatTypeJSON,
		IsCountMode: JSONOption.Count,
	}
	keyInfo := &s3s.KeyInfo{
		KeyType: s3s.KeyTypeNone,
	}

	if JSONOption.DryRun {
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
	if JSONOption.Delve {
		for _, path := range paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}
