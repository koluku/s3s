package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/koluku/s3s"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

const (
	DEFAULT_QUERY            = "SELECT * FROM S3Object s"
	DEFAULT_THREAD_COUNT     = 150
	DEFAULT_MAX_RETRIES      = 20
	DEFAULT_FIELD_DELIMITER  = ","
	DEFAULT_RECORD_DELIMITER = "\n"
)

var (
	// goreleaser
	Version = "current"

	// AWS
	region string

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

	// command option
	threadCount int
	maxRetries  int
	isDelve     bool
	isDebug     bool
	isDryRun    bool
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
				Category:    "AWS:",
				Name:        "region",
				Usage:       "region of target s3 bucket exist",
				Value:       os.Getenv("AWS_REGION"),
				DefaultText: "ENV[\"AWS_REGION\"]",
				Destination: &region,
			},
			&cli.IntFlag{
				Category:    "AWS:",
				Name:        "thread-count",
				Aliases:     []string{"t, thread_count"},
				Usage:       "max number of api requests to concurrently",
				Value:       DEFAULT_THREAD_COUNT,
				Destination: &threadCount,
			},
			&cli.IntFlag{
				Category:    "AWS:",
				Name:        "max-retries",
				Aliases:     []string{"M, max_retries"},
				Usage:       "max number of api requests to retry",
				Value:       DEFAULT_MAX_RETRIES,
				Destination: &maxRetries,
			},
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
				Usage:       "",
				Destination: &isCSV,
			},
			&cli.BoolFlag{
				Category:    "Input Format:",
				Name:        "alb-logs",
				Aliases:     []string{"alb_logs"},
				Usage:       "",
				Destination: &isALBLogs,
			},
			&cli.BoolFlag{
				Category:    "Input Format:",
				Name:        "cf-logs",
				Aliases:     []string{"cf_logs"},
				Usage:       "",
				Destination: &isCFLogs,
			},
			&cli.DurationFlag{
				Category:    "Target:",
				Name:        "duration",
				Usage:       `from current time if alb or cf (ex: "2h3m")`,
				Destination: &duration,
			},
			&cli.TimestampFlag{
				Category:    "Target:",
				Name:        "since",
				Usage:       `end at if alb or cf (ex: "2006-01-02 15:04:05")`,
				Layout:      "2006-01-02 15:04:05",
				Timezone:    time.UTC,
				Destination: &cliSince,
			},
			&cli.TimestampFlag{
				Category:    "Target:",
				Name:        "until",
				Usage:       `start at if alb or cf (ex: "2006-01-02 15:04:05")`,
				Layout:      "2006-01-02 15:04:05",
				Timezone:    time.UTC,
				Destination: &cliUntil,
			},
			&cli.BoolFlag{
				Category:    "Run:",
				Name:        "delve",
				Usage:       "like directory move before querying",
				Value:       false,
				Destination: &isDelve,
			},
			&cli.BoolFlag{
				Category:    "Run:",
				Name:        "dry-run",
				Aliases:     []string{"dry_run"},
				Usage:       "pre request for s3 select",
				Value:       false,
				Destination: &isDryRun,
			},
			&cli.BoolFlag{
				Name:        "debug",
				Usage:       "erorr check for developer",
				Value:       false,
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

			if err := cmd(c.Context, c.Args().Slice()); err != nil {
				return errors.WithStack(err)
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
	// Arguments Check
	if err := checkArgs(paths); err != nil {
		return errors.WithStack(err)
	}
	if err := checkQuery(queryStr, where, limit, isCount); err != nil {
		return errors.WithStack(err)
	}
	if err := checkFileFormat(isCSV, isALBLogs, isCFLogs); err != nil {
		return errors.WithStack(err)
	}

	// Initialize
	app, err := s3s.NewApp(ctx, region, maxRetries, threadCount)
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
	queryInfo := &s3s.QueryInfo{
		IsCountMode: isCount,
	}
	keyInfo := &s3s.KeyInfo{
		KeyType: s3s.KeyTypeNone,
	}
	switch {
	case isCSV:
		queryInfo.FormatType = s3s.FormatTypeCSV
		queryInfo.FieldDelimiter = ","
		queryInfo.RecordDelimiter = "\n"
	case isALBLogs:
		queryInfo.FormatType = s3s.FormatTypeALBLogs
		queryInfo.FieldDelimiter = " "
		queryInfo.RecordDelimiter = "\n"
		keyInfo.KeyType = s3s.KeyTypeALB
		if duration != 0 {
			keyInfo.Since = time.Now().UTC().Add(duration * -1)
		} else {
			keyInfo.Since = since
			keyInfo.Until = until
		}
	case isCFLogs:
		queryInfo.FormatType = s3s.FormatTypeCFLogs
		queryInfo.FieldDelimiter = "\t"
		queryInfo.RecordDelimiter = "\n"
		keyInfo.KeyType = s3s.KeyTypeCF
		if duration != 0 {
			keyInfo.Since = time.Now().UTC().Add(duration * -1)
		} else {
			keyInfo.Since = since
			keyInfo.Until = until
		}
	default:
		queryInfo.FormatType = s3s.FormatTypeJSON
	}

	if isDryRun {
		scanByte, count, err := app.DryRun(ctx, paths, keyInfo, queryStr, queryInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Printf("all scan byte: %s\n", humanize.Bytes(uint64(scanByte)))
		fmt.Printf("file count: %s\n", humanize.Comma(int64(count)))
	} else {
		if err := app.Run(ctx, paths, keyInfo, queryStr, queryInfo); err != nil {
			return errors.WithStack(err)
		}
	}

	// Finalize
	if isDelve {
		for _, path := range paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}
