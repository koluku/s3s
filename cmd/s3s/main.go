package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"

	"github.com/koluku/s3s"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

const (
	DEFAULT_QUERY            = "SELECT * FROM S3Object s"
	DEFAULT_THREAD_COUNT     = 150
	DEFAULT_POOL_SIZE        = 1000
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
	queryStr        string
	where           string
	limit           int
	isCount         bool
	fieldDelimiter  string
	recordDelimiter string

	fromCSV   bool
	isALBLogs bool
	isCFLogs  bool

	// command option
	threadCount int
	maxRetries  int
	isDelve     bool
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	app := &cli.App{
		Name:    "s3s",
		Version: Version,
		Usage:   "Easy S3 select like searching in directories",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "region",
				Usage:       "region of target s3 bucket exist",
				Value:       os.Getenv("AWS_REGION"),
				DefaultText: "ENV[\"AWS_REGION\"]",
				Destination: &region,
			},
			&cli.StringFlag{
				Name:        "query",
				Aliases:     []string{"q"},
				Usage:       "a query for S3 Select",
				Destination: &queryStr,
			},
			&cli.StringFlag{
				Name:        "where",
				Aliases:     []string{"w"},
				Usage:       "WHERE part of the query",
				Destination: &where,
			},
			&cli.IntFlag{
				Name:        "limit",
				Aliases:     []string{"l"},
				Usage:       "max number of results from each key to return",
				Destination: &limit,
			},
			&cli.BoolFlag{
				Name:        "count",
				Aliases:     []string{"c"},
				Usage:       "max number of results from each key to return",
				Destination: &isCount,
			},
			&cli.StringFlag{
				Name:        "field_delimiter",
				Aliases:     []string{"d"},
				Usage:       "to read fields for CSV files",
				Destination: &fieldDelimiter,
			},
			&cli.StringFlag{
				Name:        "record_delimiter",
				Aliases:     []string{"D"},
				Usage:       "to read records for CSV files",
				Destination: &recordDelimiter,
			},
			&cli.BoolFlag{
				Name:        "from_csv",
				Usage:       "",
				Destination: &fromCSV,
			},
			&cli.BoolFlag{
				Name:        "alb_logs",
				Usage:       "",
				Destination: &isALBLogs,
			},
			&cli.BoolFlag{
				Name:        "cf_logs",
				Usage:       "",
				Destination: &isCFLogs,
			},
			&cli.IntFlag{
				Name:        "thread_count",
				Aliases:     []string{"t"},
				Usage:       "max number of api requests to concurrently",
				Value:       DEFAULT_THREAD_COUNT,
				Destination: &threadCount,
			},
			&cli.IntFlag{
				Name:        "max_retries",
				Aliases:     []string{"M"},
				Usage:       "max number of api requests to retry",
				Value:       DEFAULT_MAX_RETRIES,
				Destination: &maxRetries,
			},
			&cli.BoolFlag{
				Name:        "delve",
				Usage:       "like directory move before querying",
				Value:       false,
				Destination: &isDelve,
			},
		},
		Action: func(c *cli.Context) error {
			if err := cmd(c.Context, c.Args().Slice()); err != nil {
				return err
			}
			return nil
		},
	}

	err := app.RunContext(ctx, os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func cmd(ctx context.Context, paths []string) error {
	// Arguments Check
	if err := checkArgs(paths); err != nil {
		return err
	}
	if err := checkQuery(queryStr, where, limit, isCount); err != nil {
		return err
	}
	if err := checkFileFormat(fieldDelimiter, recordDelimiter, fromCSV, isALBLogs, isCFLogs); err != nil {
		return err
	}

	if queryStr == "" {
		query := &query{
			where:   where,
			limit:   limit,
			isCount: isCount,
		}
		queryStr = query.build()
	}
	var queryOption *s3s.S3SelectOption
	switch {
	case fromCSV:
		queryOption = &s3s.S3SelectOption{
			IsCSV:           true,
			FieldDelimiter:  DEFAULT_FIELD_DELIMITER,
			RecordDelimiter: DEFAULT_RECORD_DELIMITER,
		}
	case isALBLogs:
		queryOption = &s3s.S3SelectOption{
			IsCSV:           true,
			FieldDelimiter:  " ",
			RecordDelimiter: DEFAULT_RECORD_DELIMITER,
		}
	case isCFLogs:
		queryOption = &s3s.S3SelectOption{
			IsCSV:           true,
			FieldDelimiter:  " ",
			RecordDelimiter: DEFAULT_RECORD_DELIMITER,
		}
	default:
		queryOption = &s3s.S3SelectOption{
			IsCSV:           false,
			FieldDelimiter:  "",
			RecordDelimiter: "",
		}
	}

	// Initialize
	app, err := s3s.NewApp(ctx, region, maxRetries)
	if err != nil {
		return err
	}

	if isDelve {
		newPaths, err := pathDelver(ctx, app, paths)
		if err != nil {
			return err
		}
		paths = newPaths
	}

	// Execution
	ch := make(chan s3s.Path, DEFAULT_POOL_SIZE)
	eg, egctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := getBucketKeys(egctx, app, ch, paths); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		if err := execS3Select(egctx, app, ch, queryStr, queryOption); err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	// Finalize
	if isDelve {
		for _, path := range paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}

// Get S3 Object Keys
func getBucketKeys(ctx context.Context, app *s3s.App, ch chan<- s3s.Path, paths []string) error {
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(threadCount)
	for _, path := range paths {
		path := path
		eg.Go(func() error {
			u, err := url.Parse(path)
			if err != nil {
				return err
			}
			var bucket, prefix string
			bucket = u.Hostname()
			prefix = strings.TrimPrefix(u.Path, "/")
			prefix = strings.TrimSuffix(prefix, "/")

			if s3s.GetS3KeysWithChannel(egctx, app, ch, bucket, prefix); err != nil {
				return err
			}
			return nil
		})
	}

	err := eg.Wait()
	close(ch)
	if err != nil {
		return err
	}

	return nil
}
