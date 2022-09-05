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
	DEFAULT_QUERY        = "SELECT * FROM S3Object s"
	DEFAULT_THREAD_COUNT = 150
	DEFAULT_POOL_SIZE    = 1000
)

var (
	// goreleaser
	Version = "current"

	// AWS
	region string

	// S3 Select Query
	query   string
	where   string
	limit   int
	isCount bool

	// command option
	threadCount int
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
				Destination: &query,
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
			&cli.IntFlag{
				Name:        "thread_count",
				Aliases:     []string{"t"},
				Usage:       "max number of api requests to concurrently",
				Value:       DEFAULT_THREAD_COUNT,
				Destination: &threadCount,
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
	if err := checkQuery(query, where, limit, isCount); err != nil {
		return err
	}
	if query == "" {
		query = buildQuery(where, limit, isCount)
	}

	// Initialize
	app, err := s3s.NewApp(ctx, region)
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
		if err := execS3Select(egctx, app, ch); err != nil {
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
