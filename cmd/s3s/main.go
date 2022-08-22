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
	DEFAULT_THREAD_COUNT = 150
)

var (
	// goreleaser
	Version = "current"

	// AWS
	region string

	// S3 Select Query
	query string
	where string

	// command option
	threadCount int
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	app := &cli.App{
		Name:    "s3s",
		Version: Version,
		Usage:   "Easy S3 Select like searching directory",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "region",
				Usage:       "region of target s3 bucket exist",
				Value:       os.Getenv("AWS_REGION"),
				DefaultText: "AWS_REGION",
				Destination: &region,
			},
			&cli.StringFlag{
				Name:        "query",
				Aliases:     []string{"q"},
				Usage:       "SQL query for s3 select",
				Value:       "SELECT * FROM S3Object s",
				Destination: &query,
			},
			&cli.StringFlag{
				Name:        "where",
				Aliases:     []string{"w"},
				Usage:       "WHERE part of the SQL query",
				Destination: &where,
			},
			&cli.IntFlag{
				Name:        "thread_count",
				Aliases:     []string{"t"},
				Usage:       "max number of api requests to concurrently",
				Value:       DEFAULT_THREAD_COUNT,
				Destination: &threadCount,
			},
		},
		Action: func(c *cli.Context) error {
			return cmd(c.Context, c.Args().Slice())
		},
	}

	err := app.RunContext(ctx, os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type bucketKeys struct {
	bucket string
	keys   []string
}

func cmd(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return fmt.Errorf("no argument error")
	}

	if where != "" {
		query = fmt.Sprintf("SELECT * FROM S3Object s WHERE %s", where)
	}

	app, err := s3s.NewApp(ctx, region)
	if err != nil {
		return err
	}

	targetBucketKeys := make(chan bucketKeys, len(paths))
	if err := getBucketKeys(ctx, app, paths, targetBucketKeys); err != nil {
		return err
	}

	if err := execS3Select(ctx, app, targetBucketKeys); err != nil {
		return err
	}

	return nil
}

// Get S3 Object Keys
func getBucketKeys(ctx context.Context, app *s3s.App, paths []string, targetBucketKeys chan bucketKeys) error {
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

			s3Keys, err := s3s.GetS3Keys(egctx, app, bucket, prefix)
			if err != nil {
				return err
			}
			targetBucketKeys <- bucketKeys{bucket: bucket, keys: s3Keys}

			return nil
		})
	}

	err := eg.Wait()
	close(targetBucketKeys)
	if err != nil {
		return err
	}

	return nil
}

func execS3Select(ctx context.Context, app *s3s.App, targetBucketKeys chan bucketKeys) error {
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(threadCount)
	for bk := range targetBucketKeys {
		bucket := bk.bucket
		for _, key := range bk.keys {
			key := key
			eg.Go(func() error {
				if err := s3s.S3Select(egctx, app, bucket, key, query); err != nil {
					return err
				}
				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}
