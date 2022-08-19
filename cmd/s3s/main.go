package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/koluku/s3s"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var (
	// goreleaser
	Version = "current"

	// AWS
	region string

	// S3 Select Query
	query string
	where string
)

func main() {
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
		},
		Action: func(c *cli.Context) error {
			return cmd(c.Args().Slice())
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type bucketKeys struct {
	bucket string
	keys   []string
}

func cmd(paths []string) error {
	if len(paths) == 0 {
		return fmt.Errorf("no argument error")
	}

	if where != "" {
		query = fmt.Sprintf("SELECT * FROM S3Object s WHERE %s", where)
	}

	ctx := context.Background()
	app, err := s3s.NewApp(ctx, region)
	if err != nil {
		return err
	}

	targetBucketKeys, err := getBucketKeys(ctx, app, paths)
	if err != nil {
		return err
	}

	for _, bk := range targetBucketKeys {
		for _, key := range bk.keys {
			if err := s3s.S3Select(ctx, app, bk.bucket, key, query); err != nil {
				return err
			}
		}
	}

	return nil
}

// Get S3 Object Keys
func getBucketKeys(ctx context.Context, app *s3s.App, paths []string) ([]bucketKeys, error) {
	targetBucketKeys := []bucketKeys{}

	var eg errgroup.Group
	for _, path := range paths {
		path = path
		eg.Go(func() error {
			u, err := url.Parse(path)
			if err != nil {
				return err
			}
			var bucket, prefix string
			bucket = u.Hostname()
			prefix = strings.TrimPrefix(u.Path, "/")
			prefix = strings.TrimSuffix(prefix, "/")
			s3Keys, err := s3s.GetS3Keys(ctx, app, bucket, prefix)
			if err != nil {
				return err
			}
			targetBucketKeys = append(targetBucketKeys, bucketKeys{bucket: bucket, keys: s3Keys})
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return targetBucketKeys, nil
}
