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
)

var (
	// AWS
	region string = os.Getenv("AWS_REGION")

	// S3 Select Query
	query string
	where string
)

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

	ctx := context.TODO()
	app, err := s3s.NewApp(ctx, region)
	if err != nil {
		return err
	}

	// Get S3 Object Keys
	targetBucketKeys := []bucketKeys{}
	for _, path := range paths {
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

func main() {
	app := &cli.App{
		Name:  "s3s",
		Usage: "Easy S3 Select like searching directory",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "region",
				Usage:       "region",
				Destination: &region,
			},
			&cli.StringFlag{
				Name:        "query",
				Aliases:     []string{"q"},
				Usage:       "query",
				Value:       "SELECT * FROM S3Object s",
				Destination: &query,
			},
			&cli.StringFlag{
				Name:        "where",
				Aliases:     []string{"w"},
				Usage:       "where",
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
