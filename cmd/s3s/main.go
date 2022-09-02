package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"

	"github.com/koluku/s3s"
	"github.com/ktr0731/go-fuzzyfinder"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

const (
	DEFAULT_QUERY        = "SELECT * FROM S3Object s"
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
				Value:       DEFAULT_QUERY,
				Destination: &query,
			},
			&cli.StringFlag{
				Name:        "where",
				Aliases:     []string{"w"},
				Usage:       "WHERE part of the query",
				Destination: &where,
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
	// Arguments Check
	if isDelve {
		if len(paths) > 1 {
			return fmt.Errorf("too many argument error")
		}
	} else {
		if len(paths) == 0 {
			return fmt.Errorf("no argument error")
		}
	}

	if where != "" {
		query = fmt.Sprintf("SELECT * FROM S3Object s WHERE %s", where)
	}

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

	targetBucketKeys := make(chan bucketKeys, len(paths))
	if err := getBucketKeys(ctx, app, paths, targetBucketKeys); err != nil {
		return err
	}

	if err := execS3Select(ctx, app, targetBucketKeys); err != nil {
		return err
	}

	if isDelve {
		for _, path := range paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}

func pathDelver(ctx context.Context, app *s3s.App, paths []string) ([]string, error) {
	if len(paths) == 0 {
		path, err := delveBucketList(ctx, app)
		if err != nil {
			return nil, err
		}
		paths = []string{path}
	} else {
		u, err := url.Parse(paths[0])
		if err != nil {
			return nil, err
		}

		var bucket, prefix string
		bucket = u.Hostname()
		prefix = strings.TrimPrefix(u.Path, "/")

		path, err := delvePrefix(ctx, app, bucket, prefix)
		if err != nil {
			return nil, err
		}

		paths = []string{path}
	}

	return paths, nil
}

func delveBucketList(ctx context.Context, app *s3s.App) (string, error) {
	buckets, err := s3s.GetS3Bucket(ctx, app)
	if err != nil {
		return "", err
	}

	index, err := fuzzyfinder.Find(
		buckets,
		func(i int) string {
			return buckets[i]
		},
	)
	if err != nil {
		return "", err
	}

	return delvePrefix(ctx, app, buckets[index], "")
}

func delvePrefix(ctx context.Context, app *s3s.App, bucket string, prefix string) (string, error) {
	s3Dirs, err := s3s.GetS3Dir(ctx, app, bucket, prefix)
	if err != nil {
		return "", err
	}

	current := fmt.Sprintf("Query↵ (%s/%s)", bucket, prefix)
	parent := "←Back upper path"
	s3Dirs = append([]string{parent, current}, s3Dirs...)
	index, err := fuzzyfinder.Find(
		s3Dirs,
		func(i int) string {
			switch i {
			case 0:
				return parent
			case 1:
				return current
			default:
				return bucket + "/" + s3Dirs[i]
			}
		},
	)
	if err != nil {
		return "", err
	}

	switch index {
	case 0:
		parent = path.Join(prefix, "../")
		if parent == "." {
			return delvePrefix(ctx, app, bucket, "")
		}
		if parent == ".." {
			return delveBucketList(ctx, app)
		}
		return delvePrefix(ctx, app, bucket, parent+"/")
	case 1:
		return fmt.Sprintf("s3://%s/%s", bucket, prefix), nil
	default:
		return delvePrefix(ctx, app, bucket, s3Dirs[index])
	}
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
