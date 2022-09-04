package main

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/koluku/s3s"
	"github.com/ktr0731/go-fuzzyfinder"
)

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
