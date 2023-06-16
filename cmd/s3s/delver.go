package main

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/koluku/s3s"
	"github.com/ktr0731/go-fuzzyfinder"
	"github.com/pkg/errors"
)

func pathDelver(ctx context.Context, client *s3s.Client, paths []string) ([]string, error) {
	if len(paths) == 0 {
		path, err := delveBucketList(ctx, client)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		paths = []string{path}
	} else {
		u, err := url.Parse(paths[0])
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var bucket, prefix string
		bucket = u.Hostname()
		prefix = strings.TrimPrefix(u.Path, "/")

		path, err := delvePrefix(ctx, client, bucket, prefix)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		paths = []string{path}
	}

	return paths, nil
}

func delveBucketList(ctx context.Context, client *s3s.Client) (string, error) {
	buckets, err := client.GetS3Bucket(ctx)
	if err != nil {
		return "", errors.WithStack(err)
	}

	index, err := fuzzyfinder.Find(
		buckets,
		func(i int) string {
			return buckets[i]
		},
	)
	if err != nil {
		return "", errors.WithStack(err)
	}

	return delvePrefix(ctx, client, buckets[index], "")
}

func delvePrefix(ctx context.Context, client *s3s.Client, bucket string, prefix string) (string, error) {
	s3Dirs, err := client.GetS3Dir(ctx, bucket, prefix)
	if err != nil {
		return "", errors.WithStack(err)
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
		return "", errors.WithStack(err)
	}

	switch index {
	case 0:
		parent = path.Join(prefix, "../")
		if parent == "." {
			return delvePrefix(ctx, client, bucket, "")
		}
		if parent == ".." {
			return delveBucketList(ctx, client)
		}
		return delvePrefix(ctx, client, bucket, parent+"/")
	case 1:
		return fmt.Sprintf("s3://%s/%s", bucket, prefix), nil
	default:
		return delvePrefix(ctx, client, bucket, s3Dirs[index])
	}
}
