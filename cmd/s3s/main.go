package main

import (
	"context"
	"flag"
	"log"
	"net/url"
	"os"
	"strings"

	s3s "github.com/koluku/s3select"
)

var (
	// AWS
	region string

	// S3 Select Query
	query string
)

func init() {
	// AWS
	flag.StringVar(&region, "region", "", "region")

	// S3 Select Query
	flag.StringVar(&query, "query", "SELECT * FROM S3Object s", "query")
}

type bucketKeys struct {
	bucket string
	keys   []string
}

func cmd() int {
	flag.Parse()
	paths := flag.Args()

	if len(paths) == 0 {
		log.Println("[ERROR]", "no arguments")
		return 1
	}

	ctx := context.TODO()
	app, err := s3s.NewApp(ctx)
	if err != nil {
		log.Println("[ERROR]", err)
		return 1
	}

	// Get S3 Object Keys
	targetBucketKeys := []bucketKeys{}
	for _, path := range paths {
		u, err := url.Parse(path)
		if err != nil {
			log.Println("[ERROR]", err)
			return 1
		}
		var bucket, prefix string
		bucket = u.Hostname()
		prefix = strings.TrimPrefix(u.Path, "/")
		prefix = strings.TrimSuffix(prefix, "/")
		s3Keys, err := s3s.GetS3Keys(ctx, app, bucket, prefix)
		if err != nil {
			log.Println("[ERROR]", err)
			return 1
		}
		targetBucketKeys = append(targetBucketKeys, bucketKeys{bucket: bucket, keys: s3Keys})
	}

	for _, bk := range targetBucketKeys {
		for _, key := range bk.keys {
			if err := s3s.S3Select(ctx, app, bk.bucket, key, query); err != nil {
				log.Println("[ERROR]", err)
				return 1
			}
		}
	}

	return 0
}

func main() {
	os.Exit(cmd())
}
