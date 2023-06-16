package s3s

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	DEFAULT_THREAD_COUNT = 150
)

type Client struct {
	s3 *s3.Client
}

func New(ctx context.Context, profile string, region string) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	api := s3.NewFromConfig(cfg)

	client := &Client{
		s3: api,
	}

	return client, nil
}

func (c *Client) Run(ctx context.Context, paths []string, keyInfo *KeyInfo, queryStr string, queryInfo *QueryInfo) error {
	switch keyInfo.KeyType {
	case KeyTypeALB:
		albPaths, err := c.OptimizateALBPaths(ctx, paths, keyInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		if albPaths != nil {
			paths = albPaths
		}
	case KeyTypeCF:
		cfPaths, err := c.OptimizateCFPaths(ctx, paths, keyInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		if cfPaths != nil {
			paths = cfPaths
		}
	}

	ch := make(chan ObjectInfo, DEFAULT_THREAD_COUNT)
	eg, egctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		if err := c.getBucketKeys(egctx, ch, paths, keyInfo); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
	eg.Go(func() error {
		if err := c.execS3Select(egctx, ch, queryStr, queryInfo); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (c *Client) DryRun(ctx context.Context, paths []string, keyInfo *KeyInfo, queryStr string, queryInfo *QueryInfo) (int64, int, error) {
	switch keyInfo.KeyType {
	case KeyTypeALB:
		albPaths, err := c.OptimizateALBPaths(ctx, paths, keyInfo)
		if err != nil {
			return 0, 0, errors.WithStack(err)
		}
		if albPaths != nil {
			paths = albPaths
		}
	case KeyTypeCF:
		cfPaths, err := c.OptimizateCFPaths(ctx, paths, keyInfo)
		if err != nil {
			return 0, 0, errors.WithStack(err)
		}
		if cfPaths != nil {
			paths = cfPaths
		}
	}

	var scanByte int64
	var count int
	ch := make(chan ObjectInfo, DEFAULT_THREAD_COUNT)

	eg, egctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := c.getBucketKeys(egctx, ch, paths, keyInfo); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
	eg.Go(func() error {
		for r := range ch {
			scanByte += r.Size
			count++
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return 0, 0, errors.WithStack(err)
	}

	return scanByte, count, nil
}

func (c *Client) getBucketKeys(ctx context.Context, ch chan<- ObjectInfo, paths []string, info *KeyInfo) error {
	defer close(ch)

	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(DEFAULT_THREAD_COUNT)
	for _, path := range paths {
		path := path
		eg.Go(func() error {
			u, err := url.Parse(path)
			if err != nil {
				return errors.WithStack(err)
			}
			var bucket, prefix string
			bucket = u.Hostname()
			prefix = strings.TrimPrefix(u.Path, "/")

			if c.GetS3Keys(egctx, ch, bucket, prefix, info); err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (c *Client) execS3Select(ctx context.Context, reciever <-chan ObjectInfo, queryStr string, info *QueryInfo) error {
	var count int
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(DEFAULT_THREAD_COUNT)

	for r := range reciever {
		bucket := r.Bucket
		key := r.Key

		var input Querying
		switch {
		case info.FormatType == FormatTypeCSV:
			input = &CSVInput{
				Bucket:          bucket,
				Key:             key,
				Query:           queryStr,
				FieldDelimiter:  info.FieldDelimiter,
				RecordDelimiter: info.RecordDelimiter,
			}
		case info.FormatType == FormatTypeALBLogs:
			input = &CSVInput{
				Bucket:          bucket,
				Key:             key,
				Query:           queryStr,
				FieldDelimiter:  info.FieldDelimiter,
				RecordDelimiter: info.RecordDelimiter,
			}
		case info.FormatType == FormatTypeCFLogs:
			input = &CSVInput{
				Bucket:          bucket,
				Key:             key,
				Query:           queryStr,
				FieldDelimiter:  info.FieldDelimiter,
				RecordDelimiter: info.RecordDelimiter,
			}
		default:
			input = &JSONInput{
				Bucket: bucket,
				Key:    key,
				Query:  queryStr,
			}
		}

		eg.Go(func() error {
			result, err := c.S3Select(egctx, input, info)
			if err != nil {
				return errors.WithStack(err)
			}
			count += result.Count
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.WithStack(err)
	}

	if info.IsCountMode {
		fmt.Println(count)
	}

	return nil
}
