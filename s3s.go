package s3s

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	DEFAULT_THREAD_COUNT = 150
)

type FormatType int

const (
	FormatTypeJSON FormatType = iota + 1
	FormatTypeCSV
	FormatTypeALBLogs
	FormatTypeCFLogs
)

type Query struct {
	FormatType FormatType
	Query      string
	Since      time.Time
	Until      time.Time
}

type Option struct {
	IsDryRun    bool
	IsCountMode bool
}

type Client struct {
	s3 *s3.Client
}

func New(ctx context.Context) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	api := s3.NewFromConfig(cfg)

	client := &Client{
		s3: api,
	}

	return client, nil
}

type Result struct {
	Count int
	Bytes int64
}

func (c *Client) Run(ctx context.Context, prefixes []string, query *Query, option *Option) (*Result, error) {
	result := &Result{}

	switch query.FormatType {
	case FormatTypeALBLogs:
		albPrefixes, err := c.OptimizateALBPrefixes(ctx, prefixes, query)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if albPrefixes != nil {
			prefixes = albPrefixes
		}
	case FormatTypeCFLogs:
		cfPrefixes, err := c.OptimizateCFPrefixes(ctx, prefixes, query)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if cfPrefixes != nil {
			prefixes = cfPrefixes
		}
	}

	pathCH := make(chan s3Object, DEFAULT_THREAD_COUNT)
	eg, egctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		if err := c.getBucketKeys(egctx, pathCH, prefixes, query); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	jsonCH := make(chan []byte, DEFAULT_THREAD_COUNT)

	if !option.IsDryRun {
		eg.Go(func() error {
			if err := c.execS3Select(egctx, pathCH, jsonCH, query, option); err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	} else {
		eg.Go(func() error {
			for c := range pathCH {
				result.Bytes += c.Size
				result.Count++
			}
			return nil
		})
	}

	if !option.IsDryRun {
		eg.Go(func() error {
			if err := c.writeOutput(egctx, jsonCH); err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, errors.WithStack(err)
	}

	return result, nil
}

func (c *Client) getBucketKeys(ctx context.Context, in chan<- s3Object, prefixes []string, info *Query) error {
	defer close(in)

	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(DEFAULT_THREAD_COUNT)
	for _, prefix := range prefixes {
		prefix := prefix
		eg.Go(func() error {
			u, err := url.Parse(prefix)
			if err != nil {
				return errors.WithStack(err)
			}
			bucket := u.Hostname()
			newPrefix := strings.TrimPrefix(u.Path, "/")

			if c.GetS3Keys(egctx, in, bucket, newPrefix, info); err != nil {
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

func (c *Client) execS3Select(ctx context.Context, out <-chan s3Object, in chan<- []byte, query *Query, option *Option) error {
	defer close(in)

	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(DEFAULT_THREAD_COUNT)

LOOP:
	for {
		select {
		case s3object, ok := <-out:
			if !ok {
				break LOOP
			}

			var input *s3SelectInput
			switch query.FormatType {
			case FormatTypeJSON:
				input = &s3SelectInput{
					Bucket: s3object.Bucket,
					Key:    s3object.Key,
					Query:  query.Query,
				}
			case FormatTypeCSV, FormatTypeALBLogs, FormatTypeCFLogs:
				input = &s3SelectInput{
					Bucket: s3object.Bucket,
					Key:    s3object.Key,
					Query:  query.Query,
				}
			}
			input.FormatType = query.FormatType

			eg.Go(func() error {
				if err := c.s3Select(egctx, in, input, option); err != nil {
					return errors.WithStack(err)
				}
				return nil
			})
		case <-ctx.Done():
			return nil
		}
	}

	if err := eg.Wait(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (c *Client) writeOutput(ctx context.Context, out <-chan []byte) error {
	for {
		select {
		case json, ok := <-out:
			if !ok {
				return nil
			}

			fmt.Println(string(json))
		case <-ctx.Done():
			return nil
		}
	}
}
