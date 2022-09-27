package s3s

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type App struct {
	threadCount int
	s3          *s3.Client
}

func NewApp(ctx context.Context, region string, maxRetries int, threadCount int) (*App, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.RetryMaxAttempts = maxRetries
		o.RetryMode = aws.RetryModeStandard
	})

	app := &App{
		threadCount: threadCount,
		s3:          client,
	}

	return app, nil
}

func (app *App) Run(ctx context.Context, paths []string, keyInfo *KeyInfo, queryStr string, queryInfo *QueryInfo) error {
	ch := make(chan ObjectInfo, app.threadCount)
	eg, egctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		if err := app.getBucketKeys(egctx, ch, paths, keyInfo); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
	eg.Go(func() error {
		if err := app.execS3Select(egctx, ch, queryStr, queryInfo); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (app *App) DryRun(ctx context.Context, paths []string, keyInfo *KeyInfo, queryStr string, queryInfo *QueryInfo) (int64, int, error) {
	ch := make(chan ObjectInfo, app.threadCount)

	var scanByte int64
	var count int

	if keyInfo.KeyType == KeyTypeCF && !isTimeZeroRange(keyInfo.Since, keyInfo.Until) {
		if !keyInfo.Since.IsZero() && keyInfo.Until.IsZero() {
			return 0, 0, errors.WithStack(fmt.Errorf("since only will too many logs hit"))
		}

		var npaths []string
		for _, path := range paths {
			u, err := url.Parse(path)
			if err != nil {
				return 0, 0, errors.WithStack(err)
			}
			var bucket, prefix string
			bucket = u.Hostname()
			prefix = strings.TrimPrefix(u.Path, "/")
			oi, err := app.GetS3OneKey(ctx, bucket, prefix)
			if err != nil {
				return 0, 0, errors.WithStack(err)
			}

			rep := regexp.MustCompile(`/?.+?\.`)
			distribution := rep.FindString(oi.Key)
			distribution = strings.TrimPrefix(distribution, "/")
			paths = nil
			t := keyInfo.Since
			s := keyInfo.Until
			if s.IsZero() {
				s = time.Now()
			}
			for {
				if t.After(s) {
					break
				}
				abs := s.Sub(t)
				if abs > time.Hour*24 {
					npaths = append(npaths, fmt.Sprintf("s3://%s/%s%s", oi.Bucket, distribution, t.Format("2006-01-02")))
					t = t.Add(time.Hour * 24)
				} else {
					npaths = append(npaths, fmt.Sprintf("s3://%s/%s%s", oi.Bucket, distribution, t.Format("2006-01-02-15.")))
					t = t.Add(time.Hour)
				}
			}
		}
		paths = npaths
	}

	eg, egctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := app.getBucketKeys(egctx, ch, paths, keyInfo); err != nil {
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

func (app *App) getBucketKeys(ctx context.Context, ch chan<- ObjectInfo, paths []string, info *KeyInfo) error {
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(app.threadCount)
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

			if app.GetS3Keys(egctx, ch, bucket, prefix, info); err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	}

	err := eg.Wait()
	close(ch)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (app *App) execS3Select(ctx context.Context, reciever <-chan ObjectInfo, queryStr string, info *QueryInfo) error {
	var count int
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(app.threadCount)

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
			result, err := app.S3Select(egctx, input, info)
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
