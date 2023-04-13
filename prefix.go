package s3s

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

func (app *App) GetS3Dir(ctx context.Context, bucket string, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3, input)

	var s3Keys []string
	for pagenator.HasMorePages() {
		output, err := pagenator.NextPage(ctx)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if len(output.CommonPrefixes) == 0 {
			break
		}

		pageKeys := make([]string, len(output.CommonPrefixes))
		for i := range output.CommonPrefixes {
			pageKeys[i] = *output.CommonPrefixes[i].Prefix
		}

		s3Keys = append(s3Keys, pageKeys...)
	}

	return s3Keys, nil
}

type KeyType int

const (
	KeyTypeNone KeyType = iota
	KeyTypeALB
	KeyTypeCF
)

type KeyInfo struct {
	KeyType KeyType
	Since   time.Time
	Until   time.Time
}

type ObjectInfo struct {
	Bucket string
	Key    string
	Size   int64
}

func (app *App) GetS3OneKey(ctx context.Context, bucket string, prefix string) (*ObjectInfo, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: 1,
	}
	output, err := app.s3.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &ObjectInfo{
		Bucket: bucket,
		Key:    *output.Contents[0].Key,
		Size:   output.Contents[0].Size,
	}, nil
}

func (app *App) GetS3Keys(ctx context.Context, sender chan<- ObjectInfo, bucket string, prefix string, info *KeyInfo) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3, input)

	for pagenator.HasMorePages() {
		output, err := pagenator.NextPage(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		for i := range output.Contents {
			sender <- ObjectInfo{
				Bucket: bucket,
				Key:    *output.Contents[i].Key,
				Size:   output.Contents[i].Size,
			}
		}
	}

	return nil
}

func (app *App) OptimizateALBPaths(ctx context.Context, paths []string, keyInfo *KeyInfo) ([]string, error) {
	if keyInfo.KeyType != KeyTypeALB || isTimeZeroRange(keyInfo.Since, keyInfo.Until) {
		return nil, nil
	}

	var npaths []string
	for _, path := range paths {
		u, err := url.Parse(path)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var bucket, prefix string
		bucket = u.Hostname()
		prefix = strings.TrimPrefix(u.Path, "/")
		oi, err := app.GetS3OneKey(ctx, bucket, prefix)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		rep := regexp.MustCompile(`(^.*)\d{4}/\d{2}/\d{2}(.*)_\d{8}T\d{4}Z_`)
		submatches := rep.FindStringSubmatch(oi.Key)
		if len(submatches) == 0 {
			return nil, fmt.Errorf("non-match alb path")
		}
		prefixA := submatches[1]
		prefixB := submatches[2]

		since := roundUpTime(keyInfo.Since, time.Minute*5)
		until := roundUpTime(keyInfo.Until, time.Minute*5)

		for {
			if since.After(until) {
				break
			}
			delta := until.Sub(since)
			if delta >= time.Hour*24 {
				npaths = append(npaths, fmt.Sprintf("s3://%s/%s%s%s_%s", bucket, prefixA, since.Format("2006/01/02"), prefixB, since.Format("20060102")))
				since = since.Add(time.Hour * 24)
			} else if delta >= time.Hour {
				npaths = append(npaths, fmt.Sprintf("s3://%s/%s%s%s_%s", bucket, prefixA, since.Format("2006/01/02"), prefixB, since.Format("20060102T15")))
				since = since.Add(time.Hour)
			} else {
				npaths = append(npaths, fmt.Sprintf("s3://%s/%s%s%s_%s", bucket, prefixA, since.Format("2006/01/02"), prefixB, since.Format("20060102T1504Z")))
				since = since.Add(time.Minute * 5)
			}
		}
	}

	return npaths, nil
}

func (app *App) OptimizateCFPaths(ctx context.Context, paths []string, keyInfo *KeyInfo) ([]string, error) {
	if keyInfo.KeyType != KeyTypeCF || isTimeZeroRange(keyInfo.Since, keyInfo.Until) {
		return nil, nil
	}

	var npaths []string
	for _, path := range paths {
		u, err := url.Parse(path)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var bucket, prefix string
		bucket = u.Hostname()
		prefix = strings.TrimPrefix(u.Path, "/")
		oi, err := app.GetS3OneKey(ctx, bucket, prefix)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		rep := regexp.MustCompile(`/?.+?\.`)
		distribution := rep.FindString(oi.Key)
		distribution = strings.TrimPrefix(distribution, "/")

		since := keyInfo.Since
		until := keyInfo.Until
		for {
			if since.After(until) {
				break
			}
			delta := until.Sub(since)
			if delta >= time.Hour*24 {
				npaths = append(npaths, fmt.Sprintf("s3://%s/%s%s", bucket, distribution, since.Format("2006-01-02")))
				since = since.Add(time.Hour * 24)
			} else {
				npaths = append(npaths, fmt.Sprintf("s3://%s/%s%s", bucket, distribution, since.Format("2006-01-02-15.")))
				since = since.Add(time.Hour)
			}
		}
	}

	return npaths, nil
}
