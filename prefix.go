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

func (c *Client) GetS3Dir(ctx context.Context, bucket string, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	pagenator := s3.NewListObjectsV2Paginator(c.s3, input)

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

type s3Object struct {
	Bucket string
	Key    string
	Size   int64
}

func (c *Client) GetS3OneKey(ctx context.Context, bucket string, prefix string) (*s3Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: 1,
	}

	output, err := c.s3.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &s3Object{
		Bucket: bucket,
		Key:    *output.Contents[0].Key,
		Size:   output.Contents[0].Size,
	}, nil
}

func (c *Client) GetS3Keys(ctx context.Context, sender chan<- s3Object, bucket string, prefix string, info *Query) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	pagenator := s3.NewListObjectsV2Paginator(c.s3, input)

	for pagenator.HasMorePages() {
		output, err := pagenator.NextPage(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		for i := range output.Contents {
			sender <- s3Object{
				Bucket: bucket,
				Key:    *output.Contents[i].Key,
				Size:   output.Contents[i].Size,
			}
		}
	}

	return nil
}

func (c *Client) OptimizateALBPrefixes(ctx context.Context, prefixes []string, keyInfo *Query) ([]string, error) {
	if keyInfo.FormatType != FormatTypeALBLogs {
		return nil, nil
	}
	if isTimeZeroRange(keyInfo.Since, keyInfo.Until) {
		return nil, nil
	}

	var newPrefixes []string
	for _, prefix := range prefixes {
		u, err := url.Parse(prefix)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var bucket, prefix string
		bucket = u.Hostname()
		prefix = strings.TrimPrefix(u.Path, "/")
		oi, err := c.GetS3OneKey(ctx, bucket, prefix)
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
				newPrefixes = append(newPrefixes, fmt.Sprintf("s3://%s/%s%s%s_%s", bucket, prefixA, since.Format("2006/01/02"), prefixB, since.Format("20060102")))
				since = since.Add(time.Hour * 24)
			} else if delta >= time.Hour {
				newPrefixes = append(newPrefixes, fmt.Sprintf("s3://%s/%s%s%s_%s", bucket, prefixA, since.Format("2006/01/02"), prefixB, since.Format("20060102T15")))
				since = since.Add(time.Hour)
			} else {
				newPrefixes = append(newPrefixes, fmt.Sprintf("s3://%s/%s%s%s_%s", bucket, prefixA, since.Format("2006/01/02"), prefixB, since.Format("20060102T1504Z")))
				since = since.Add(time.Minute * 5)
			}
		}
	}

	return newPrefixes, nil
}

func (c *Client) OptimizateCFPrefixes(ctx context.Context, prefixes []string, keyInfo *Query) ([]string, error) {
	if keyInfo.FormatType != FormatTypeCFLogs {
		return nil, nil
	}
	if isTimeZeroRange(keyInfo.Since, keyInfo.Until) {
		return nil, nil
	}

	var newPrefixes []string
	for _, prefix := range prefixes {
		u, err := url.Parse(prefix)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var bucket, prefix string
		bucket = u.Hostname()
		prefix = strings.TrimPrefix(u.Path, "/")
		oi, err := c.GetS3OneKey(ctx, bucket, prefix)
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
				newPrefixes = append(newPrefixes, fmt.Sprintf("s3://%s/%s%s", bucket, distribution, since.Format("2006-01-02")))
				since = since.Add(time.Hour * 24)
			} else {
				newPrefixes = append(newPrefixes, fmt.Sprintf("s3://%s/%s%s", bucket, distribution, since.Format("2006-01-02-15.")))
				since = since.Add(time.Hour)
			}
		}
	}

	return newPrefixes, nil
}
