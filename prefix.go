package s3s

import (
	"context"
	"log"
	"regexp"
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
			switch info.KeyType {
			case KeyTypeNone:
				sender <- ObjectInfo{
					Bucket: bucket,
					Key:    *output.Contents[i].Key,
					Size:   output.Contents[i].Size,
				}
			case KeyTypeALB:
				if isTimeZeroRange(info.Since, info.Until) {
					sender <- ObjectInfo{
						Bucket: bucket,
						Key:    *output.Contents[i].Key,
						Size:   output.Contents[i].Size,
					}
					continue
				}
				if isTimeWithin(*output.Contents[i].Key, info.Since, info.Until) {
					sender <- ObjectInfo{
						Bucket: bucket,
						Key:    *output.Contents[i].Key,
						Size:   output.Contents[i].Size,
					}
					continue
				}
			case KeyTypeCF:
			}
		}
	}

	return nil
}

func isTimeZeroRange(since time.Time, until time.Time) bool {
	return since.IsZero() && until.IsZero()
}

func isTimeWithin(key string, since time.Time, until time.Time) bool {
	rep := regexp.MustCompile(`_\d{8}T\d{4}Z_`)
	timeStr := rep.FindString(key)

	t, err := time.Parse("_20060102T1504Z_", timeStr)
	if err != nil {
		log.Fatalf("%+v\n%+v\n%+v\n", key, timeStr, err)
	}

	if !since.IsZero() && t.Before(since) {
		return false
	}
	if !until.IsZero() && t.After(until) {
		return false
	}
	return true
}
