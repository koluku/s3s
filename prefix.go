package s3s

import (
	"context"
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
			switch info.KeyType {
			case KeyTypeALB:
				if isTimeZeroRange(info.Since, info.Until) {
					sender <- ObjectInfo{
						Bucket: bucket,
						Key:    *output.Contents[i].Key,
						Size:   output.Contents[i].Size,
					}
					continue
				}

				endTime, err := getALBKeyEndTime(*output.Contents[i].Key)
				if err != nil {
					return errors.WithStack(err)
				}
				if isTimeWithin(endTime, info.Since, info.Until) {
					sender <- ObjectInfo{
						Bucket: bucket,
						Key:    *output.Contents[i].Key,
						Size:   output.Contents[i].Size,
					}
					continue
				}
			default:
				sender <- ObjectInfo{
					Bucket: bucket,
					Key:    *output.Contents[i].Key,
					Size:   output.Contents[i].Size,
				}
			}
		}
	}

	return nil
}
