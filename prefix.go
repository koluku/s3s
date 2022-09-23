package s3s

import (
	"context"

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

type ObjectInfo struct {
	Bucket string
	Key    string
	Size   int64
}

func (app *App) GetS3Keys(ctx context.Context, sender chan<- ObjectInfo, bucket string, prefix string) error {
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
