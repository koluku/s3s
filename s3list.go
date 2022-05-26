package s3s

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func GetS3Keys(ctx context.Context, app *App, bucket string, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	paginator := s3.NewListObjectsV2Paginator(app.s3client, input)

	s3keys := []string{}
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, content := range output.Contents {
			s3keys = append(s3keys, *content.Key)
		}
	}

	return s3keys, nil
}
