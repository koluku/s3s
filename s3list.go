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
	output, err := app.s3client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, err
	}

	var s3keys = make([]string, len(output.Contents))
	for i, content := range output.Contents {
		s3keys[i] = *content.Key
	}

	return s3keys, nil
}
