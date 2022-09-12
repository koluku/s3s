package s3s

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (app *App) GetS3Bucket(ctx context.Context) ([]string, error) {
	input := &s3.ListBucketsInput{}
	output, err := app.s3.ListBuckets(ctx, input)
	if err != nil {
		return nil, err
	}

	var s3keys = make([]string, len(output.Buckets))
	for i, content := range output.Buckets {
		s3keys[i] = *content.Name
	}

	return s3keys, nil
}

func GetS3Bucket(ctx context.Context, app *App) ([]string, error) {
	input := &s3.ListBucketsInput{}
	output, err := app.s3.ListBuckets(ctx, input)
	if err != nil {
		return nil, err
	}

	var s3keys = make([]string, len(output.Buckets))
	for i, content := range output.Buckets {
		s3keys[i] = *content.Name
	}

	return s3keys, nil
}
