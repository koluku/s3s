package s3s

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

func (app *App) GetS3Bucket(ctx context.Context) ([]string, error) {
	input := &s3.ListBucketsInput{}
	output, err := app.s3.ListBuckets(ctx, input)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var s3keys = make([]string, len(output.Buckets))
	for i, content := range output.Buckets {
		s3keys[i] = *content.Name
	}

	return s3keys, nil
}
