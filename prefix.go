package s3s

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
			return nil, err
		}

		pageKeys := make([]string, len(output.CommonPrefixes))
		for i := range output.CommonPrefixes {
			pageKeys[i] = *output.CommonPrefixes[i].Prefix
		}

		s3Keys = append(s3Keys, pageKeys...)
	}

	return s3Keys, nil
}

func GetS3Dir(ctx context.Context, app *App, bucket string, prefix string) ([]string, error) {
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
			return nil, err
		}

		pageKeys := make([]string, len(output.CommonPrefixes))
		for i := range output.CommonPrefixes {
			pageKeys[i] = *output.CommonPrefixes[i].Prefix
		}

		s3Keys = append(s3Keys, pageKeys...)
	}

	return s3Keys, nil
}

func (app *App) GetS3Keys(ctx context.Context, bucket string, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3, input)

	var s3Keys []string
	for pagenator.HasMorePages() {
		output, err := pagenator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		pageKeys := make([]string, output.KeyCount)
		for i := range output.Contents {
			pageKeys[i] = *output.Contents[i].Key
		}

		s3Keys = append(s3Keys, pageKeys...)
	}

	return s3Keys, nil
}

func GetS3Keys(ctx context.Context, app *App, bucket string, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3, input)

	var s3Keys []string
	for pagenator.HasMorePages() {
		output, err := pagenator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		pageKeys := make([]string, output.KeyCount)
		for i := range output.Contents {
			pageKeys[i] = *output.Contents[i].Key
		}

		s3Keys = append(s3Keys, pageKeys...)
	}

	return s3Keys, nil
}

type Path struct {
	Bucket string
	Key    string
}

func (app *App) GetS3KeysWithChannel(ctx context.Context, sender chan<- Path, bucket string, prefix string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3, input)

	for pagenator.HasMorePages() {
		output, err := pagenator.NextPage(ctx)
		if err != nil {
			return err
		}

		for i := range output.Contents {
			sender <- Path{
				Bucket: bucket,
				Key:    *output.Contents[i].Key,
			}
		}
	}

	return nil
}

func GetS3KeysWithChannel(ctx context.Context, app *App, sender chan<- Path, bucket string, prefix string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3, input)

	for pagenator.HasMorePages() {
		output, err := pagenator.NextPage(ctx)
		if err != nil {
			return err
		}

		for i := range output.Contents {
			sender <- Path{
				Bucket: bucket,
				Key:    *output.Contents[i].Key,
			}
		}
	}

	return nil
}
