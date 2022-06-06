package s3s

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	SelectObjectContent(ctx context.Context, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error)
}
type App struct {
	s3client S3API
}

func NewApp(ctx context.Context, region string) (*App, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(cfg)
	return &App{s3client: s3Client}, nil
}

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

func S3Select(ctx context.Context, app *App, bucket string, key string, query string) error {
	params := &s3.SelectObjectContentInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		ExpressionType:  types.ExpressionTypeSql,
		Expression:      aws.String(query),
		RequestProgress: &types.RequestProgress{},
		InputSerialization: &types.InputSerialization{
			CompressionType: types.CompressionTypeGzip,
			JSON: &types.JSONInput{
				Type: types.JSONTypeLines,
			},
		},
		OutputSerialization: &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		},
	}

	resp, err := app.s3client.SelectObjectContent(ctx, params)
	if err != nil {
		return err
	}
	stream := resp.GetStream()
	defer stream.Close()

	for event := range stream.Events() {
		v, ok := event.(*types.SelectObjectContentEventStreamMemberRecords)
		if ok {
			value := string(v.Value.Payload)
			fmt.Print(value)
		}
	}

	if err := stream.Err(); err != nil {
		return err
	}

	return nil
}
