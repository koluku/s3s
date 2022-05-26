package s3s

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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
