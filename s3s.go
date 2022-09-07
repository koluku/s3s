package s3s

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type App struct {
	s3client *s3.Client
}

func NewApp(ctx context.Context, region string, maxRetries int) (*App, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.RetryMaxAttempts = maxRetries
		o.RetryMode = aws.RetryModeStandard
	})
	return &App{s3client: s3Client}, nil
}

func GetS3Bucket(ctx context.Context, app *App) ([]string, error) {
	input := &s3.ListBucketsInput{}
	output, err := app.s3client.ListBuckets(ctx, input)
	if err != nil {
		return nil, err
	}

	var s3keys = make([]string, len(output.Buckets))
	for i, content := range output.Buckets {
		s3keys[i] = *content.Name
	}

	return s3keys, nil
}

func GetS3Dir(ctx context.Context, app *App, bucket string, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3client, input)

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

func GetS3Keys(ctx context.Context, app *App, bucket string, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3client, input)

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

func GetS3KeysWithChannel(ctx context.Context, app *App, sender chan<- Path, bucket string, prefix string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	pagenator := s3.NewListObjectsV2Paginator(app.s3client, input)

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

type S3SelectOption struct {
	IsCSV           bool
	FieldDelimiter  string
	RecordDelimiter string
}

func S3Select(ctx context.Context, app *App, bucket string, key string, query string, option *S3SelectOption) error {
	compressionType := suggestCompressionType(key)

	params := &s3.SelectObjectContentInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		ExpressionType:  types.ExpressionTypeSql,
		Expression:      aws.String(query),
		RequestProgress: &types.RequestProgress{},
		InputSerialization: &types.InputSerialization{
			CompressionType: compressionType,
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

type S3SelectQuery struct {
	Value string `json:"-"`
	Count int    `json:"_1"`
}

type S3SelectResult struct {
	Value string `json:"-"`
	Count int    `json:"_1"`
}

func S3SelectWithChannel(ctx context.Context, app *App, bucket string, key string, query string, isCount bool, sender chan<- S3SelectResult, option *S3SelectOption) error {
	compressionType := suggestCompressionType(key)

	params := &s3.SelectObjectContentInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		ExpressionType:  types.ExpressionTypeSql,
		Expression:      aws.String(query),
		RequestProgress: &types.RequestProgress{},
	}
	if option.IsCSV {
		params.InputSerialization = &types.InputSerialization{
			CompressionType: compressionType,
			CSV: &types.CSVInput{
				FieldDelimiter:  aws.String(option.FieldDelimiter),
				RecordDelimiter: aws.String(option.RecordDelimiter),
				FileHeaderInfo:  types.FileHeaderInfoIgnore,
			},
		}
		params.OutputSerialization = &types.OutputSerialization{
			CSV: &types.CSVOutput{},
		}
	} else {
		params.InputSerialization = &types.InputSerialization{
			CompressionType: compressionType,
			JSON: &types.JSONInput{
				Type: types.JSONTypeLines,
			},
		}
		params.OutputSerialization = &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		}
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
			var result S3SelectResult
			if isCount {
				if option.IsCSV {
					result.Count, err = strconv.Atoi(strings.TrimRight(string(v.Value.Payload), "\n"))
					if err != nil {
						return err
					}
				} else {
					if err := json.Unmarshal(v.Value.Payload, &result); err != nil {
						return err
					}
				}
			}
			result.Value = string(v.Value.Payload)
			sender <- result
		}
	}

	if err := stream.Err(); err != nil {
		return err
	}

	return nil
}

func suggestCompressionType(key string) types.CompressionType {
	switch {
	case strings.HasSuffix(key, ".gz"):
		return types.CompressionTypeGzip
	case strings.HasSuffix(key, ".bz2"):
		return types.CompressionTypeBzip2
	default:
		return types.CompressionTypeNone
	}
}
