package s3s

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3SelectOption struct {
	IsCSV           bool
	FieldDelimiter  string
	RecordDelimiter string
}

func (app *App) S3Select(ctx context.Context, bucket string, key string, query string, option *S3SelectOption) error {
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

	resp, err := app.s3.SelectObjectContent(ctx, params)
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

func (app *App) S3SelectWithChannel(ctx context.Context, bucket string, key string, query string, isCount bool, sender chan<- S3SelectResult, option *S3SelectOption) error {
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

	resp, err := app.s3.SelectObjectContent(ctx, params)
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
