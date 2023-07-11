package s3s

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/koluku/s3s/internal/schema"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type s3SelectInput struct {
	FormatType FormatType
	Bucket     string
	Key        string
	Query      string
}

func (input *s3SelectInput) toParameter() *s3.SelectObjectContentInput {
	params := &s3.SelectObjectContentInput{
		Bucket:         aws.String(input.Bucket),
		Key:            aws.String(input.Key),
		ExpressionType: types.ExpressionTypeSql,
		Expression:     aws.String(input.Query),
		InputSerialization: &types.InputSerialization{
			CompressionType: input.suggestCompressionType(),
		},
		OutputSerialization: &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		},
	}
	switch input.FormatType {
	case FormatTypeJSON:
		params.InputSerialization.JSON = &types.JSONInput{
			Type: types.JSONTypeLines,
		}
	case FormatTypeCSV:
		params.InputSerialization.CSV = &types.CSVInput{
			FieldDelimiter:  aws.String(","),
			RecordDelimiter: aws.String("\n"),
			FileHeaderInfo:  types.FileHeaderInfoNone,
		}
	case FormatTypeALBLogs:
		params.InputSerialization.CSV = &types.CSVInput{
			FieldDelimiter:  aws.String(" "),
			RecordDelimiter: aws.String("\n"),
			FileHeaderInfo:  types.FileHeaderInfoNone,
		}
	case FormatTypeCFLogs:
		params.InputSerialization.CSV = &types.CSVInput{
			FieldDelimiter:  aws.String("\t"),
			RecordDelimiter: aws.String("\n"),
			FileHeaderInfo:  types.FileHeaderInfoNone,
		}
	}

	return params
}

func (input *s3SelectInput) suggestCompressionType() types.CompressionType {
	switch {
	case strings.HasSuffix(input.Key, ".gz"):
		return types.CompressionTypeGzip
	case strings.HasSuffix(input.Key, ".bz2"):
		return types.CompressionTypeBzip2
	default:
		return types.CompressionTypeNone
	}
}

func (c *Client) s3Select(ctx context.Context, in chan<- []byte, input *s3SelectInput, option *Option) error {
	params := input.toParameter()
	resp, err := c.s3.SelectObjectContent(ctx, params)
	if err != nil {
		return errors.WithStack(err)
	}

	stream := resp.GetStream()
	defer stream.Close()

	pr, pw := io.Pipe()

	eg, egctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		defer pw.Close()
	LOOP:
		for event := range stream.Events() {
			select {
			case <-egctx.Done():
				break LOOP
			default:
				record, ok := event.(*types.SelectObjectContentEventStreamMemberRecords)
				if ok {
					pw.Write(record.Value.Payload)
				}
			}
		}
		return nil
	})

	eg.Go(func() error {
		var lines [][]byte
		decoder := json.NewDecoder(pr)
		for decoder.More() {
			var v json.RawMessage
			if err := decoder.Decode(&v); err != nil {
				return errors.WithStack(err)
			}
			in <- v
		}

		if option.IsCountMode {
			var schema schema.Count
			if err := json.Unmarshal(lines[0], &schema); err != nil {
				return errors.WithStack(err)
			}
			return nil
		}

		return nil
	})

	if err := eg.Wait(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
