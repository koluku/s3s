package s3s

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Querying interface {
	toParameter() *s3.SelectObjectContentInput
}

type JSONInput struct {
	Bucket string
	Key    string
	Query  string
}

func (input *JSONInput) toParameter() *s3.SelectObjectContentInput {
	compressionType := suggestCompressionType(input.Key)

	params := &s3.SelectObjectContentInput{
		Bucket:          aws.String(input.Bucket),
		Key:             aws.String(input.Key),
		ExpressionType:  types.ExpressionTypeSql,
		Expression:      aws.String(input.Query),
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

	return params
}

type CSVInput struct {
	Bucket          string
	Key             string
	Query           string
	FieldDelimiter  string
	RecordDelimiter string
}

func (input *CSVInput) toParameter() *s3.SelectObjectContentInput {
	compressionType := suggestCompressionType(input.Key)

	params := &s3.SelectObjectContentInput{
		Bucket:          aws.String(input.Bucket),
		Key:             aws.String(input.Key),
		ExpressionType:  types.ExpressionTypeSql,
		Expression:      aws.String(input.Query),
		RequestProgress: &types.RequestProgress{},
		InputSerialization: &types.InputSerialization{
			CompressionType: compressionType,
			CSV: &types.CSVInput{
				FieldDelimiter:  aws.String(input.FieldDelimiter),
				RecordDelimiter: aws.String(input.RecordDelimiter),
				FileHeaderInfo:  types.FileHeaderInfoNone,
			},
		},
		OutputSerialization: &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		},
	}

	return params
}

type FormatType int

const (
	FormatTypeJSON FormatType = iota
	FormatTypeCSV
	FormatTypeALBLogs
	FormatTypeCFLogs
)

type QueryInfo struct {
	FormatType      FormatType
	FieldDelimiter  string
	RecordDelimiter string
	IsCountMode     bool
}

func (app *App) S3Select(ctx context.Context, input Querying, info *QueryInfo) error {
	params := input.toParameter()
	resp, err := app.s3.SelectObjectContent(ctx, params)
	if err != nil {
		return err
	}
	stream := resp.GetStream()
	defer stream.Close()

	for event := range stream.Events() {
		record, ok := event.(*types.SelectObjectContentEventStreamMemberRecords)
		if ok {
			if info.FormatType == FormatTypeALBLogs {
				var alblogs ALBLogsSchema
				if err := json.Unmarshal(record.Value.Payload, &alblogs); err != nil {
					return err
				}
				if info.IsCountMode {
					if err := json.Unmarshal(record.Value.Payload, &alblogs); err != nil {
						return err
					}
				} else {
					buf, err := json.Marshal(alblogs)
					if err != nil {
						return err
					}

					fmt.Println(string(buf))
				}
			} else if info.FormatType == FormatTypeCFLogs {
				var cflogs CFLogsSchema
				if err := json.Unmarshal(record.Value.Payload, &cflogs); err != nil {
					return err
				}

				if info.IsCountMode {
					if err := json.Unmarshal(record.Value.Payload, &cflogs); err != nil {
						return err
					}
				} else {
					buf, err := json.Marshal(cflogs)
					if err != nil {
						return err
					}

					fmt.Println(string(buf))
				}
			} else {
				fmt.Print(string(record.Value.Payload))
			}
		}
	}

	if err := stream.Err(); err != nil {
		return err
	}

	return nil
}

type ALBLogsSchema struct {
	Type                   interface{} `json:"type"`
	Time                   interface{} `json:"time"`
	Elb                    interface{} `json:"elb"`
	ClientPort             interface{} `json:"client:port"`
	TargetPort             interface{} `json:"target:port"`
	RequestProcessingTime  interface{} `json:"request_processing_time"`
	TargetProcessingTime   interface{} `json:"target_processing_time"`
	ResponseProcessingTime interface{} `json:"response_processing_time"`
	ElbStatusCode          interface{} `json:"elb_status_code"`
	TargetStatusCode       interface{} `json:"target_status_code"`
	ReceivedBytes          interface{} `json:"received_bytes"`
	SentBytes              interface{} `json:"sent_bytes"`
	Request                interface{} `json:"request"`
	UserAgent              interface{} `json:"user_agent"`
	SslCipher              interface{} `json:"ssl_cipher"`
	SslProtocol            interface{} `json:"ssl_protocol"`
	TargetGroupArn         interface{} `json:"target_group_arn"`
	TraceId                interface{} `json:"trace_id"`
	ChosenCertArn          interface{} `json:"chosen_cert_arn"`
	MatchedRulePriority    interface{} `json:"matched_rule_priority"`
	RequestCreationTime    interface{} `json:"request_creation_time"`
	ActionsExecuted        interface{} `json:"actions_executed"`
	RedirectUrl            interface{} `json:"redirect_url"`
	ErrorReason            interface{} `json:"error_reason"`
	TargetPortList         interface{} `json:"target:port_list"`
	TargetStatusCodeList   interface{} `json:"target_status_code_list"`
	Classification         interface{} `json:"classification"`
	ClassificationReason   interface{} `json:"classification_reason"`
}

func (schema *ALBLogsSchema) UnmarshalJSON(b []byte) error {
	raw := map[string]interface{}{}
	err := json.Unmarshal(b, &raw)
	if err != nil {
		return err
	}

	schema.Type = raw["_1"]
	schema.Time = raw["_2"]
	schema.Elb = raw["_3"]
	schema.ClientPort = raw["_4"]
	schema.TargetPort = raw["_5"]
	schema.RequestProcessingTime = raw["_6"]
	schema.TargetProcessingTime = raw["_7"]
	schema.ResponseProcessingTime = raw["_8"]
	schema.ElbStatusCode = raw["_9"]
	schema.TargetStatusCode = raw["_10"]
	schema.ReceivedBytes = raw["_11"]
	schema.SentBytes = raw["_12"]
	schema.Request = raw["_13"]
	schema.UserAgent = raw["_14"]
	schema.SslCipher = raw["_15"]
	schema.SslProtocol = raw["_16"]
	schema.TargetGroupArn = raw["_17"]
	schema.TraceId = raw["_18"]
	schema.ChosenCertArn = raw["_19"]
	schema.MatchedRulePriority = raw["_20"]
	schema.RequestCreationTime = raw["_21"]
	schema.ActionsExecuted = raw["_22"]
	schema.RedirectUrl = raw["_23"]
	schema.ErrorReason = raw["_24"]
	schema.TargetPortList = raw["_25"]
	schema.TargetStatusCodeList = raw["_26"]
	schema.Classification = raw["_27"]
	schema.ClassificationReason = raw["_28"]

	return nil
}

type CFLogsSchema struct {
	Date                    interface{} `json:"date"`
	Time                    interface{} `json:"time"`
	XEdgeLocation           interface{} `json:"x-edge-location"`
	ScBytes                 interface{} `json:"sc-bytes"`
	CIp                     interface{} `json:"c-ip"`
	CsMethod                interface{} `json:"cs-method"`
	CsHost                  interface{} `json:"cs(Host)"`
	CsUriStem               interface{} `json:"cs-uri-stem"`
	ScStatus                interface{} `json:"sc-status"`
	CsReferer               interface{} `json:"cs(Referer)"`
	CsUserAgent             interface{} `json:"cs(User-Agent)"`
	CsUriQuery              interface{} `json:"cs-uri-query"`
	CsCookie                interface{} `json:"cs(Cookie)"`
	XEdgeResultType         interface{} `json:"x-edge-result-type"`
	XEdgeRequestId          interface{} `json:"x-edge-request-id"`
	XHostHeader             interface{} `json:"x-host-header"`
	CsProtocol              interface{} `json:"cs-protocol"`
	CsBytes                 interface{} `json:"cs-bytes"`
	TimeTaken               interface{} `json:"time-taken"`
	XForwardedFor           interface{} `json:"x-forwarded-for"`
	SslProtocol             interface{} `json:"ssl-protocol"`
	SslCipher               interface{} `json:"ssl-cipher"`
	XEdgeResponseResultType interface{} `json:"x-edge-response-result-type"`
	CsProtocolVersion       interface{} `json:"cs-protocol-version"`
	FleStatus               interface{} `json:"fle-status"`
	FleEncryptedFields      interface{} `json:"fle-encrypted-fields"`
	CPort                   interface{} `json:"c-port"`
	TimeToFirstByte         interface{} `json:"time-to-first-byte"`
	XEdgeDetailedResultType interface{} `json:"x-edge-detailed-result-type"`
	ScContentType           interface{} `json:"sc-content-type"`
	ScContentLen            interface{} `json:"sc-content-len"`
	ScRangeStart            interface{} `json:"sc-range-start"`
	ScRangeEnd              interface{} `json:"sc-range-end"`
}

func (schema *CFLogsSchema) UnmarshalJSON(b []byte) error {
	raw := map[string]interface{}{}
	err := json.Unmarshal(b, &raw)
	if err != nil {
		return err
	}

	schema.Date = raw["_1"]
	schema.Time = raw["_2"]
	schema.XEdgeLocation = raw["_3"]
	schema.ScBytes = raw["_4"]
	schema.CIp = raw["_5"]
	schema.CsMethod = raw["_6"]
	schema.CsHost = raw["_7"]
	schema.CsUriStem = raw["_8"]
	schema.ScStatus = raw["_9"]
	schema.CsReferer = raw["_10"]
	schema.CsUserAgent = raw["_11"]
	schema.CsUriQuery = raw["_12"]
	schema.CsCookie = raw["_13"]
	schema.XEdgeResultType = raw["_14"]
	schema.XEdgeRequestId = raw["_15"]
	schema.XHostHeader = raw["_16"]
	schema.CsProtocol = raw["_17"]
	schema.CsBytes = raw["_18"]
	schema.TimeTaken = raw["_19"]
	schema.XForwardedFor = raw["_20"]
	schema.SslProtocol = raw["_21"]
	schema.SslCipher = raw["_22"]
	schema.XEdgeResponseResultType = raw["_23"]
	schema.CsProtocolVersion = raw["_24"]
	schema.FleStatus = raw["_25"]
	schema.FleEncryptedFields = raw["_26"]
	schema.CPort = raw["_27"]
	schema.TimeToFirstByte = raw["_28"]
	schema.XEdgeDetailedResultType = raw["_29"]
	schema.ScContentType = raw["_30"]
	schema.ScContentLen = raw["_31"]
	schema.ScRangeStart = raw["_32"]
	schema.ScRangeEnd = raw["_33"]

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
