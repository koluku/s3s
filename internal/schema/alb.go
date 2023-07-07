package schema

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type ALBLogs struct {
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
	DomainName             interface{} `json:"domain_name"`
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

func (schema *ALBLogs) UnmarshalJSON(b []byte) error {
	raw := map[string]interface{}{}
	err := json.Unmarshal(b, &raw)
	if err != nil {
		return errors.WithStack(err)
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
	schema.DomainName = raw["_19"]
	schema.ChosenCertArn = raw["_20"]
	schema.MatchedRulePriority = raw["_21"]
	schema.RequestCreationTime = raw["_22"]
	schema.ActionsExecuted = raw["_23"]
	schema.RedirectUrl = raw["_24"]
	schema.ErrorReason = raw["_25"]
	schema.TargetPortList = raw["_26"]
	schema.TargetStatusCodeList = raw["_27"]
	schema.Classification = raw["_28"]
	schema.ClassificationReason = raw["_29"]

	return nil
}
