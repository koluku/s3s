package schema

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type CFLogs struct {
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

func (schema *CFLogs) UnmarshalJSON(b []byte) error {
	raw := map[string]interface{}{}
	err := json.Unmarshal(b, &raw)
	if err != nil {
		return errors.WithStack(err)
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
