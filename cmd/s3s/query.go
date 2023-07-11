package main

import (
	"regexp"
	"strconv"
)

const (
	DEFAULT_QUERY = "SELECT * FROM S3Object s"
)

var (
	albLogsWhereMap = map[string]string{
		"type":                     "_1",
		"time":                     "_2",
		"elb":                      "_3",
		"client:port":              "_4",
		"target:port":              "_5",
		"request_processing_time":  "_6",
		"target_processing_time":   "_7",
		"response_processing_time": "_8",
		"elb_status_code":          "_9",
		"target_status_code":       "_10",
		"received_bytes":           "_11",
		"sent_bytes":               "_12",
		"request":                  "_13",
		"user_agent":               "_14",
		"ssl_cipher":               "_15",
		"ssl_protocol":             "_16",
		"target_group_arn":         "_17",
		"trace_id":                 "_18",
		"domain_name":              "_19",
		"chosen_cert_arn":          "_20",
		"matched_rule_priority":    "_21",
		"request_creation_time":    "_22",
		"actions_executed":         "_23",
		"redirect_url":             "_24",
		"error_reason":             "_25",
		"target:port_list":         "_26",
		"target_status_code_list":  "_27",
		"classification":           "_28",
		"classification_reason":    "_29",
	}
	cfLogsWhereMap = map[string]string{
		"date":                        "_1",
		"time":                        "_2",
		"x-edge-location":             "_3",
		"sc-bytes":                    "_4",
		"c-ip":                        "_5",
		"cs-method":                   "_6",
		"cs(Host)":                    "_7",
		"cs-uri-stem":                 "_8",
		"sc-status":                   "_9",
		"cs(Referer)":                 "_10",
		"cs(User-Agent)":              "_11",
		"cs-uri-query":                "_12",
		"cs(Cookie)":                  "_13",
		"x-edge-result-type":          "_14",
		"x-edge-request-id":           "_15",
		"x-host-header":               "_16",
		"cs-protocol":                 "_17",
		"cs-bytes":                    "_18",
		"time-taken":                  "_19",
		"x-forwarded-for":             "_20",
		"ssl-protocol":                "_21",
		"ssl-cipher":                  "_22",
		"x-edge-response-result-type": "_23",
		"cs-protocol-version":         "_24",
		"fle-status":                  "_25",
		"fle-encrypted-fields":        "_26",
		"c-port":                      "_27",
		"time-to-first-byte":          "_28",
		"x-edge-detailed-result-type": "_29",
		"sc-content-type":             "_30",
		"sc-content-len":              "_31",
		"sc-range-start":              "_32",
		"sc-range-end":                "_33",
	}
)

func buildQuery(where string, limit int, isCount bool, isALBLogs bool, isCFLogs bool) string {
	if where == "" && limit == 0 && !isCount {
		return DEFAULT_QUERY
	}

	query := "SELECT"
	if isCount {
		query += " COUNT(*)"
	} else {
		query += " *"
	}
	query += " FROM S3Object s"
	if where != "" {
		query += " WHERE " + where
	}
	if limit != 0 {
		query += " LIMIT " + strconv.Itoa(limit)
	}

	if isALBLogs {
		for k, v := range albLogsWhereMap {
			rep := regexp.MustCompile(` (s\.)?` + "`?" + k + "`" + `? `)
			query = rep.ReplaceAllString(query, " s."+v+" ")
		}
	} else if isCFLogs {
		for k, v := range cfLogsWhereMap {
			rep := regexp.MustCompile(` (s\.)?` + "`?" + k + "`" + `? `)
			query = rep.ReplaceAllString(query, " s."+v+" ")
		}
	}

	return query
}
