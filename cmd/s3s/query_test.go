package main

import "testing"

type buildQueryTestCase struct {
	name      string
	where     string
	limit     int
	isCount   bool
	isALBLogs bool
	isCFLogs  bool
	want      string
}

var (
	buildQueryTestCases = []*buildQueryTestCase{
		{
			name:      "default",
			where:     "",
			limit:     0,
			isCount:   false,
			isALBLogs: false,
			isCFLogs:  false,
			want:      "SELECT * FROM S3Object s",
		},
		{
			name:      "where",
			where:     "s.time > '2022-09-26 00:00:00'",
			limit:     0,
			isCount:   false,
			isALBLogs: false,
			isCFLogs:  false,
			want:      "SELECT * FROM S3Object s WHERE s.time > '2022-09-26 00:00:00'",
		},
		{
			name:      "limit",
			where:     "",
			limit:     1,
			isCount:   false,
			isALBLogs: false,
			isCFLogs:  false,
			want:      "SELECT * FROM S3Object s LIMIT 1",
		},
		{
			name:      "count",
			where:     "",
			limit:     0,
			isCount:   true,
			isALBLogs: false,
			isCFLogs:  false,
			want:      "SELECT COUNT(*) FROM S3Object s",
		},
		{
			name:      "where as alb-logs",
			where:     "s.time > '2022-09-26 00:00:00'",
			limit:     0,
			isCount:   false,
			isALBLogs: true,
			isCFLogs:  false,
			want:      "SELECT * FROM S3Object s WHERE s._2 > '2022-09-26 00:00:00'",
		},
		{
			name:      "where as alb-logs without s.",
			where:     "time > '2022-09-26 00:00:00'",
			limit:     0,
			isCount:   false,
			isALBLogs: true,
			isCFLogs:  false,
			want:      "SELECT * FROM S3Object s WHERE s._2 > '2022-09-26 00:00:00'",
		},
		{
			name:      "where as alb-logs using backquote",
			where:     "s.`time` > '2022-09-26 00:00:00'",
			limit:     0,
			isCount:   false,
			isALBLogs: true,
			isCFLogs:  false,
			want:      "SELECT * FROM S3Object s WHERE s._2 > '2022-09-26 00:00:00'",
		},
		{
			name:      "where as alb-logs using backquote without s.",
			where:     "`time` > '2022-09-26 00:00:00'",
			limit:     0,
			isCount:   false,
			isALBLogs: true,
			isCFLogs:  false,
			want:      "SELECT * FROM S3Object s WHERE s._2 > '2022-09-26 00:00:00'",
		},
		{
			name:      "where as cf-logs",
			where:     "s.date > '2022-09-26'",
			limit:     0,
			isCount:   false,
			isALBLogs: false,
			isCFLogs:  true,
			want:      "SELECT * FROM S3Object s WHERE s._1 > '2022-09-26'",
		},
		{
			name:      "where as cf-logs without s",
			where:     "date > '2022-09-26'",
			limit:     0,
			isCount:   false,
			isALBLogs: false,
			isCFLogs:  true,
			want:      "SELECT * FROM S3Object s WHERE s._1 > '2022-09-26'",
		},
		{
			name:      "where as cf-logs using backquote",
			where:     "s.`date` > '2022-09-26'",
			limit:     0,
			isCount:   false,
			isALBLogs: false,
			isCFLogs:  true,
			want:      "SELECT * FROM S3Object s WHERE s._1 > '2022-09-26'",
		},
		{
			name:      "where as cf-logs using backquote without s",
			where:     "`date` > '2022-09-26'",
			limit:     0,
			isCount:   false,
			isALBLogs: false,
			isCFLogs:  true,
			want:      "SELECT * FROM S3Object s WHERE s._1 > '2022-09-26'",
		},
	}
)

func TestBuildQuery(t *testing.T) {
	for _, tt := range buildQueryTestCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := buildQuery(tt.where, tt.limit, tt.isCount, tt.isALBLogs, tt.isCFLogs)
			if got != tt.want {
				t.Errorf("want = %s,\nbut got = %s", tt.want, got)
			}
		})
	}
}

func BenchmarkBuildQuery(b *testing.B) {
	for _, bb := range buildQueryTestCases {
		bb := bb
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buildQuery(bb.where, bb.limit, bb.isCount, bb.isALBLogs, bb.isCFLogs)
			}
		})
	}
}
