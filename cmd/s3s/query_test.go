package main

import "testing"

func TestBuildQuery(t *testing.T) {
	cases := []struct {
		name    string
		where   string
		limit   int
		isCount bool
		want    string
	}{
		{
			name:    "default",
			where:   "",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s",
		},
		{
			name:    "where",
			where:   "s.time > '2022-09-26 00:00:00'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s.time > '2022-09-26 00:00:00'",
		},
		{
			name:    "limit",
			where:   "",
			limit:   1,
			isCount: false,
			want:    "SELECT * FROM S3Object s LIMIT 1",
		},
		{
			name:    "count",
			where:   "",
			limit:   0,
			isCount: true,
			want:    "SELECT COUNT(*) FROM S3Object s",
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := BuildQuery(tt.where, tt.limit, tt.isCount)
			if got != tt.want {
				t.Errorf("want = %s,\nbut got = %s", tt.want, got)
			}
		})
	}
}

func TestBuildQueryForALB(t *testing.T) {
	cases := []struct {
		name    string
		where   string
		limit   int
		isCount bool
		want    string
	}{
		{
			name:    "default",
			where:   "",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s",
		},
		{
			name:    "where",
			where:   "s.time > '2022-09-26 00:00:00'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s.time > '2022-09-26 00:00:00'",
		},
		{
			name:    "limit",
			where:   "",
			limit:   1,
			isCount: false,
			want:    "SELECT * FROM S3Object s LIMIT 1",
		},
		{
			name:    "count",
			where:   "",
			limit:   0,
			isCount: true,
			want:    "SELECT COUNT(*) FROM S3Object s",
		},
		{
			name:    "where as alb-logs",
			where:   "s.time > '2022-09-26 00:00:00'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s._2 > '2022-09-26 00:00:00'",
		},
		{
			name:    "where as alb-logs without s.",
			where:   "time > '2022-09-26 00:00:00'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s._2 > '2022-09-26 00:00:00'",
		},
		{
			name:    "where as alb-logs using backquote",
			where:   "s.`time` > '2022-09-26 00:00:00'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s._2 > '2022-09-26 00:00:00'",
		},
		{
			name:    "where as alb-logs using backquote without s.",
			where:   "`time` > '2022-09-26 00:00:00'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s._2 > '2022-09-26 00:00:00'",
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := BuildQuery(tt.where, tt.limit, tt.isCount)
			if got != tt.want {
				t.Errorf("want = %s,\nbut got = %s", tt.want, got)
			}
		})
	}
}

func TestBuildQueryForCF(t *testing.T) {
	cases := []struct {
		name    string
		where   string
		limit   int
		isCount bool
		want    string
	}{
		{
			name:    "default",
			where:   "",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s",
		},
		{
			name:    "where",
			where:   "s.time > '2022-09-26 00:00:00'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s.time > '2022-09-26 00:00:00'",
		},
		{
			name:    "limit",
			where:   "",
			limit:   1,
			isCount: false,
			want:    "SELECT * FROM S3Object s LIMIT 1",
		},
		{
			name:    "count",
			where:   "",
			limit:   0,
			isCount: true,
			want:    "SELECT COUNT(*) FROM S3Object s",
		},
		{
			name:    "where as cf-logs",
			where:   "s.date > '2022-09-26'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s._1 > '2022-09-26'",
		},
		{
			name:    "where as cf-logs without s",
			where:   "date > '2022-09-26'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s._1 > '2022-09-26'",
		},
		{
			name:    "where as cf-logs using backquote",
			where:   "s.`date` > '2022-09-26'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s._1 > '2022-09-26'",
		},
		{
			name:    "where as cf-logs using backquote without s",
			where:   "`date` > '2022-09-26'",
			limit:   0,
			isCount: false,
			want:    "SELECT * FROM S3Object s WHERE s._1 > '2022-09-26'",
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := BuildQuery(tt.where, tt.limit, tt.isCount)
			if got != tt.want {
				t.Errorf("want = %s,\nbut got = %s", tt.want, got)
			}
		})
	}
}
