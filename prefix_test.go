package s3s

import (
	"testing"
	"time"
)

func TestIsTimeZeroRange(t *testing.T) {

	cases := []struct {
		name  string
		since time.Time
		until time.Time
		want  bool
	}{
		{
			name:  "since and until are not zero time",
			since: time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			until: time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			want:  false,
		},
		{
			name:  "since is zero time",
			since: time.Time{},
			until: time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			want:  false,
		},
		{
			name:  "until is zero time",
			since: time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			until: time.Time{},
			want:  false,
		},
		{
			name:  "since and until are zero time",
			since: time.Time{},
			until: time.Time{},
			want:  true,
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isTimeZeroRange(tt.since, tt.until)
			if got != tt.want {
				t.Errorf("want = %+v, but got = %+v", tt.want, got)
			}
		})
	}
}

func TestIsTimeWithin(t *testing.T) {

	cases := []struct {
		name  string
		key   string
		since time.Time
		until time.Time
		want  bool
	}{
		{
			name:  "include",
			key:   "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since: time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			until: time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			want:  true,
		},
		{
			name:  "include: in border",
			key:   "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since: time.Date(2022, 9, 27, 0, 0, 0, 0, time.UTC),
			until: time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			want:  true,
		},
		{
			name:  "exclude",
			key:   "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since: time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			until: time.Date(2022, 9, 29, 0, 0, 0, 0, time.UTC),
			want:  false,
		},
		{
			name:  "exclude: when since only",
			key:   "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since: time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			until: time.Time{},
			want:  false,
		},
		{
			name:  "include: when since only",
			key:   "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since: time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			until: time.Time{},
			want:  true,
		},
		{
			name:  "exclude: when until only",
			key:   "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since: time.Time{},
			until: time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			want:  false,
		},
		{
			name:  "include: when until only",
			key:   "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since: time.Time{},
			until: time.Date(2022, 9, 29, 0, 0, 0, 0, time.UTC),
			want:  true,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isTimeWithin(tt.key, tt.since, tt.until)
			if got != tt.want {
				t.Errorf("want = %+v, but got = %+v", tt.want, got)
			}
		})
	}
}
