package s3s

import (
	"strings"
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

func TestIsTimeWithinWhenALB(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		since   time.Time
		until   time.Time
		want    bool
		wantErr string
	}{
		{
			name:    "include",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since:   time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			until:   time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			want:    true,
			wantErr: "",
		},
		{
			name:    "include: in border",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since:   time.Date(2022, 9, 27, 0, 0, 0, 0, time.UTC),
			until:   time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			want:    true,
			wantErr: "",
		},
		{
			name:    "exclude",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since:   time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			until:   time.Date(2022, 9, 29, 0, 0, 0, 0, time.UTC),
			want:    false,
			wantErr: "",
		},
		{
			name:    "exclude: when since only",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since:   time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			until:   time.Time{},
			want:    false,
			wantErr: "",
		},
		{
			name:    "include: when since only",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since:   time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			until:   time.Time{},
			want:    true,
			wantErr: "",
		},
		{
			name:    "exclude: when until only",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since:   time.Time{},
			until:   time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			want:    false,
			wantErr: "",
		},
		{
			name:    "include: when until only",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T0000Z_192.168.1.1_123abc.log.gz",
			since:   time.Time{},
			until:   time.Date(2022, 9, 29, 0, 0, 0, 0, time.UTC),
			want:    true,
			wantErr: "",
		},
		{
			name:    "error",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_2022.09.27T00.00Z_192.168.1.1_123abc.log.gz",
			since:   time.Date(2022, 9, 26, 0, 0, 0, 0, time.UTC),
			until:   time.Date(2022, 9, 28, 0, 0, 0, 0, time.UTC),
			want:    false,
			wantErr: ErrTimeParseFailed,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := isTimeWithinWhenALB(tt.key, tt.since, tt.until)
			if err != nil && !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err mismatch want = %+v, but got = %+v", tt.wantErr, err)
			}
			if got != tt.want {
				t.Errorf("want = %+v, but got = %+v", tt.want, got)
			}
		})
	}
}
