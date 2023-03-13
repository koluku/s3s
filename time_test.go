package s3s

import (
	"strings"
	"testing"
	"time"
)

func TestRoundUpTime(t *testing.T) {
	cases := []struct {
		input time.Time
		want  time.Time
	}{
		{
			input: time.Date(2022, 9, 28, 12, 34, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 35, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 35, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 35, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 36, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 40, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 39, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 40, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 40, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 40, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 41, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 45, 0, 0, time.UTC),
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.input.String(), func(t *testing.T) {
			t.Parallel()
			got := roundUpTime(tt.input, time.Minute*5)
			if got != tt.want {
				t.Errorf("want = %+v, but got = %+v", tt.want, got)
			}
		})
	}
}

func BenchmarkRoundUpTime(b *testing.B) {
	t := time.Date(2022, 9, 27, 12, 35, 0, 0, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roundUpTime(t, time.Minute*5)
	}
}

func TestRoundDownTime(t *testing.T) {
	cases := []struct {
		input time.Time
		want  time.Time
	}{
		{
			input: time.Date(2022, 9, 28, 12, 34, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 30, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 35, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 35, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 36, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 35, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 39, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 35, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 40, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 40, 0, 0, time.UTC),
		},
		{
			input: time.Date(2022, 9, 28, 12, 41, 0, 0, time.UTC),
			want:  time.Date(2022, 9, 28, 12, 40, 0, 0, time.UTC),
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.input.String(), func(t *testing.T) {
			t.Parallel()
			got := roundDownTime(tt.input, time.Minute*5)
			if got != tt.want {
				t.Errorf("want = %+v, but got = %+v", tt.want, got)
			}
		})
	}
}

func BenchmarkRoundDownTime(b *testing.B) {
	t := time.Date(2022, 9, 27, 12, 35, 0, 0, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roundDownTime(t, time.Minute*5)
	}
}

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

func TestGetALBKeyEndTime(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		want    time.Time
		wantErr string
	}{
		{
			name:    "success",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_20220927T1234Z_192.168.1.1_123abc.log.gz",
			want:    time.Date(2022, 9, 27, 12, 34, 0, 0, time.UTC),
			wantErr: "",
		},
		{
			name:    "error",
			key:     "alb-logs/AWSLogs/aws-account-id/elasticloadbalancing/region/2022/09/27/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_2022.09.27T00.00Z_192.168.1.1_123abc.log.gz",
			want:    time.Time{},
			wantErr: ErrTimeParseFailed,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := getALBKeyEndTime(tt.key)
			if err != nil && !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err mismatch want = %+v, but got = %+v", tt.wantErr, err)
			}
			if got != tt.want {
				t.Errorf("want = %+v, but got = %+v", tt.want, got)
			}
		})
	}
}

func TestIsTimeWithin(t *testing.T) {
	baseTime := time.Date(2022, 9, 26, 12, 34, 0, 0, time.UTC)
	cases := []struct {
		name    string
		input   time.Time
		since   time.Time
		until   time.Time
		want    bool
		wantErr string
	}{
		{
			name:  "include",
			input: baseTime,
			since: baseTime.Add(-time.Hour),
			until: baseTime.Add(time.Hour),
			want:  true,
		},
		{
			name:  "include: in border",
			input: baseTime,
			since: baseTime.Add(-time.Hour),
			until: baseTime,
			want:  true,
		},
		{
			name:  "exclude",
			input: baseTime,
			since: baseTime.Add(time.Hour),
			until: baseTime.Add(time.Hour * 2),
			want:  false,
		},
		{
			name:  "exclude: in border",
			input: baseTime,
			since: baseTime.Add(time.Minute * 1),
			until: baseTime.Add(time.Minute * 2),
			want:  false,
		},
		{
			name:  "include: when since only",
			input: baseTime,
			since: baseTime.Add(-time.Minute * 4),
			until: time.Time{},
			want:  true,
		},
		{
			name:  "exclude: when since only",
			input: baseTime,
			since: baseTime.Add(time.Minute),
			until: time.Time{},
			want:  false,
		},
		{
			name:  "include: when until only",
			input: baseTime,
			since: time.Time{},
			until: baseTime.Add(time.Minute),
			want:  true,
		},
		{
			name:  "exclude: when until only",
			input: baseTime,
			since: time.Time{},
			until: baseTime.Add(-time.Minute * 5),
			want:  false,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isTimeWithin(tt.input, tt.since, tt.until)
			if got != tt.want {
				t.Errorf("want = %+v, but got = %+v", tt.want, got)
				if tt.since.IsZero() {
					t.Errorf("%v <= until: %v", tt.input, roundUpTime(tt.until, time.Minute*5))
				} else if tt.until.IsZero() {
					t.Errorf("since: %v <= %v", roundDownTime(tt.since, time.Minute*5), tt.input)
				} else {
					t.Errorf("since: %v <= %v <= until: %v", roundDownTime(tt.since, time.Minute*5), tt.input, roundUpTime(tt.until, time.Minute*5))
				}
			}
		})
	}
}
