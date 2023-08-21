package main

import (
	"testing"
	"time"
)

func ptrTime(timeTime time.Time) *time.Time {
	return &timeTime
}

type checkTimeTestCase struct {
	name  string
	input *State
	want  error
}

var checkTimeTestCases = []*checkTimeTestCase{
	{
		name:  "default",
		input: &State{},
		want:  nil,
	},
	{
		name: "NG: no time option when ALBlogs",
		input: &State{
			IsAlbLogs: true,
		},
		want: ErrNoTimeOption,
	},
	{
		name: "NG: no time option when CFLogs",
		input: &State{
			IsCfLogs: true,
		},
		want: ErrNoTimeOption,
	},
	{
		name: "OK: now - duration < now",
		input: &State{
			IsAlbLogs: true,
			Duration:  time.Hour,
		},
		want: nil,
	},
	{
		name: "OK: since < now < since + duration",
		input: &State{
			IsAlbLogs: true,
			Duration:  time.Hour,
			Since:     ptrTime(time.Now().UTC()),
		},
		want: nil,
	},
	{
		name: "NG: now < since < since + duration",
		input: &State{
			IsAlbLogs: true,
			Duration:  time.Hour,
			Since:     ptrTime(time.Now().UTC().Add(time.Hour)),
		},
		want: ErrOverTimeOption,
	},
	{
		name: "OK: until - duration < now < until",
		input: &State{
			IsAlbLogs: true,
			Duration:  time.Hour * 2,
			Until:     ptrTime(time.Now().UTC().Add(time.Hour)),
		},
		want: nil,
	},
	{
		name: "OK: until - duration < until < now",
		input: &State{
			IsAlbLogs: true,
			Duration:  time.Hour,
			Until:     ptrTime(time.Now().UTC().Add(-time.Hour)),
		},
		want: nil,
	},
	{
		name: "NG: now < until - duration < until",
		input: &State{
			IsAlbLogs: true,
			Duration:  time.Hour,
			Until:     ptrTime(time.Now().UTC().Add(time.Hour * 2)),
		},
		want: ErrOverTimeOption,
	},
	{
		name: "OK: since < until < now",
		input: &State{
			IsAlbLogs: true,
			Since:     ptrTime(time.Now().UTC().Add(-time.Hour * 2)),
			Until:     ptrTime(time.Now().UTC().Add(-time.Hour)),
		},
		want: nil,
	},
	{
		name: "OK: since < now < until",
		input: &State{
			IsAlbLogs: true,
			Since:     ptrTime(time.Now().UTC().Add(-time.Hour)),
			Until:     ptrTime(time.Now().UTC().Add(time.Hour)),
		},
		want: nil,
	},
	{
		name: "NG: now < since < until",
		input: &State{
			IsAlbLogs: true,
			Since:     ptrTime(time.Now().UTC().Add(time.Hour)),
			Until:     ptrTime(time.Now().UTC().Add(time.Hour * 2)),
		},
		want: ErrOverTimeOption,
	},
}

func TestCheckTime(t *testing.T) {
	for _, tt := range checkTimeTestCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.input.checkTime(); got != tt.want {
				t.Errorf("want = %s, but got = %s", tt.want, got)
			}
		})
	}
}

func BenchmarkCheckTime(b *testing.B) {
	for _, bb := range checkTimeTestCases {
		bb := bb
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				bb.input.checkTime()
			}
		})
	}
}
