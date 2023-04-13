package s3s

import (
	"time"
)

const (
	ErrTimeParseFailed = "time parse failed"
)

func roundUpTime(t time.Time, d time.Duration) time.Time {
	if t.Truncate(d).Before(t) {
		return t.Truncate(d).Add(d)
	}
	return t
}

func isTimeZeroRange(since time.Time, until time.Time) bool {
	return since.IsZero() && until.IsZero()
}

func isTimeWithin(t time.Time, since time.Time, until time.Time) bool {
	if !since.IsZero() && t.Before(since) {
		return false
	}
	if !until.IsZero() && t.After(until) {
		return false
	}
	return true
}
