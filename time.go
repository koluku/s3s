package s3s

import (
	"regexp"
	"time"

	"github.com/pkg/errors"
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

func roundDownTime(t time.Time, d time.Duration) time.Time {
	return t.Truncate(d)
}

func isTimeZeroRange(since time.Time, until time.Time) bool {
	return since.IsZero() && until.IsZero()
}

func getALBKeyEndTime(key string) (time.Time, error) {
	rep := regexp.MustCompile(`_\d{8}T\d{4}Z_`)
	timeStr := rep.FindString(key)

	t, err := time.Parse("_20060102T1504Z_", timeStr)
	if err != nil {
		return time.Time{}, errors.Wrap(err, ErrTimeParseFailed)
	}

	return t, nil
}

func isTimeWithin(t time.Time, since time.Time, until time.Time) bool {
	if !since.IsZero() && t.Before(roundDownTime(since, time.Minute*5)) {
		return false
	}
	if !until.IsZero() && t.After(roundUpTime(until, time.Minute*5)) {
		return false
	}
	return true
}
