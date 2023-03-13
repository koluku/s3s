package main

import (
	"time"

	"github.com/pkg/errors"
)

func checkArgs(paths []string) error {
	if isDelve {
		if len(paths) > 1 {
			return errors.Errorf("too many argument error")
		}
	} else {
		if len(paths) == 0 {
			return errors.Errorf("no argument error")
		}
	}

	return nil
}

func checkTime(duration time.Duration, until, since time.Time) error {
	if duration < 0 {
		return errors.Errorf("minus duration error")
	}
	if duration > 0 && (!until.IsZero() || !since.IsZero()) {
		return errors.Errorf("duration with since/until error")
	}
	if !until.IsZero() && !since.IsZero() && !since.Before(until) {
		return errors.Errorf("since >= until error")
	}

	return nil
}

func checkQuery(queryStr string, where string, limit int, isCount bool) error {
	if queryStr != "" {
		if where != "" {
			return errors.Errorf("can't use query option with query option")
		}
		if limit != 0 {
			return errors.Errorf("can't use query option with limit option")
		}
	}

	return nil
}

func checkFileFormat(isCSV bool, isALBLogs bool, isCFLogs bool) error {
	var count int
	for _, format := range []bool{isCSV, isALBLogs, isCFLogs} {
		if format {
			count++
		}
	}

	if count > 1 {
		return errors.Errorf("too many option: --csv, --alb-logs or --cf-logs")
	}

	return nil
}
