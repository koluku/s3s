package main

import (
	"fmt"
)

func checkArgs(paths []string) error {
	if isDelve {
		if len(paths) > 1 {
			return fmt.Errorf("too many argument error")
		}
	} else {
		if len(paths) == 0 {
			return fmt.Errorf("no argument error")
		}
	}

	return nil
}

func checkQuery(queryStr string, where string, limit int, isCount bool) error {
	if queryStr != "" {
		if where != "" {
			return fmt.Errorf("can't use query option with query option")
		}
		if limit != 0 {
			return fmt.Errorf("can't use query option with limit option")
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
		return fmt.Errorf("too many option: --csv, --alb-logs or --cf-logs")
	}

	return nil
}
