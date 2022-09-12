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

func checkFileFormat(fieldDelimiter string, recordDelimiter string, isCSV bool, isALBLogs bool, isCFLogs bool) error {
	var asCSV = fieldDelimiter != "" || recordDelimiter != ""
	var count int
	for _, format := range []bool{asCSV, isCSV, isALBLogs, isCFLogs} {
		if format {
			count++
		}
	}

	if count > 1 {
		if asCSV {
			return fmt.Errorf("too many option: field_delimiter and recordDelimiter can't use with isJSON, isCSV, isALBLogs or isCFLogs")
		} else {
			return fmt.Errorf("too many option: fromCSV, isALBLogs or isCFLogs")
		}
	}

	return nil
}
