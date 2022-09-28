package main

import "github.com/pkg/errors"

func CheckArgs(paths []string, isDelve bool) error {
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

func CheckQuery(queryStr string, where string, limit int, isCount bool) error {
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
