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

func checkQuery(query string, where string, limit int, count bool) error {
	if query != "" {
		if where != "" {
			return fmt.Errorf("can't use where option with query option")
		}
		if limit != 0 {
			return fmt.Errorf("can't use where option with limit option")
		}
	}

	return nil
}
