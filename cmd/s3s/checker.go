package main

import (
	"context"
	"fmt"
	"strconv"
)

func checkArgs(ctx context.Context, paths []string) error {
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

func checkQuery(ctx context.Context, query string, where string, limit int) (string, error) {
	// Query Check
	if query == "" {
		query = DEFAULT_QUERY
		if where != "" {
			query = query + " WHERE " + where
		}
		if limit != 0 {
			query = query + " LIMIT " + strconv.Itoa(limit)
		}
	} else {
		if where != "" {
			return "", fmt.Errorf("can't use where option with query option")
		}
		if limit != 0 {
			return "", fmt.Errorf("can't use where option with limit option")
		}
	}

	return query, nil
}
