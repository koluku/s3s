package main

import "strconv"

func buildQuery(where string, limit int, isCount bool) string {
	if where == "" && limit == 0 && !isCount {
		return DEFAULT_QUERY
	}

	query = "SELECT"
	if isCount {
		query += " COUNT(*)"
	} else {
		query += " *"
	}
	query += " FROM S3Object s"
	if where != "" {
		query += " WHERE " + where
	}
	if limit != 0 {
		query += " LIMIT " + strconv.Itoa(limit)
	}
	return query
}
