package mtfdb

import (
	"database/sql"
)

func ns(s sql.NullString) string {
	if s.Valid {
		return s.String
	}
	return ""
}

func ni64(s sql.NullInt64) int64 {
	if s.Valid {
		return s.Int64
	}
	return 0
}

func ni(s sql.NullInt64) int {
	return int(ni64(s))
}

func nb(s sql.NullInt64) bool {
	return ni64(s) != 0
}
