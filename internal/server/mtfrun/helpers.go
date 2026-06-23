package mtfrun

import "database/sql"

func nullStr(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func nullInt(n int64) sql.NullInt64 {
	return sql.NullInt64{Int64: n, Valid: true}
}

func mtfqueryBool(b bool) sql.NullInt64 {
	if b {
		return sql.NullInt64{Int64: 1, Valid: true}
	}
	return sql.NullInt64{Int64: 0, Valid: true}
}
