//go:build linux

package store

import (
	"database/sql"
	"encoding/json"
	"time"
)

func nullString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}

func nullInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: true}
}

func fromNullString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func fromNullTime(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.Unix(value.Int64, 0)
}

func nullBytes(value sql.NullString) []byte {
	if !value.Valid {
		return nil
	}
	return []byte(value.String)
}

func jsonMarshal(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
