//go:build linux

package database

import (
	"database/sql"
	"fmt"
	"strconv"
)

func GetAgentTargetName(hostname string, volumeId string, os string) string {
	if os == "windows" {
		return hostname + " - " + volumeId
	}
	return hostname + " - Root"
}

func toNullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func fromNullString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func intToNullString(i int) sql.NullString {
	s := fmt.Sprintf("%d", i)
	return sql.NullString{String: s, Valid: s != ""}
}

func fromNullStringToInt(ns sql.NullString) int {
	if ns.Valid {
		num, err := strconv.Atoi(ns.String)
		if err == nil {
			return num
		}
	}
	return 0
}

func toNullInt64(i int) sql.NullInt64 {
	return sql.NullInt64{Int64: int64(i), Valid: true}
}

func fromNullInt64(ni sql.NullInt64) int {
	if ni.Valid {
		return int(ni.Int64)
	}
	return 0
}

func boolToNullInt64(b bool) sql.NullInt64 {
	i := boolToInt64(b)
	return sql.NullInt64{Int64: int64(i), Valid: true}
}

func fromNullInt64ToBool(ni sql.NullInt64) bool {
	if ni.Valid {
		return int64ToBool(ni.Int64)
	}
	return false
}

func toNullBool(b bool) sql.NullBool {
	return sql.NullBool{Bool: b, Valid: true}
}

func fromNullBool(ni sql.NullBool) bool {
	if ni.Valid {
		return ni.Bool
	}
	return false
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func int64ToBool(i int64) bool {
	return i != 0
}

func interfaceToString(i interface{}) string {
	if i == nil {
		return ""
	}
	if s, ok := i.(string); ok {
		return s
	}
	return ""
}
