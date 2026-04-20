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
	v := int64(0)
	if b {
		v = 1
	}
	return sql.NullInt64{Int64: v, Valid: true}
}

func fromNullInt64ToBool(ni sql.NullInt64) bool {
	return ni.Valid && ni.Int64 != 0
}

func toNullBool(b bool) sql.NullBool {
	return sql.NullBool{Bool: b, Valid: true}
}

func fromNullBool(ni sql.NullBool) bool {
	return ni.Valid && ni.Bool
}

func interfaceToString(i any) string {
	if i == nil {
		return ""
	}
	if s, ok := i.(string); ok {
		return s
	}
	return ""
}
