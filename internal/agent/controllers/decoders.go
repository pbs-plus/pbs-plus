//go:build windows

package controllers

import "github.com/goccy/go-json"

func decodeBackupReq(v json.RawMessage) (BackupReq, error) {
	var reqData BackupReq

	err := json.Unmarshal(v, &reqData)
	if err != nil {
		return BackupReq{}, err
	}

	return reqData, nil
}
