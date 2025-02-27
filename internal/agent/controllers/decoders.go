//go:build windows

package controllers

import (
	"fmt"

	"github.com/valyala/fastjson"
)

func decodeBackupReq(v *fastjson.Value) (BackupReq, error) {
	var reqData BackupReq
	if v == nil || v.Type() != fastjson.TypeObject {
		return reqData, fmt.Errorf("payload is not a valid JSON object")
	}
	jobId := v.Get("jobId")
	if jobId == nil || jobId.GetStringBytes() == nil {
		return reqData, fmt.Errorf("missing 'jobId' field")
	}
	reqData.JobId = string(jobId.GetStringBytes())

	// Optional field "drive"
	drive := v.Get("drive")
	if drive != nil {
		reqData.Drive = string(drive.GetStringBytes())
	}
	return reqData, nil
}
