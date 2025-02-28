//go:generate msgp

package controllers

type BackupReq struct {
	JobId string `msg:"job_id"`
	Drive string `msg:"drive"`
}

