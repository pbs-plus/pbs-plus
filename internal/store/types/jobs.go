package types

type Job struct {
	ID               string      `json:"id"`
	Store            string      `config:"type=string,required" json:"store"`
	Target           string      `config:"type=string,required" json:"target"`
	Subpath          string      `config:"type=string" json:"subpath"`
	Schedule         string      `config:"type=string" json:"schedule"`
	Comment          string      `config:"type=string" json:"comment"`
	NotificationMode string      `config:"key=notification_mode,type=string" json:"notification-mode"`
	Namespace        string      `config:"type=string" json:"ns"`
	NextRun          *int64      `json:"next-run"`
	LastRunUpid      string      `config:"key=last_run_upid,type=string" json:"last-run-upid"`
	LastRunState     *string     `json:"last-run-state"`
	LastRunEndtime   *int64      `json:"last-run-endtime"`
	Duration         *int64      `json:"duration"`
	Exclusions       []Exclusion `json:"exclusions"`
	RawExclusions    string      `json:"rawexclusions"`
}
