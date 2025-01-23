package types

type Job struct {
	ID               string      `db:"id" json:"id"`
	Store            string      `db:"store" json:"store"`
	Target           string      `db:"target" json:"target"`
	Subpath          string      `db:"subpath" json:"subpath"`
	Schedule         string      `db:"schedule" json:"schedule"`
	Comment          string      `db:"comment" json:"comment"`
	NotificationMode string      `db:"notification_mode" json:"notification-mode"`
	Namespace        string      `db:"namespace" json:"ns"`
	NextRun          *int64      `db:"next_run" json:"next-run"`
	LastRunUpid      *string     `db:"last_run_upid" json:"last-run-upid"`
	LastRunState     *string     `json:"last-run-state"`
	LastRunEndtime   *int64      `json:"last-run-endtime"`
	Duration         *int64      `json:"duration"`
	Exclusions       []Exclusion `json:"exclusions"`
	RawExclusions    string      `json:"rawexclusions"`
}
