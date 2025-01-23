package types

type Exclusion struct {
	JobID   string `db:"job_id" json:"job_id"`
	Path    string `db:"path" json:"path"`
	Comment string `db:"comment" json:"comment"`
}

