package types

type Exclusion struct {
	Path    string `config:"type=string,required" json:"path"`
	Comment string `config:"type=string" json:"comment"`
	JobID   string `config:"key=job_id,type=string" json:"job_id"`
}
