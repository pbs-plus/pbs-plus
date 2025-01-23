package types

type PartialFile struct {
	Path    string `db:"path" json:"path"`
	Comment string `db:"comment" json:"comment"`
}
