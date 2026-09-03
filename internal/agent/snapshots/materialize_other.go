//go:build !linux

package snapshots

import "errors"

var errNoMountNamespace = errors.New("snapshot mounts are only supported on linux")

func Materialize(snap *Snapshot) error {
	return errNoMountNamespace
}

func Unmaterialize(snap *Snapshot) error {
	return nil
}

func PrivateMounts() error {
	return errNoMountNamespace
}
