//go:build linux

package snapshotmount

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// NamespaceGroup identifies one backup group (type/id) living in a namespace.
type NamespaceGroup struct {
	Namespace  string
	BackupType string
	BackupID   string
}

var backupTypeDirs = map[string]bool{"host": true, "vm": true, "ct": true}

// ListNamespaceGroups walks the datastore layout and returns every backup
// group at or under parentNS ("" = root), including nested namespaces.
func ListNamespaceGroups(storeRoot, parentNS string) ([]NamespaceGroup, error) {
	base := nsBaseDir(storeRoot, parentNS)
	if _, err := os.ReadDir(base); err != nil {
		return nil, fmt.Errorf("reading namespace %q: %w", parentNS, err)
	}
	var out []NamespaceGroup
	if err := walkGroups(base, parentNS, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func nsBaseDir(storeRoot, ns string) string {
	parts := []string{storeRoot}
	for part := range strings.SplitSeq(ns, "/") {
		if part != "" {
			parts = append(parts, "ns", part)
		}
	}
	return filepath.Join(parts...)
}

func joinNS(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "/" + child
}

func walkGroups(dir, ns string, out *[]NamespaceGroup) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		switch {
		case e.Name() == "ns":
			children, err := os.ReadDir(filepath.Join(dir, "ns"))
			if err != nil {
				return err
			}
			for _, c := range children {
				if !c.IsDir() || strings.HasPrefix(c.Name(), ".") {
					continue
				}
				if err := walkGroups(filepath.Join(dir, "ns", c.Name()), joinNS(ns, c.Name()), out); err != nil {
					return err
				}
			}
		case backupTypeDirs[e.Name()]:
			ids, err := os.ReadDir(filepath.Join(dir, e.Name()))
			if err != nil {
				return err
			}
			for _, id := range ids {
				if !id.IsDir() || strings.HasPrefix(id.Name(), ".") {
					continue
				}
				groupDir := filepath.Join(dir, e.Name(), id.Name())
				if !hasSnapshots(groupDir) {
					continue
				}
				*out = append(*out, NamespaceGroup{Namespace: ns, BackupType: e.Name(), BackupID: id.Name()})
			}
		}
	}
	return nil
}

func hasSnapshots(groupDir string) bool {
	entries, err := os.ReadDir(groupDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() {
			if _, err := time.Parse(dirTimeLayout, e.Name()); err == nil {
				return true
			}
		}
	}
	return false
}

// planBatch assigns each group its sub path inside the shared target: the
// namespace path relative to the parent, suffixed with type-id when the
// namespace carries several groups (or is the parent itself, where the bare
// root would be ambiguous).
func planBatch(groups []NamespaceGroup, parentNS string) map[string]string {
	counts := map[string]int{}
	for _, g := range groups {
		counts[g.Namespace]++
	}
	subs := make(map[string]string, len(groups))
	for _, g := range groups {
		sub := ""
		if g.Namespace != parentNS {
			sub = strings.TrimPrefix(strings.TrimPrefix(g.Namespace, parentNS), "/")
		}
		if sub == "" || counts[g.Namespace] > 1 {
			if sub != "" {
				sub += "/"
			}
			sub += g.BackupType + "-" + g.BackupID
		}
		subs[groupKeyOf(g.Namespace, g.BackupType, g.BackupID)] = sub
	}
	return subs
}
