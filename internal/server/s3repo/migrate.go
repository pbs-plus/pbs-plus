//go:build linux

package s3repo

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
)

type legacyOutpost struct {
	Name       string              `json:"name"`
	Type       string              `json:"type"`
	ListenAddr string              `json:"listen_addr"`
	S3         *objectstore.Config `json:"s3"`
	CreatedAt  int64               `json:"created_at"`
}

// MigrateLegacyOutposts moves outposts of the retired "s3" type into repositories; run it before outposts start.
func MigrateLegacyOutposts() error {
	legacyDir := filepath.Join(conf.StatePrefix, "outposts")
	entries, err := os.ReadDir(legacyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var errs []error
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		file := filepath.Join(legacyDir, e.Name())
		data, err := os.ReadFile(file)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var legacy legacyOutpost
		if err := json.Unmarshal(data, &legacy); err != nil || legacy.Type != "s3" {
			continue
		}
		if err := migrateOne(legacy); err != nil {
			errs = append(errs, fmt.Errorf("migrating s3 outpost %s: %w", legacy.Name, err))
			continue
		}
		if err := os.Remove(file); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func migrateOne(legacy legacyOutpost) error {
	if legacy.S3 == nil {
		return errors.New("missing s3 config")
	}
	if _, exists, err := Load(legacy.Name); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("repository %s already exists", legacy.Name)
	}
	return Save(Repository{
		Name:       legacy.Name,
		ListenAddr: legacy.ListenAddr,
		Config:     *legacy.S3,
		CreatedAt:  legacy.CreatedAt,
	})
}
