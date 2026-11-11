//go:build linux

package s3repo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
)

// Repository is the persisted configuration of one S3 backup repository.
type Repository struct {
	Name       string             `json:"name"`
	ListenAddr string             `json:"listen_addr"`
	Config     objectstore.Config `json:"config"`
	CreatedAt  int64              `json:"created_at"`
}

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

func Validate(r Repository) error {
	if !nameRe.MatchString(r.Name) {
		return fmt.Errorf("invalid repository name %q: lowercase alphanumerics and dashes, max 32 chars", r.Name)
	}
	if r.ListenAddr == "" {
		return errors.New("listen_addr is required")
	}
	if _, port, err := net.SplitHostPort(r.ListenAddr); err != nil {
		return fmt.Errorf("invalid listen_addr: %w", err)
	} else if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("invalid listen_addr port %q", port)
	}
	return r.Config.Validate()
}

func dir() string { return filepath.Join(conf.StatePrefix, "s3-repositories") }

func path(name string) (string, error) {
	if !nameRe.MatchString(name) {
		return "", fmt.Errorf("invalid repository name %q", name)
	}
	return filepath.Join(dir(), name+".json"), nil
}

// Save validates and persists r, stamping CreatedAt on first save.
func Save(r Repository) error {
	if r.CreatedAt == 0 {
		r.CreatedAt = time.Now().Unix()
	}
	if err := Validate(r); err != nil {
		return err
	}
	if err := os.MkdirAll(dir(), 0o700); err != nil {
		return fmt.Errorf("create repositories dir: %w", err)
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	p, err := path(r.Name)
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o600)
}

func Load(name string) (Repository, bool, error) {
	p, err := path(name)
	if err != nil {
		return Repository{}, false, err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return Repository{}, false, nil
		}
		return Repository{}, false, err
	}
	var r Repository
	if err := json.Unmarshal(data, &r); err != nil {
		return Repository{}, false, fmt.Errorf("decoding repository %s: %w", name, err)
	}
	return r, true, nil
}

func Delete(name string) error {
	p, err := path(name)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

func List() ([]Repository, error) {
	entries, err := os.ReadDir(dir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	repos := make([]Repository, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		r, ok, err := Load(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			return nil, err
		}
		if ok {
			repos = append(repos, r)
		}
	}
	sort.Slice(repos, func(i, j int) bool { return repos[i].Name < repos[j].Name })
	return repos, nil
}
