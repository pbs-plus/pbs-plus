//go:build linux

package database

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

const (
	EnginePostgreSQL = "postgresql"
	EngineMySQL      = "mysql"

	FamilyPostgreSQL = "postgresql"
	FamilyMySQL      = "mysql"
	FamilyMariaDB    = "mariadb"
)

type ClientBundle struct {
	Engine            string `json:"engine"`
	Family            string `json:"family"`
	Directory         string `json:"directory"`
	Version           string `json:"version"`
	DumpProgram       string `json:"dump_program"`
	ServerDumpProgram string `json:"server_dump_program,omitempty"`
	RestoreProgram    string `json:"restore_program"`
}

func DiscoverClientBundles(ctx context.Context) ([]ClientBundle, error) {
	dirs, err := candidateDirectories()
	if err != nil {
		return nil, err
	}
	return discoverClientBundles(ctx, dirs, []string{"/usr", "/usr/local", "/opt"}), nil
}

func FindClientBundle(bundles []ClientBundle, engine, family, directory string) (ClientBundle, error) {
	directory = filepath.Clean(directory)
	for _, bundle := range bundles {
		if bundle.Engine == engine && bundle.Family == family && filepath.Clean(bundle.Directory) == directory {
			return bundle, nil
		}
	}
	return ClientBundle{}, errors.New("database client bundle is unavailable")
}

func ResolveClientBundle(ctx context.Context, target coredb.Target, family, directory string) (ClientBundle, error) {
	if family == "" {
		family = target.DatabaseClientFamily
		if target.Type == coredb.TargetTypePostgreSQL {
			family = FamilyPostgreSQL
		}
	}
	if directory == "" {
		directory = target.DatabaseDefaultClientDir
	}
	bundles, err := DiscoverClientBundles(ctx)
	if err != nil {
		return ClientBundle{}, err
	}
	return FindClientBundle(bundles, string(target.Type), family, directory)
}

func candidateDirectories() ([]string, error) {
	dirs := []string{"/usr/local/mysql/bin", "/usr/local/mariadb/bin"}
	for _, pattern := range []string{
		filepath.Join("/usr/lib/postgresql", "*", "bin"),
		"/usr/pgsql-*/bin",
		filepath.Join("/opt/mysql", "*", "bin"),
		filepath.Join("/opt/mariadb", "*", "bin"),
	} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		dirs = append(dirs, matches...)
	}
	dirs = append(dirs, "/usr/bin", "/usr/local/bin")
	return dirs, nil
}

func discoverClientBundles(ctx context.Context, dirs, trustedRoots []string) []ClientBundle {
	seen := make(map[string]struct{})
	bundles := make([]ClientBundle, 0)
	for _, dir := range dirs {
		canonicalDir, err := filepath.EvalSymlinks(dir)
		if err != nil || !trustedPath(canonicalDir, trustedRoots) {
			continue
		}

		if pgDump := trustedProgram(canonicalDir, "pg_dump", trustedRoots); pgDump != "" {
			if psql := trustedProgram(canonicalDir, "psql", trustedRoots); psql != "" {
				bundle := ClientBundle{
					Engine:            EnginePostgreSQL,
					Family:            FamilyPostgreSQL,
					Directory:         canonicalDir,
					Version:           programVersion(ctx, pgDump),
					DumpProgram:       pgDump,
					ServerDumpProgram: trustedProgram(canonicalDir, "pg_dumpall", trustedRoots),
					RestoreProgram:    psql,
				}
				bundles = appendUniqueBundle(bundles, seen, bundle)
			}
		}

		for _, programs := range []struct {
			family  string
			dump    string
			restore string
		}{
			{family: FamilyMariaDB, dump: "mariadb-dump", restore: "mariadb"},
			{family: FamilyMySQL, dump: "mysqldump", restore: "mysql"},
		} {
			dump := trustedProgram(canonicalDir, programs.dump, trustedRoots)
			restore := trustedProgram(canonicalDir, programs.restore, trustedRoots)
			if restore == "" && programs.family == FamilyMariaDB {
				restore = trustedProgram(canonicalDir, "mysql", trustedRoots)
			}
			if dump == "" || restore == "" {
				continue
			}
			version := programVersion(ctx, dump)
			family := programs.family
			if programs.dump == "mysqldump" && strings.Contains(strings.ToLower(version), "mariadb") {
				family = FamilyMariaDB
			}
			bundle := ClientBundle{
				Engine:            EngineMySQL,
				Family:            family,
				Directory:         canonicalDir,
				Version:           version,
				DumpProgram:       dump,
				ServerDumpProgram: dump,
				RestoreProgram:    restore,
			}
			bundles = appendUniqueBundle(bundles, seen, bundle)
		}
	}

	sort.Slice(bundles, func(i, j int) bool {
		if bundles[i].Engine != bundles[j].Engine {
			return bundles[i].Engine < bundles[j].Engine
		}
		if bundles[i].Family != bundles[j].Family {
			return bundles[i].Family < bundles[j].Family
		}
		return bundles[i].Directory < bundles[j].Directory
	})
	return bundles
}

func appendUniqueBundle(bundles []ClientBundle, seen map[string]struct{}, bundle ClientBundle) []ClientBundle {
	key := strings.Join([]string{bundle.Engine, bundle.Family, bundle.DumpProgram, bundle.RestoreProgram}, "|")
	if _, ok := seen[key]; ok {
		return bundles
	}
	seen[key] = struct{}{}
	return append(bundles, bundle)
}

func trustedProgram(dir, name string, trustedRoots []string) string {
	path, err := filepath.EvalSymlinks(filepath.Join(dir, name))
	if err != nil || !trustedPath(path, trustedRoots) {
		return ""
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return ""
	}
	return path
}

func trustedPath(path string, roots []string) bool {
	path = filepath.Clean(path)
	for _, root := range roots {
		rel, err := filepath.Rel(filepath.Clean(root), path)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func programVersion(ctx context.Context, program string) string {
	versionCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(versionCtx, program, "--version").Output()
	if err != nil {
		return "unknown"
	}
	version := strings.TrimSpace(string(out))
	if version == "" {
		return "unknown"
	}
	return version
}
