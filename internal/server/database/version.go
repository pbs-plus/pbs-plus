//go:build linux

package database

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

type ServerVersion struct {
	Raw    string
	Major  int
	Minor  int
	Family string
}

func (v ServerVersion) String() string {
	if v.Raw != "" {
		return v.Raw
	}
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// SelectClientBundle picks the installed client whose version matches the live
// server. A non-empty client directory on the target pins the choice instead.
func SelectClientBundle(ctx context.Context, target coredb.Target, password string, logWriter io.Writer) (ClientBundle, error) {
	if !target.IsDatabase() {
		return ClientBundle{}, errors.New("target is not a database target")
	}
	bundles, err := DiscoverClientBundles(ctx)
	if err != nil {
		return ClientBundle{}, err
	}
	engine := string(target.Type)
	candidates := make([]ClientBundle, 0, len(bundles))
	for _, bundle := range bundles {
		if bundle.Engine == engine {
			candidates = append(candidates, bundle)
		}
	}
	if len(candidates) == 0 {
		return ClientBundle{}, fmt.Errorf("no %s client tools are installed", engine)
	}

	if target.DatabaseDefaultClientDir != "" {
		family := target.DatabaseClientFamily
		if engine == EnginePostgreSQL {
			family = FamilyPostgreSQL
		}
		return FindClientBundle(bundles, engine, family, target.DatabaseDefaultClientDir)
	}

	server, err := detectServerVersion(ctx, target, password, candidates)
	if err != nil {
		return ClientBundle{}, err
	}
	bundle, warning := pickClientBundle(candidates, server)
	logVersionLine(logWriter, fmt.Sprintf("using %s client %s from %s for server %s", bundle.Family, clientVersionText(bundle), bundle.Directory, server))
	if warning != "" {
		logVersionLine(logWriter, "warning: "+warning)
	}
	return bundle, nil
}

// detectServerVersion probes with the newest installed client because older clients refuse newer servers.
func detectServerVersion(ctx context.Context, target coredb.Target, password string, candidates []ClientBundle) (ServerVersion, error) {
	probe := candidates[0]
	for _, bundle := range candidates[1:] {
		if compareVersions(bundle, probe) > 0 {
			probe = bundle
		}
	}

	secretsDir, err := os.MkdirTemp("", ".pbs-plus-database-probe-")
	if err != nil {
		return ServerVersion{}, fmt.Errorf("create database probe directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(secretsDir) }()
	if err := os.Chmod(secretsDir, 0o700); err != nil {
		return ServerVersion{}, fmt.Errorf("secure database probe directory: %w", err)
	}

	var cmd *exec.Cmd
	if target.Type == coredb.TargetTypePostgreSQL {
		passfile, err := writePostgreSQLPassfile(secretsDir, target, password)
		if err != nil {
			return ServerVersion{}, err
		}
		args := append(postgreSQLBaseArgs(target), "--dbname=template1", "--tuples-only", "--no-align", "--command=SHOW server_version")
		cmd = exec.CommandContext(ctx, probe.RestoreProgram, args...)
		cmd.Env = append(os.Environ(), "PGPASSFILE="+passfile, "PGSSLMODE="+target.DatabaseTLSMode)
		if target.DatabaseCACertificate != "" {
			cmd.Env = append(cmd.Env, "PGSSLROOTCERT="+target.DatabaseCACertificate)
		}
	} else {
		defaultsFile, err := writeMySQLDefaultsFile(secretsDir, password)
		if err != nil {
			return ServerVersion{}, err
		}
		args := append(mySQLBaseArgs(target, defaultsFile), "--batch", "--skip-column-names", "--execute=SELECT VERSION()")
		args = append(args, mySQLTLSArgs(target.DatabaseTLSMode, target.DatabaseCACertificate, probe.Family)...)
		cmd = exec.CommandContext(ctx, probe.RestoreProgram, args...)
	}

	out, err := runClientCommand(cmd, password)
	if err != nil {
		return ServerVersion{}, fmt.Errorf("detect database server version: %w", err)
	}
	return parseServerVersion(string(target.Type), string(out))
}

func parseServerVersion(engine, out string) (ServerVersion, error) {
	raw := strings.TrimSpace(out)
	if index := strings.IndexAny(raw, "\r\n"); index >= 0 {
		raw = strings.TrimSpace(raw[:index])
	}
	major, minor, ok := versionNumbers(raw)
	if !ok {
		return ServerVersion{}, fmt.Errorf("unrecognized database server version %q", limitedText(raw, 128))
	}
	version := ServerVersion{Raw: raw, Major: major, Minor: minor, Family: FamilyPostgreSQL}
	if engine == EngineMySQL {
		version.Family = FamilyMySQL
		if strings.Contains(strings.ToLower(raw), "mariadb") {
			version.Family = FamilyMariaDB
		}
	}
	return version, nil
}

func pickClientBundle(candidates []ClientBundle, server ServerVersion) (ClientBundle, string) {
	best := candidates[0]
	bestRank := clientRank(best, server)
	for _, bundle := range candidates[1:] {
		if rank := clientRank(bundle, server); rank < bestRank {
			best, bestRank = bundle, rank
		}
	}

	major, _, known := clientVersion(best)
	switch {
	case best.Family != server.Family:
		return best, fmt.Sprintf("no %s client is installed, falling back to %s", server.Family, best.Family)
	case !known:
		return best, fmt.Sprintf("could not read the version of %s", best.DumpProgram)
	case major > server.Major:
		return best, fmt.Sprintf("client major %d is newer than server major %d, the dump may not restore into this server", major, server.Major)
	case major < server.Major:
		return best, fmt.Sprintf("client major %d is older than server major %d, the dump may be incomplete", major, server.Major)
	}
	return best, ""
}

// clientRank orders candidates: same family first, then exact major, then newer, then older.
func clientRank(bundle ClientBundle, server ServerVersion) int {
	rank := 0
	if bundle.Family != server.Family {
		rank += 1_000_000
	}
	major, minor, ok := clientVersion(bundle)
	if !ok {
		return rank + 900_000
	}
	switch {
	case majorKey(major, minor) == majorKey(server.Major, server.Minor):
		return rank
	case majorKey(major, minor) > majorKey(server.Major, server.Minor):
		return rank + 1_000 + majorKey(major, minor) - majorKey(server.Major, server.Minor)
	default:
		return rank + 100_000 + majorKey(server.Major, server.Minor) - majorKey(major, minor)
	}
}

// majorKey folds the PostgreSQL 9.x two-part major numbering into one comparable value.
func majorKey(major, minor int) int {
	if major >= 10 {
		return major * 100
	}
	return major*100 + minor
}

func compareVersions(a, b ClientBundle) int {
	aMajor, aMinor, aOK := clientVersion(a)
	bMajor, bMinor, bOK := clientVersion(b)
	switch {
	case aOK && !bOK:
		return 1
	case !aOK && bOK:
		return -1
	case !aOK && !bOK:
		return 0
	}
	return majorKey(aMajor, aMinor) - majorKey(bMajor, bMinor)
}

func clientVersion(bundle ClientBundle) (int, int, bool) {
	fields := strings.Fields(bundle.Version)
	for index, field := range fields {
		if !strings.EqualFold(field, "Distrib") && !strings.EqualFold(field, "from") {
			continue
		}
		if index+1 < len(fields) {
			if major, minor, ok := versionNumbers(fields[index+1]); ok {
				return major, minor, true
			}
		}
	}
	for index, field := range fields {
		if index > 0 && strings.EqualFold(fields[index-1], "client") {
			continue
		}
		if major, minor, ok := versionNumbers(field); ok {
			return major, minor, true
		}
	}
	return 0, 0, false
}

func clientVersionText(bundle ClientBundle) string {
	if major, minor, ok := clientVersion(bundle); ok {
		return fmt.Sprintf("%d.%d", major, minor)
	}
	return "unknown"
}

func versionNumbers(token string) (int, int, bool) {
	end := 0
	for end < len(token) && (token[end] == '.' || (token[end] >= '0' && token[end] <= '9')) {
		end++
	}
	parts := strings.Split(strings.Trim(token[:end], "."), ".")
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	minor := 0
	if len(parts) > 1 {
		minor, _ = strconv.Atoi(parts[1])
	}
	return major, minor, true
}

func logVersionLine(logWriter io.Writer, line string) {
	if logWriter == nil {
		return
	}
	_, _ = io.WriteString(logWriter, line+"\n")
}
