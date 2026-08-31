//go:build linux

package database

import "testing"

func TestClientVersionReadsReportedVersions(t *testing.T) {
	for _, testCase := range []struct {
		version string
		major   int
		minor   int
	}{
		{version: "pg_dump (PostgreSQL) 17.2", major: 17, minor: 2},
		{version: "pg_dump (PostgreSQL) 9.6.24", major: 9, minor: 6},
		{version: "mysqldump  Ver 8.0.35 for Linux on x86_64 (MySQL Community Server - GPL)", major: 8, minor: 0},
		{version: "mysqldump  Ver 10.13 Distrib 5.7.44, for Linux (x86_64)", major: 5, minor: 7},
		{version: "mariadb-dump  Ver 10.19 Distrib 11.4.4-MariaDB, for debian-linux-gnu (x86_64)", major: 11, minor: 4},
		{version: "mariadb-dump from 11.4.13-MariaDB, client 10.19 for debian-linux-gnu (x86_64)", major: 11, minor: 4},
		{version: "mariadb from 11.8.2-MariaDB, client 15.2 for Linux (x86_64) using readline 5.2", major: 11, minor: 8},
	} {
		major, minor, ok := clientVersion(ClientBundle{Version: testCase.version})
		if !ok || major != testCase.major || minor != testCase.minor {
			t.Errorf("clientVersion(%q) = %d.%d ok=%v", testCase.version, major, minor, ok)
		}
	}

	if _, _, ok := clientVersion(ClientBundle{Version: "unknown"}); ok {
		t.Error("unknown version was parsed")
	}
}

func TestParseServerVersionDetectsFamily(t *testing.T) {
	postgres, err := parseServerVersion(EnginePostgreSQL, "17.4 (Debian 17.4-1.pgdg120+1)\n")
	if err != nil {
		t.Fatal(err)
	}
	if postgres.Major != 17 || postgres.Family != FamilyPostgreSQL {
		t.Errorf("postgres server = %#v", postgres)
	}

	maria, err := parseServerVersion(EngineMySQL, "11.4.4-MariaDB-ubu2404\n")
	if err != nil {
		t.Fatal(err)
	}
	if maria.Major != 11 || maria.Minor != 4 || maria.Family != FamilyMariaDB {
		t.Errorf("mariadb server = %#v", maria)
	}

	mysql, err := parseServerVersion(EngineMySQL, "8.0.35\n")
	if err != nil {
		t.Fatal(err)
	}
	if mysql.Family != FamilyMySQL {
		t.Errorf("mysql server = %#v", mysql)
	}

	if _, err := parseServerVersion(EnginePostgreSQL, "ERROR: permission denied"); err == nil {
		t.Error("unparsable server version was accepted")
	}
}

func TestPickClientBundleMatchesServerMajor(t *testing.T) {
	candidates := []ClientBundle{
		{Family: FamilyPostgreSQL, Directory: "/usr/lib/postgresql/15/bin", Version: "pg_dump (PostgreSQL) 15.8"},
		{Family: FamilyPostgreSQL, Directory: "/usr/lib/postgresql/17/bin", Version: "pg_dump (PostgreSQL) 17.2"},
		{Family: FamilyPostgreSQL, Directory: "/usr/lib/postgresql/16/bin", Version: "pg_dump (PostgreSQL) 16.4"},
	}

	bundle, warning := pickClientBundle(candidates, ServerVersion{Major: 16, Minor: 4, Family: FamilyPostgreSQL})
	if bundle.Directory != "/usr/lib/postgresql/16/bin" || warning != "" {
		t.Fatalf("exact match = %q warning = %q", bundle.Directory, warning)
	}

	bundle, warning = pickClientBundle(candidates, ServerVersion{Major: 14, Family: FamilyPostgreSQL})
	if bundle.Directory != "/usr/lib/postgresql/15/bin" || warning == "" {
		t.Fatalf("older server = %q warning = %q", bundle.Directory, warning)
	}

	bundle, warning = pickClientBundle(candidates, ServerVersion{Major: 18, Family: FamilyPostgreSQL})
	if bundle.Directory != "/usr/lib/postgresql/17/bin" || warning == "" {
		t.Fatalf("newer server = %q warning = %q", bundle.Directory, warning)
	}
}

func TestPickClientBundlePrefersServerFamily(t *testing.T) {
	candidates := []ClientBundle{
		{Family: FamilyMySQL, Directory: "/usr/bin", Version: "mysqldump  Ver 8.0.35 for Linux on x86_64"},
		{Family: FamilyMariaDB, Directory: "/usr/bin", Version: "mariadb-dump  Ver 10.19 Distrib 11.4.4-MariaDB, for debian-linux-gnu"},
	}

	bundle, warning := pickClientBundle(candidates, ServerVersion{Major: 11, Minor: 4, Family: FamilyMariaDB})
	if bundle.Family != FamilyMariaDB || warning != "" {
		t.Fatalf("mariadb server picked %q warning = %q", bundle.Family, warning)
	}

	bundle, warning = pickClientBundle(candidates[1:], ServerVersion{Major: 8, Family: FamilyMySQL})
	if bundle.Family != FamilyMariaDB || warning == "" {
		t.Fatalf("family fallback = %q warning = %q", bundle.Family, warning)
	}
}
