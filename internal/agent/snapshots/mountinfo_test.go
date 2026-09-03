package snapshots

import "testing"

func TestParseMountInfoLine(t *testing.T) {
	line := `36 35 253:1 / /var/lib/pg rw,noatime shared:1 - xfs /dev/mapper/vg0-pgdata rw,attr2`
	entry, ok := parseMountInfoLine(line)
	if !ok {
		t.Fatal("expected line to parse")
	}
	if entry.MountPoint != "/var/lib/pg" || entry.FSType != "xfs" || entry.Source != "/dev/mapper/vg0-pgdata" {
		t.Fatalf("unexpected entry: %+v", entry)
	}

	escaped := `40 35 0:44 / /mnt/my\040disk rw - ext4 /dev/sdb1 rw`
	entry, ok = parseMountInfoLine(escaped)
	if !ok || entry.MountPoint != "/mnt/my disk" {
		t.Fatalf("octal escape not decoded: %+v", entry)
	}

	if _, ok := parseMountInfoLine("garbage line without separator"); ok {
		t.Fatal("expected malformed line to be rejected")
	}
}

func TestPickMountLongestPrefix(t *testing.T) {
	entries := []MountEntry{
		{MountPoint: "/", FSType: "ext4", Source: "/dev/sda1"},
		{MountPoint: "/var", FSType: "ext4", Source: "/dev/sda2"},
		{MountPoint: "/var/lib/pg", FSType: "xfs", Source: "/dev/mapper/vg0-pgdata"},
		{MountPoint: "/var/lib/pgother", FSType: "ext4", Source: "/dev/sdc1"},
	}

	cases := map[string]string{
		"/var/lib/pg/base/1": "/var/lib/pg",
		"/var/lib/pg":        "/var/lib/pg",
		"/var/lib/pgs":       "/var",
		"/var/log":           "/var",
		"/etc":               "/",
	}

	for path, want := range cases {
		got, err := pickMount(entries, path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if got.MountPoint != want {
			t.Fatalf("%s: got %s, want %s", path, got.MountPoint, want)
		}
	}
}

func TestPickMountPrefersShadowingMount(t *testing.T) {
	entries := []MountEntry{
		{MountPoint: "/data", FSType: "ext4", Source: "/dev/sda3"},
		{MountPoint: "/data", FSType: "xfs", Source: "/dev/sdb3"},
	}
	got, err := pickMount(entries, "/data/files")
	if err != nil {
		t.Fatal(err)
	}
	if got.Source != "/dev/sdb3" {
		t.Fatalf("expected the shadowing mount, got %s", got.Source)
	}
}
