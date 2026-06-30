package tapeio

import "testing"

func TestEscapeUnitPath(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"/dev/nst0", "dev-nst0"},
		{"/dev/nst1", "dev-nst1"},
		{"/dev/sg0", "dev-sg0"},
		{"/", "-"},
		{"", "-"},
		{"/dev/tape/by-id/scsi-CZ24020L46-nst", "dev-tape-by\\x2did-scsi\\x2dCZ24020L46\\x2dnst"},
		{"/dev/.hidden", "dev-.hidden"},
		{"/.hidden", "\\x2ehidden"},
	}
	for _, c := range cases {
		got := escapeUnitPath(c.in)
		if got != c.want {
			t.Errorf("escapeUnitPath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestLockFilePath(t *testing.T) {
	got := lockFilePath("/dev/nst0")
	want := "/var/run/proxmox-backup/drive-lock/dev-nst0"
	if got != want {
		t.Errorf("lockFilePath(/dev/nst0) = %q, want %q", got, want)
	}
}
