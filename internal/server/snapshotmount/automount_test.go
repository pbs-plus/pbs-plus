//go:build linux

package snapshotmount

import (
	"testing"
	"time"
)

func sess(backupTime, mode, mountPoint string) Session {
	parsed, _ := time.Parse(time.RFC3339, backupTime)
	return Session{
		Datastore: "ds1", Namespace: "", BackupType: "host", BackupID: "id1",
		BackupTime: backupTime, Mode: mode, MountPoint: mountPoint,
		ServiceKey: Key("ds1", "", "host", "id1", parsed.Format("2006-01-02_15-04-05")),
	}
}

func defaultPath(s Session) string {
	parsed, _ := time.Parse(time.RFC3339, s.BackupTime)
	return DefaultMountPoint(s.Datastore, s.Namespace, s.BackupType, s.BackupID, parsed)
}

func TestDecideRemount(t *testing.T) {
	oldSnap := "2026-01-01T00:00:00Z"
	newSnap := "2026-02-01T00:00:00Z"
	freshSnap := "2026-02-01T00:00:01Z"
	staleRO := sess(oldSnap, ModeRO, "")
	freshRO := sess(newSnap, ModeRO, "")
	rwSession := sess(newSnap, ModeRW, "")

	cases := []struct {
		name     string
		profile  Profile
		sessions []Session
		latest   string
		want     remountAction
	}{
		{"no session mounts", Profile{Mode: ModeRO}, nil, newSnap, remountMount},
		{"stale ro remounts", Profile{Mode: ModeRO}, []Session{staleRO}, newSnap, remountUnmount},
		{"fresh ro stays", Profile{Mode: ModeRO}, []Session{freshRO}, newSnap, remountNone},
		{"rw session skips", Profile{Mode: ModeRO}, []Session{rwSession}, freshSnap, remountSkipRW},
		{
			"newest of several default-path sessions decides",
			Profile{Mode: ModeRO},
			[]Session{staleRO, sess(oldSnap, ModeRO, "")},
			newSnap,
			remountUnmount,
		},
		{
			"custom profile path ignores other default mounts",
			Profile{Mode: ModeRO, MountPath: "/mnt/pinned"},
			[]Session{staleRO},
			newSnap,
			remountMount,
		},
		{
			"custom profile path stale remounts",
			Profile{Mode: ModeRO, MountPath: "/mnt/pinned"},
			[]Session{sess(oldSnap, ModeRO, "/mnt/pinned")},
			newSnap,
			remountUnmount,
		},
		{
			"custom profile path fresh stays",
			Profile{Mode: ModeRO, MountPath: "/mnt/pinned"},
			[]Session{sess(newSnap, ModeRO, "/mnt/pinned")},
			newSnap,
			remountNone,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sessions := make([]Session, len(c.sessions))
			copy(sessions, c.sessions)
			for i := range sessions {
				if sessions[i].MountPoint == "" {
					sessions[i].MountPoint = defaultPath(sessions[i])
				}
			}
			latest, err := time.Parse(time.RFC3339, c.latest)
			if err != nil {
				t.Fatal(err)
			}
			action, target := decideRemount(c.profile, sessions, latest)
			if action != c.want {
				t.Fatalf("action = %d, want %d", action, c.want)
			}
			if action == remountUnmount && target.MountPoint == "" {
				t.Fatal("unmount action without target session")
			}
		})
	}
}

func TestGroupKeyOfCollidesOnlyOnSameGroup(t *testing.T) {
	a := groupKeyOf("ds1", "ns", "host", "id1")
	if a != groupKeyOf("ds1", "ns", "host", "id1") {
		t.Fatal("same group produced different keys")
	}
	for _, other := range []string{
		groupKeyOf("ds2", "ns", "host", "id1"),
		groupKeyOf("ds1", "ns2", "host", "id1"),
		groupKeyOf("ds1", "ns", "vm", "id1"),
		groupKeyOf("ds1", "ns", "host", "id2"),
	} {
		if a == other {
			t.Fatal("different groups produced same key")
		}
	}
}
