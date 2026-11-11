//go:build linux

package outpost

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
)

type advertisedListener struct {
	net.Listener
	addr net.Addr
}

func (l advertisedListener) Addr() net.Addr { return l.addr }

type fakePlacement struct{ calls []string }

func (p *fakePlacement) Listen(ctx context.Context, o Outpost) (net.Listener, error) {
	p.calls = append(p.calls, o.Agent)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return advertisedListener{Listener: ln, addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 2049}}, nil
}

func TestValidatePlacement(t *testing.T) {
	cases := []struct {
		name string
		o    Outpost
		want string
	}{
		{"server nfs", testOutpost(), ""},
		{"agent nfs", Outpost{Name: "a", Type: TypeNFS, Agent: "host-1.lan", ListenAddr: "0.0.0.0:2049"}, ""},
		{"agent s3", func() Outpost { o := testS3Outpost(); o.Agent = "host-1"; return o }(), ""},
		{"agent samba", Outpost{Name: "a", Type: TypeSamba, Agent: "host-1"}, "cannot be placed on an agent"},
		{"bad agent", Outpost{Name: "a", Type: TypeNFS, Agent: "../x", ListenAddr: "0.0.0.0:2049"}, "invalid agent name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateOutpost(tc.o)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestAgentPlacementUnregistered(t *testing.T) {
	SetAgentPlacement(nil)
	o := testOutpost()
	o.Agent = "host-1"
	if _, err := (nfsDriver{}).Start(t.Context(), o); !errors.Is(err, ErrAgentPlacementUnavailable) {
		t.Fatalf("got %v, want ErrAgentPlacementUnavailable", err)
	}
}

func TestAgentPlacementAdvertisesRemoteAddr(t *testing.T) {
	p := &fakePlacement{}
	SetAgentPlacement(p)
	t.Cleanup(func() { SetAgentPlacement(nil) })

	o := testOutpost()
	o.Agent = "host-1"
	inst, err := (nfsDriver{}).Start(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inst.Stop() })

	if len(p.calls) != 1 || p.calls[0] != "host-1" {
		t.Fatalf("placement calls = %v", p.calls)
	}
	if got, want := inst.Endpoint("share"), "nfs://10.0.0.7:2049/share"; got != want {
		t.Fatalf("endpoint = %q, want %q", got, want)
	}
}

func TestServerPlacementIgnoresAgentRegistration(t *testing.T) {
	p := &fakePlacement{}
	SetAgentPlacement(p)
	t.Cleanup(func() { SetAgentPlacement(nil) })

	inst, err := (nfsDriver{}).Start(t.Context(), testOutpost())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inst.Stop() })
	if len(p.calls) != 0 {
		t.Fatalf("server outpost used the agent placement: %v", p.calls)
	}
	if ep := inst.Endpoint("share"); !strings.HasPrefix(ep, "nfs://127.0.0.1:") {
		t.Fatalf("endpoint = %q", ep)
	}
}
