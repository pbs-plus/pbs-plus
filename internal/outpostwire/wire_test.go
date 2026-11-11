package outpostwire

import "testing"

func TestValidateName(t *testing.T) {
	for _, name := range []string{"nfs", "e2e-agent-nfs", "a0"} {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v", name, err)
		}
	}
	for _, name := range []string{"", "-x", "Upper", "a/b", "../x", "a b", "a_b", "x\n"} {
		if err := ValidateName(name); err == nil {
			t.Errorf("ValidateName(%q) accepted", name)
		}
	}
}

func TestValidateListenAddr(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:2049", ":9100", "[::]:1", "10.0.0.5:65535"} {
		if err := ValidateListenAddr(addr); err != nil {
			t.Errorf("ValidateListenAddr(%q) = %v", addr, err)
		}
	}
	for _, addr := range []string{"", "2049", "host:0", "host:65536", "host:nfs", "--evil:1 x"} {
		if err := ValidateListenAddr(addr); err == nil {
			t.Errorf("ValidateListenAddr(%q) accepted", addr)
		}
	}
}
