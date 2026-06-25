package crypto

import (
	"crypto/fips140"
	"crypto/tls"
	"fmt"
	"log/slog"
)

func FIPSServerTLSConfig() *tls.Config {
	cfg := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
	}
	if fips140.Enabled() {
		slog.Info("TLS server config: FIPS 140-3 mode active, using approved cipher suites")
	} else {
		slog.Warn("TLS server config: FIPS 140-3 mode NOT active — set GODEBUG=fips140=on to enable")
		cfg.CipherSuites = FIPSAllowedCipherSuites
		cfg.CurvePreferences = FIPSAllowedCurvePreferences
	}
	return cfg
}

func FIPSClientTLSConfig(minVersion uint16) *tls.Config {
	if minVersion < tls.VersionTLS12 {
		minVersion = tls.VersionTLS12
	}
	cfg := &tls.Config{
		MinVersion: minVersion,
	}
	if fips140.Enabled() {
		slog.Info("TLS client config: FIPS 140-3 mode active, using approved cipher suites")
	} else {
		slog.Warn("TLS client config: FIPS 140-3 mode NOT active — set GODEBUG=fips140=on to enable")
		cfg.CipherSuites = FIPSAllowedCipherSuites
		cfg.CurvePreferences = FIPSAllowedCurvePreferences
	}
	return cfg
}

func FIPSActive() bool {
	return fips140.Enabled()
}

func FIPSStrict() bool {
	return fips140.Enforced()
}

func AssertFIPS() error {
	if fips140.Enabled() {
		return nil
	}
	return fmt.Errorf("crypto: FIPS 140-3 mode is not active — set GODEBUG=fips140=on or GODEBUG=fips140=only before starting the process")
}

var FIPSAllowedCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
}

var FIPSAllowedCurvePreferences = []tls.CurveID{
	tls.X25519,
	tls.CurveP256,
	tls.CurveP384,
}
