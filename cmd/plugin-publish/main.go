// Command plugin-publish turns built plugin binaries into a static
// repository tree (index.toml, index.toml.sig, releases/) that can be served
// from any HTTPS host, including a plain public git repository.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const usage = `usage:
  plugin-publish -key <ecdsa-pem> -id <repository-id> -publisher <name> -out <dir> [flags] <plugin-binary>...

flags:
  -key       path to PEM-encoded ECDSA P-256 private key (or env ECDSA_PRIVATE_KEY)
  -id        repository identifier, reverse-domain (e.g. com.example.plugins)
  -publisher publisher display name recorded in the index
  -out       output directory; created if missing, rewritten per run
  -channel   release channel (default "stable")
  -host-min  minimum host version (default "1.0.0")
  -host-max  maximum host version, optional
  -os        artifact GOOS (default "linux")
  -arch      artifact GOARCH (default: this machine's)

Each binary is launched once for plugin.describe; plugin id, version, target
types, protocol, and schemas come from the binary itself. The tree uses
relative URLs only, so it works unchanged at any hosting root (raw.githubusercontent
.com, GitHub Pages, any static server). Do not track artifacts with Git LFS:
raw hosts serve the LFS pointer, not the binary.
`

type config struct {
	keyPath   string
	repoID    string
	publisher string
	outDir    string
	channel   string
	hostMin   string
	hostMax   string
	goOS      string
	goArch    string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.keyPath, "key", "", "ECDSA P-256 private key PEM path")
	flag.StringVar(&cfg.repoID, "id", "", "repository identifier")
	flag.StringVar(&cfg.publisher, "publisher", "", "publisher name")
	flag.StringVar(&cfg.outDir, "out", "", "output directory")
	flag.StringVar(&cfg.channel, "channel", "stable", "release channel")
	flag.StringVar(&cfg.hostMin, "host-min", "1.0.0", "minimum host version")
	flag.StringVar(&cfg.hostMax, "host-max", "", "maximum host version")
	flag.StringVar(&cfg.goOS, "os", "linux", "artifact GOOS")
	flag.StringVar(&cfg.goArch, "arch", "", "artifact GOARCH")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if cfg.keyPath == "" {
		cfg.keyPath = os.Getenv("ECDSA_PRIVATE_KEY")
	}
	binaries := flag.Args()
	if cfg.keyPath == "" || cfg.repoID == "" || cfg.publisher == "" || cfg.outDir == "" || len(binaries) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if cfg.goArch == "" {
		cfg.goArch = os.Getenv("GOARCH")
		if cfg.goArch == "" {
			cfg.goArch = "amd64"
		}
	}

	if err := run(cfg, binaries); err != nil {
		fmt.Fprintln(os.Stderr, "plugin-publish:", err)
		os.Exit(1)
	}
}

func run(cfg config, binaries []string) error {
	key, err := loadECDSAKey(cfg.keyPath)
	if err != nil {
		return err
	}
	fingerprint, err := targetplugin.PublicKeyFingerprint(&key.PublicKey)
	if err != nil {
		return fmt.Errorf("fingerprint publisher key: %w", err)
	}

	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return err
	}

	index := targetplugin.RepositoryIndex{
		FormatVersion: 1,
		RepositoryID:  cfg.repoID,
	}
	for _, binary := range binaries {
		release, err := publishRelease(cfg, key, binary)
		if err != nil {
			return fmt.Errorf("%s: %w", binary, err)
		}
		index.Releases = append(index.Releases, release)
	}

	var indexBytes strings.Builder
	if err := toml.NewEncoder(&indexBytes).Encode(index); err != nil {
		return fmt.Errorf("encode index: %w", err)
	}
	if err := writeAndVerifyIndex(cfg, key, index, []byte(indexBytes.String())); err != nil {
		return err
	}

	fmt.Printf("repository written to %s\n", cfg.outDir)
	fmt.Printf("publisher key fingerprint: %s\n", fingerprint)
	fmt.Printf("confirm this fingerprint out of band before hosts trust the repository\n")
	return nil
}

func publishRelease(cfg config, key *ecdsa.PrivateKey, binary string) (targetplugin.RepositoryRelease, error) {
	descriptor, err := describeBinary(binary)
	if err != nil {
		return targetplugin.RepositoryRelease{}, fmt.Errorf("describe plugin: %w", err)
	}
	schemaDigest, err := targetplugin.SchemaDigest(descriptor)
	if err != nil {
		return targetplugin.RepositoryRelease{}, fmt.Errorf("schema digest: %w", err)
	}

	releaseDir := filepath.Join(cfg.outDir, "releases", descriptor.PluginID, descriptor.Version)
	if err := os.MkdirAll(releaseDir, 0o755); err != nil {
		return targetplugin.RepositoryRelease{}, err
	}

	manifest := targetplugin.PluginManifest{
		FormatVersion:   1,
		ProtocolVersion: descriptor.ProtocolVersion,
		PluginID:        descriptor.PluginID,
		Version:         descriptor.Version,
		TargetTypes:     descriptor.TargetTypes,
		SchemaSHA256:    schemaDigest,
		TargetSchema:    descriptor.TargetSchema,
		BackupSchema:    descriptor.BackupSchema,
		RestoreSchema:   descriptor.RestoreSchema,
	}
	var manifestBytes strings.Builder
	if err := toml.NewEncoder(&manifestBytes).Encode(manifest); err != nil {
		return targetplugin.RepositoryRelease{}, fmt.Errorf("encode manifest: %w", err)
	}
	manifestPath := filepath.Join(releaseDir, "manifest.toml")
	if err := os.WriteFile(manifestPath, []byte(manifestBytes.String()), 0o644); err != nil {
		return targetplugin.RepositoryRelease{}, err
	}
	manifestDigest := sha256.Sum256([]byte(manifestBytes.String()))

	artifactName := fmt.Sprintf("plugin-%s-%s", cfg.goOS, cfg.goArch)
	artifactPath := filepath.Join(releaseDir, artifactName)
	size, err := copyArtifact(artifactPath, binary)
	if err != nil {
		return targetplugin.RepositoryRelease{}, err
	}
	artifactBytes, err := os.ReadFile(artifactPath)
	if err != nil {
		return targetplugin.RepositoryRelease{}, err
	}
	digest := sha256.Sum256(artifactBytes)
	signature, err := signP256Digest(key, digest[:])
	if err != nil {
		return targetplugin.RepositoryRelease{}, fmt.Errorf("sign artifact: %w", err)
	}
	fingerprint, err := targetplugin.PublicKeyFingerprint(&key.PublicKey)
	if err != nil {
		return targetplugin.RepositoryRelease{}, err
	}

	release := targetplugin.RepositoryRelease{
		PluginID:                descriptor.PluginID,
		Version:                 descriptor.Version,
		Publisher:               cfg.publisher,
		PublisherKeyFingerprint: fingerprint,
		MinimumHostVersion:      cfg.hostMin,
		MaximumHostVersion:      cfg.hostMax,
		ProtocolVersion:         descriptor.ProtocolVersion,
		TargetTypes:             descriptor.TargetTypes,
		ManifestURL:             filepath.ToSlash(filepath.Join("releases", descriptor.PluginID, descriptor.Version, "manifest.toml")),
		ManifestSHA256:          hex.EncodeToString(manifestDigest[:]),
		Channel:                 cfg.channel,
		Artifacts: []targetplugin.RepositoryArtifact{{
			OS:        cfg.goOS,
			Arch:      cfg.goArch,
			URL:       filepath.ToSlash(filepath.Join("releases", descriptor.PluginID, descriptor.Version, artifactName)),
			Size:      uint64(size),
			SHA256:    hex.EncodeToString(digest[:]),
			Signature: signature,
		}},
	}
	return release, nil
}

func describeBinary(binary string) (targetplugin.Descriptor, error) {
	absolute, err := filepath.Abs(binary)
	if err != nil {
		return targetplugin.Descriptor{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	process, err := targetplugin.Start(ctx, absolute)
	if err != nil {
		return targetplugin.Descriptor{}, err
	}
	defer process.Close()
	return process.Describe(ctx)
}

func copyArtifact(dst, src string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		return 0, err
	}
	defer out.Close()
	return io.Copy(out, in)
}

func writeAndVerifyIndex(cfg config, key *ecdsa.PrivateKey, index targetplugin.RepositoryIndex, indexBytes []byte) error {
	digest := sha256.Sum256(indexBytes)
	signature, err := signP256Digest(key, digest[:])
	if err != nil {
		return fmt.Errorf("sign index: %w", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.outDir, "index.toml"), indexBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(cfg.outDir, "index.toml.sig"), []byte(signature), 0o644); err != nil {
		return err
	}

	parsed, err := targetplugin.ParseRepositoryIndex(indexBytes, []byte(signature), &key.PublicKey)
	if err != nil {
		return fmt.Errorf("self-check parse: %w", err)
	}
	if err := parsed.Validate(); err != nil {
		return fmt.Errorf("self-check validate: %w", err)
	}
	return nil
}

func signP256Digest(key *ecdsa.PrivateKey, digest []byte) (string, error) {
	r, s, err := ecdsa.Sign(rand.Reader, key, digest)
	if err != nil {
		return "", err
	}
	sig, err := asn1.Marshal(struct{ R, S *big.Int }{R: r, S: s})
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

func loadECDSAKey(path string) (*ecdsa.PrivateKey, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		pemBytes = []byte(path)
		if !strings.HasPrefix(string(pemBytes), "-----BEGIN") {
			return nil, fmt.Errorf("read key: %w", err)
		}
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block in signing key")
	}
	parsed, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse ECDSA key: %w", err)
	}
	if parsed.Curve != elliptic.P256() {
		return nil, errors.New("signing key must be ECDSA P-256")
	}
	return parsed, nil
}
