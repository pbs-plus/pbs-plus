//go:build linux

package store

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/arpc"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	sqlite "github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"

	_ "modernc.org/sqlite"
)

type TLSConfig struct {
	ServerCertPath string
	ServerCertPEM  []byte
	ServerKeyPath  string
	ServerKeyPEM   []byte
	CACertPath     string
	CACertPEM      []byte
	CAKeyPath      string
	CAKeyPEM       []byte
	sync.Mutex
}

type Store struct {
	Ctx                context.Context
	Database           *sqlite.Database
	ARPCSessionManager *arpc.SessionManager
	arpcFS             *safemap.Map[string, *arpcfs.ARPCFS]
	mTLS               *TLSConfig
}

func (s *Store) SignAgentCSR(csr []byte) (cert []byte, ca []byte, err error) {
	s.mTLS.Lock()
	defer s.mTLS.Unlock()
	cert, err = mtls.SignCSR(
		s.mTLS.CACertPEM,
		s.mTLS.CAKeyPEM,
		csr,
		365,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	if err != nil {
		return
	}

	return cert, s.mTLS.CACertPEM, nil
}

func (s *Store) ValidateServerCertificates() error {
	serverCertPath, serverKeyPath, caCertPath, caKeyPath, err := mtls.EnsureLocalCAAndServerCert(
		filepath.Dir(constants.AgentTLSCACertFile),
		"PBS Plus",
		"PBS Plus CA",
		2048,
		365,
	)
	if err != nil {
		return err
	}

	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return fmt.Errorf("read ca cert: %w", err)
	}
	caKey, err := os.ReadFile(caKeyPath)
	if err != nil {
		return fmt.Errorf("read ca key: %w", err)
	}
	serverCert, err := os.ReadFile(serverCertPath)
	if err != nil {
		return fmt.Errorf("read server cert: %w", err)
	}
	serverKey, err := os.ReadFile(serverKeyPath)
	if err != nil {
		return fmt.Errorf("read server key: %w", err)
	}

	s.mTLS.Lock()
	defer s.mTLS.Unlock()

	s.mTLS.ServerCertPEM = serverCert
	s.mTLS.ServerCertPath = serverCertPath
	s.mTLS.ServerKeyPEM = serverKey
	s.mTLS.ServerKeyPath = serverKeyPath
	s.mTLS.CACertPEM = caCert
	s.mTLS.CACertPath = caCertPath
	s.mTLS.CAKeyPEM = caKey
	s.mTLS.CAKeyPath = caKeyPath

	return nil
}

func (s *Store) ListenAndServeAgentEndpoint(server *http.Server) error {
	s.mTLS.Lock()
	serverCert := s.mTLS.ServerCertPath
	serverKey := s.mTLS.ServerKeyPath
	s.mTLS.Unlock()

	return server.ListenAndServeTLS(serverCert, serverKey)
}

func (s *Store) GetServerTLSConfig() (*tls.Config, error) {
	s.mTLS.Lock()
	defer s.mTLS.Unlock()

	conf, err := mtls.BuildServerTLS(s.mTLS.ServerCertPath, s.mTLS.ServerKeyPath, s.mTLS.CACertPath, tls.VerifyClientCertIfGiven)
	return conf, err
}

func Initialize(ctx context.Context, paths map[string]string) (*Store, error) {
	sqlitePath := ""
	if paths != nil {
		sqlitePathTmp, ok := paths["sqlite"]
		if ok {
			sqlitePath = sqlitePathTmp
		}
	}

	db, err := sqlite.Initialize(ctx, sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	store := &Store{
		Ctx:                ctx,
		Database:           db,
		arpcFS:             safemap.New[string, *arpcfs.ARPCFS](),
		ARPCSessionManager: arpc.NewSessionManager(),
		mTLS:               &TLSConfig{},
	}

	return store, nil
}
