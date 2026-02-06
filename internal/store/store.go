//go:build linux

package store

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/arpc"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/store/config"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
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
	Ctx               context.Context
	Database          *sqlite.Database
	appConfig         *config.AppConfig
	configMu          sync.RWMutex
	ARPCAgentsManager *arpc.AgentsManager
	arpcFS            *safemap.Map[string, *arpcfs.ARPCFS]
	mTLS              *TLSConfig
}

func (s *Store) GetAppConfig() *config.AppConfig {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	return s.appConfig
}

func (s *Store) ReloadConfig() error {
	newConfig, err := config.Load(constants.AppConfigFile)
	if err != nil {
		return fmt.Errorf("reload config: %w", err)
	}

	s.configMu.Lock()
	s.appConfig = newConfig
	s.configMu.Unlock()

	log.Println("Configuration reloaded successfully via SIGHUP")
	return nil
}

func (s *Store) watchSignals() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)

	go func() {
		for {
			select {
			case <-sigChan:
				if err := s.ReloadConfig(); err != nil {
					log.Printf("Error reloading configuration: %v", err)
				}
			case <-s.Ctx.Done():
				return
			}
		}
	}()
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

func (s *Store) GetAPIServerTLSConfig() (*tls.Config, error) {
	s.mTLS.Lock()
	defer s.mTLS.Unlock()

	conf, err := mtls.BuildServerTLS(s.mTLS.ServerCertPath, s.mTLS.ServerKeyPath, s.mTLS.CACertPath, constants.AgentTLSPrevCACertFile, nil, tls.VerifyClientCertIfGiven, false)
	return conf, err
}

func (s *Store) GetARPCServerTLSConfig() (*tls.Config, error) {
	s.mTLS.Lock()
	defer s.mTLS.Unlock()

	conf, err := mtls.BuildServerTLS(s.mTLS.ServerCertPath, s.mTLS.ServerKeyPath, s.mTLS.CACertPath, constants.AgentTLSPrevCACertFile, []string{"pbsarpc"}, tls.VerifyClientCertIfGiven, true)
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

	go func() {
		_ = database.PurgeAllLegacyTimerUnits(ctx)
		jobs, err := db.GetAllBackups()
		if err == nil {
			database.SetBatchBackupSchedules(ctx, jobs)
		}
	}()

	conf, err := config.Load(constants.AppConfigFile)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing config.toml -> %w", err)
	}

	store := &Store{
		Ctx:               ctx,
		Database:          db,
		appConfig:         conf,
		arpcFS:            safemap.New[string, *arpcfs.ARPCFS](),
		ARPCAgentsManager: arpc.NewAgentsManager(),
		mTLS:              &TLSConfig{},
	}

	store.watchSignals()

	return store, nil
}
