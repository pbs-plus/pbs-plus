//go:build linux

package s3repo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
)

type instance struct {
	listener     net.Listener
	server       *http.Server
	handler      *objectstore.Handler
	cancel       context.CancelFunc
	maintainDone chan struct{}
	tls          bool
}

// stateDir keeps the pre-split objectstore paths so migrated repositories retain their key index and uploads.
func stateDir() string { return filepath.Join(conf.StatePrefix, "objectstore") }

func start(ctx context.Context, r Repository) (*instance, error) {
	var tlsConfig *tls.Config
	if r.Config.TLSEnabled() {
		certFile, keyFile := r.Config.TLSCertFile, r.Config.TLSKeyFile
		if certFile == "" {
			certFile, keyFile = conf.CertFile, conf.KeyFile
		}
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("s3 repository tls: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	}
	handler, err := objectstore.NewHandler(r.Config, time.Unix(r.CreatedAt, 0))
	if err != nil {
		return nil, fmt.Errorf("s3 repository config: %w", err)
	}
	if err := handler.OpenMultipartSpool(filepath.Join(stateDir(), r.Name+"-uploads")); err != nil {
		log.Error(err, "s3 repository "+r.Name+" multipart spool")
	}
	if err := handler.OpenKeyIndex(filepath.Join(stateDir(), r.Name+".db")); err != nil {
		log.Error(err, "s3 repository "+r.Name+" key index; serving via snapshot scan")
	}
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", r.ListenAddr)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("s3 repository listen: %w", err), handler.Close())
	}
	server := &http.Server{
		Handler:           handler,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: conf.HTTPReadTimeout,
		WriteTimeout:      conf.HTTPWriteTimeout,
		IdleTimeout:       conf.HTTPIdleTimeout,
		MaxHeaderBytes:    conf.HTTPMaxHeaderBytes,
	}
	inst := &instance{listener: listener, server: server, handler: handler, tls: r.Config.TLSEnabled()}
	ctx, cancel := context.WithCancel(ctx)
	inst.cancel = cancel
	inst.maintainDone = make(chan struct{})
	go func() {
		defer close(inst.maintainDone)
		maintain := func() {
			if err := handler.ReconcileIndex(ctx); err != nil {
				log.Error(err, "s3 repository "+r.Name+" index reconcile")
			}
			if err := handler.ReapMultipartUploads(time.Now()); err != nil {
				log.Error(err, "s3 repository "+r.Name+" multipart reap")
			}
		}
		maintain()
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				maintain()
			}
		}
	}()
	go func() {
		var err error
		if inst.tls {
			err = server.ServeTLS(listener, "", "")
		} else {
			err = server.Serve(listener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error(err, "s3 repository "+r.Name)
		}
	}()
	return inst, nil
}

func (i *instance) endpoint(bucket string) string {
	scheme := "http"
	if i.tls {
		scheme = "https"
	}
	return scheme + "://" + i.listener.Addr().String() + "/" + bucket
}

func (i *instance) stop() error {
	i.cancel()
	<-i.maintainDone
	return errors.Join(i.server.Close(), i.handler.Close())
}

// Status reports a repository's runtime state for the API and UI.
type Status struct {
	Repository
	Running   bool     `json:"running"`
	Error     string   `json:"error,omitempty"`
	Endpoints []string `json:"endpoints,omitempty"`
}

type manager struct {
	mu        sync.RWMutex
	instances map[string]*instance
	errs      map[string]error
}

var mgr = &manager{instances: map[string]*instance{}, errs: map[string]error{}}

// StartAll starts every persisted repository; failures land in Status instead of aborting the rest.
func StartAll(ctx context.Context) {
	repos, err := List()
	if err != nil {
		log.Error(err, "listing s3 repositories")
		return
	}
	for _, r := range repos {
		if err := startOne(ctx, r); err != nil {
			log.Error(err, "starting s3 repository "+r.Name)
		}
	}
}

func startOne(ctx context.Context, r Repository) error {
	inst, err := start(ctx, r)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if err != nil {
		mgr.errs[r.Name] = err
		return err
	}
	mgr.instances[r.Name] = inst
	delete(mgr.errs, r.Name)
	return nil
}

func Apply(ctx context.Context, r Repository) error {
	if err := Save(r); err != nil {
		return err
	}
	Stop(r.Name)
	return startOne(ctx, r)
}

// Stop stops the running instance of name, if any, and clears its error.
func Stop(name string) {
	mgr.mu.Lock()
	inst := mgr.instances[name]
	delete(mgr.instances, name)
	delete(mgr.errs, name)
	mgr.mu.Unlock()
	if inst != nil {
		if err := inst.stop(); err != nil {
			log.Error(err, "stopping s3 repository "+name)
		}
	}
}

func StopAll() {
	mgr.mu.RLock()
	names := make([]string, 0, len(mgr.instances))
	for name := range mgr.instances {
		names = append(names, name)
	}
	mgr.mu.RUnlock()
	for _, name := range names {
		Stop(name)
	}
}

func StatusOf(r Repository) Status {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	st := Status{Repository: r}
	if err := mgr.errs[r.Name]; err != nil {
		st.Error = err.Error()
	}
	if inst := mgr.instances[r.Name]; inst != nil {
		st.Running = true
		for _, bucket := range r.Config.Buckets {
			st.Endpoints = append(st.Endpoints, inst.endpoint(bucket.Name))
		}
	}
	return st
}

func StatusAll() ([]Status, error) {
	repos, err := List()
	if err != nil {
		return nil, err
	}
	statuses := make([]Status, 0, len(repos))
	for _, r := range repos {
		statuses = append(statuses, StatusOf(r))
	}
	return statuses, nil
}
