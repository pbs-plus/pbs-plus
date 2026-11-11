//go:build linux

package outpost

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
)

type s3Driver struct{}

func (s3Driver) Type() string { return TypeS3 }

func (s3Driver) Validate(o Outpost) error {
	if o.ListenAddr == "" {
		return errors.New("listen_addr is required for s3 outposts")
	}
	if _, port, err := net.SplitHostPort(o.ListenAddr); err != nil {
		return fmt.Errorf("invalid listen_addr: %w", err)
	} else if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("invalid listen_addr port %q", port)
	}
	if o.S3 == nil {
		return errors.New("s3 config is required for s3 outposts")
	}
	return o.S3.Validate()
}

func (s3Driver) Start(ctx context.Context, o Outpost) (Instance, error) {
	var tlsConfig *tls.Config
	if o.S3.TLSEnabled() {
		certFile, keyFile := o.S3.TLSCertFile, o.S3.TLSKeyFile
		if certFile == "" {
			certFile, keyFile = conf.CertFile, conf.KeyFile
		}
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("s3 outpost tls: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	}
	createdAt := time.Unix(o.CreatedAt, 0)
	handler, err := objectstore.NewHandler(*o.S3, createdAt)
	if err != nil {
		return nil, fmt.Errorf("s3 outpost config: %w", err)
	}
	if err := handler.OpenMultipartSpool(filepath.Join(conf.StatePrefix, "objectstore", o.Name+"-uploads")); err != nil {
		log.Error(err, "s3 outpost "+o.Name+" multipart spool")
	}
	if err := handler.OpenKeyIndex(filepath.Join(conf.StatePrefix, "objectstore", o.Name+".db")); err != nil {
		log.Error(err, "s3 outpost "+o.Name+" key index; serving via snapshot scan")
	}
	listener, err := net.Listen("tcp", o.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("s3 outpost listen: %w", err)
	}
	server := &http.Server{
		Handler:           handler,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: conf.HTTPReadTimeout,
		WriteTimeout:      conf.HTTPWriteTimeout,
		IdleTimeout:       conf.HTTPIdleTimeout,
		MaxHeaderBytes:    conf.HTTPMaxHeaderBytes,
	}
	instance := &s3Instance{listener: listener, server: server, handler: handler, tls: o.S3.TLSEnabled()}
	ctx, cancel := context.WithCancel(ctx)
	instance.cancel = cancel
	instance.maintainDone = make(chan struct{})
	go func() {
		defer close(instance.maintainDone)
		maintain := func() {
			if err := handler.ReconcileIndex(ctx); err != nil {
				log.Error(err, "s3 outpost "+o.Name+" index reconcile")
			}
			if err := handler.ReapMultipartUploads(time.Now()); err != nil {
				log.Error(err, "s3 outpost "+o.Name+" multipart reap")
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
		if instance.tls {
			err = server.ServeTLS(listener, "", "")
		} else {
			err = server.Serve(listener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error(err, "s3 outpost "+o.Name)
		}
	}()
	return instance, nil
}

type s3Instance struct {
	listener     net.Listener
	server       *http.Server
	handler      *objectstore.Handler
	cancel       context.CancelFunc
	maintainDone chan struct{}
	tls          bool
}

func (s *s3Instance) Attach(a Attachment) error {
	return errors.New("s3 outposts do not accept mount attachments")
}

func (s *s3Instance) AttachSub(share, sub string, a Attachment) error {
	return errors.New("s3 outposts do not accept mount attachments")
}

func (s *s3Instance) DetachSub(share, sub string) {}

func (s *s3Instance) Detach(name string) error { return nil }

func (s *s3Instance) Attached() []string { return nil }

func (s *s3Instance) Endpoint(bucket string) string {
	scheme := "http"
	if s.tls {
		scheme = "https"
	}
	endpoint := scheme + "://" + s.listener.Addr().String()
	if bucket == "" {
		return endpoint + "/"
	}
	return endpoint + "/" + bucket
}

func (s *s3Instance) Stop() error {
	s.cancel()
	<-s.maintainDone
	return errors.Join(s.server.Close(), s.handler.Close())
}
