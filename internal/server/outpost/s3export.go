//go:build linux

package outpost

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/s3export"
)

type s3ExportDriver struct{}

func (s3ExportDriver) Type() string { return TypeS3Export }

func (s3ExportDriver) Validate(o Outpost) error {
	if o.ListenAddr == "" {
		return errors.New("listen_addr is required for s3-export outposts")
	}
	if _, port, err := net.SplitHostPort(o.ListenAddr); err != nil {
		return fmt.Errorf("invalid listen_addr: %w", err)
	} else if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("invalid listen_addr port %q", port)
	}
	if o.S3Export == nil {
		return errors.New("s3_export config is required for s3-export outposts")
	}
	return o.S3Export.Validate()
}

func (s3ExportDriver) Start(ctx context.Context, o Outpost) (Instance, error) {
	config := *o.S3Export
	var tlsConfig *tls.Config
	if config.TLSEnabled() {
		certFile, keyFile := config.TLSCertFile, config.TLSKeyFile
		if certFile == "" {
			certFile, keyFile = conf.CertFile, conf.KeyFile
		}
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("s3-export outpost tls: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	}
	ln, err := openListener(ctx, o)
	if err != nil {
		return nil, fmt.Errorf("s3-export outpost listen: %w", err)
	}
	inst := &s3ExportInstance{
		ln:      ln,
		tls:     config.TLSEnabled(),
		handler: s3export.NewHandler(config),
		subs:    map[string]*dirFS{},
	}
	inst.server = &http.Server{
		Handler:           inst.handler,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: conf.HTTPReadTimeout,
		WriteTimeout:      conf.HTTPWriteTimeout,
		IdleTimeout:       conf.HTTPIdleTimeout,
		MaxHeaderBytes:    conf.HTTPMaxHeaderBytes,
	}
	go func() {
		var err error
		if inst.tls {
			err = inst.server.ServeTLS(ln, "", "")
		} else {
			err = inst.server.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error(err, "s3-export outpost "+o.Name)
		}
	}()
	return inst, nil
}

type s3ExportInstance struct {
	ln      net.Listener
	server  *http.Server
	handler *s3export.Handler
	tls     bool

	mu   sync.Mutex
	subs map[string]*dirFS
}

func (i *s3ExportInstance) Attach(a Attachment) error {
	if !a.ReadOnly {
		return errors.New("s3-export outposts only serve read-only mounts")
	}
	if a.FS == nil {
		return errors.New("s3-export mounts need a filesystem")
	}
	return i.handler.Add(a.Name, a.FS)
}

func (i *s3ExportInstance) AttachSub(share, sub string, a Attachment) error {
	if !a.ReadOnly {
		return errors.New("s3-export outposts only serve read-only mounts")
	}
	if a.FS == nil {
		return errors.New("s3-export mounts need a filesystem")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	root, ok := i.subs[share]
	if !ok {
		root = newDirFS()
		if err := i.handler.Add(share, root); err != nil {
			return err
		}
		i.subs[share] = root
	}
	return root.add(sub, a.FS)
}

func (i *s3ExportInstance) DetachSub(share, sub string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if root, ok := i.subs[share]; ok && root.remove(sub) == 0 {
		delete(i.subs, share)
		i.handler.Remove(share)
	}
}

func (i *s3ExportInstance) Detach(name string) error {
	i.mu.Lock()
	delete(i.subs, name)
	i.mu.Unlock()
	i.handler.Remove(name)
	return nil
}

func (i *s3ExportInstance) Attached() []string { return i.handler.Shares() }

func (i *s3ExportInstance) Endpoint(share string) string {
	scheme := "http"
	if i.tls {
		scheme = "https"
	}
	return scheme + "://" + i.ln.Addr().String() + "/" + s3export.BucketName(share)
}

func (i *s3ExportInstance) Stop() error { return i.server.Close() }
