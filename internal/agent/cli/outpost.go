package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/outpostwire"
)

const outpostReadyTimeout = time.Minute

func cmdOutpost(name, listenAddr *string) {
	if err := outpostwire.ValidateName(*name); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := outpostwire.ValidateListenAddr(*listenAddr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "outpost: listen:", err)
		os.Exit(outpostwire.ExitListenFailed)
	}
	defer func() { _ = ln.Close() }()

	serverURL, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		log.Error(err, "outpost: failed to get server URL")
		os.Exit(1)
	}
	uri, err := agent.ParseURI(serverURL.Value)
	if err != nil {
		log.Error(err, "outpost: failed to parse URI")
		os.Exit(1)
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		log.Error(err, "outpost: failed to get TLS config")
		os.Exit(1)
	}
	address := fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), conf.ARPCServerPort)
	headers := http.Header{}
	headers.Add(outpostwire.Header, *name)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	winquit.SimulateSigTermOnQuit(signals)
	go func() {
		select {
		case sig := <-signals:
			log.Info(fmt.Sprintf("outpost: received signal %v", sig))
			cancel()
		case <-ctx.Done():
		}
	}()

	session, err := arpc.ConnectToServer(ctx, address, headers, tlsConfig)
	if err != nil {
		log.Error(err, "outpost: connection to server failed")
		os.Exit(1)
	}
	defer session.Close()

	ready := make(chan struct{})
	var readyOnce sync.Once
	router := arpc.NewRouter()
	router.Handle(outpostwire.MethodReady, func(req *arpc.Request) (arpc.Response, error) {
		readyOnce.Do(func() { close(ready) })
		return arpc.Response{Status: 200, Message: "ready"}, nil
	})
	session.SetRouter(router)
	go func() {
		if err := session.Serve(); err != nil {
			log.Warn("outpost: session ended", "outpost", *name, "error", err.Error())
		}
		cancel()
	}()

	select {
	case <-ready:
	case <-ctx.Done():
		return
	case <-time.After(outpostReadyTimeout):
		log.Error(errors.New("server never routed the outpost tunnel"), "outpost: giving up", "outpost", *name)
		os.Exit(1)
	}
	log.Info("outpost: forwarding", "outpost", *name, "listen", *listenAddr)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Warn("outpost: accept failed", "error", err.Error())
			time.Sleep(100 * time.Millisecond)
			continue
		}
		wg.Go(func() { forwardOutpostConn(ctx, session, conn) })
	}
	wg.Wait()
	log.Info("outpost: finished", "outpost", *name)
}

func forwardOutpostConn(ctx context.Context, session *arpc.StreamPipe, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	err := session.Call(ctx, outpostwire.MethodConn, &outpostwire.ConnReq{Remote: conn.RemoteAddr().String()},
		arpc.RawStreamHandler(func(stream arpc.ARPCStream) error { return bridge(conn, stream) }))
	if err != nil && ctx.Err() == nil {
		log.Warn("outpost: forwarding connection failed", "remote", conn.RemoteAddr().String(), "error", err.Error())
	}
}

// bridge copies both directions until either side ends, then closes both so the other copy unblocks.
func bridge(a, b io.ReadWriteCloser) error {
	errs := make(chan error, 2)
	go func() {
		_, err := io.Copy(b, a)
		errs <- err
	}()
	go func() {
		_, err := io.Copy(a, b)
		errs <- err
	}()
	err := <-errs
	_ = a.Close()
	_ = b.Close()
	<-errs
	return err
}

func ExecOutpost(name, listenAddr string) (*exec.Cmd, error) {
	if err := outpostwire.ValidateName(name); err != nil {
		return nil, err
	}
	if err := outpostwire.ValidateListenAddr(listenAddr); err != nil {
		return nil, err
	}
	execCmd, err := os.Executable()
	if err != nil {
		return nil, err
	}
	tokenBytes, err := crypto.SecureRandomBytes(32)
	if err != nil {
		return nil, err
	}
	token := base64.StdEncoding.EncodeToString(tokenBytes)
	tokenFile := filepath.Join(os.TempDir(), fmt.Sprintf(".pbs-plus-token-outpost-%s", name))
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		return nil, err
	}

	cmd := exec.Command(execCmd, "--cmdMode=outpost", "--id="+name, "--listenAddr="+listenAddr, "--token="+token)
	setProcAttributes(cmd)
	cmd.Env = os.Environ()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		if rmErr := os.Remove(tokenFile); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Error(rmErr, "")
		}
		return nil, err
	}
	log.Info("outpost: child process started", "outpost", name, "pid", cmd.Process.Pid)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			log.Info(scanner.Text(), "forked", true, "outpost", name)
		}
	}()
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Error(errors.New(scanner.Text()), "", "forked", true, "outpost", name)
		}
	}()
	return cmd, nil
}
