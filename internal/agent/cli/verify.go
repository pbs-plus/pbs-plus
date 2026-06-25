package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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
	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	agentverification "github.com/pbs-plus/pbs-plus/internal/agent/verification"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func ExecVerification(verifyID string) (int, error) {
	log.Info("verify: exec begin")
	if err := validate.ValidateJobId(verifyID); err != nil {
		return -1, fmt.Errorf("invalid verifyID: %w", err)
	}

	tokenBytes, err := crypto.SecureRandomBytes(32)
	if err != nil {
		return -1, err
	}
	token := base64.StdEncoding.EncodeToString(tokenBytes)

	tokenFile := filepath.Join(os.TempDir(), fmt.Sprintf(".pbs-plus-token-verify-%s", verifyID))
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		return -1, err
	}

	defer func() {
		time.Sleep(5 * time.Second)
		if err := os.Remove(tokenFile); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
	}()

	execCmd, err := os.Executable()
	if err != nil {
		return -1, err
	}

	args := []string{
		"--cmdMode=verify",
		"--id=" + verifyID,
		"--token=" + token,
	}

	cmd := exec.Command(execCmd, args...)
	setProcAttributes(cmd)
	cmd.Env = os.Environ()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return -1, err
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return -1, err
	}

	if err := cmd.Start(); err != nil {
		return -1, err
	}
	log.Info("verify: child process started", "args", strings.Join(args, " "), "pid", cmd.Process.Pid)

	go func() {
		for scanner := bufio.NewScanner(stdoutPipe); scanner.Scan(); {
			log.Info(scanner.Text(), "forked", true)

		}
	}()

	go func() {
		for errScanner := bufio.NewScanner(stderrPipe); errScanner.Scan(); {
			log.Error(errors.New(errScanner.Text()), "", "forked", true)

		}
	}()
	log.Info("verify: returning to parent", "pid", cmd.Process.Pid)

	return cmd.Process.Pid, nil
}

func cmdVerify(verifyID *string) {
	if *verifyID == "" {
		fmt.Fprintln(os.Stderr, "Error: verifyID is required")
		os.Exit(1)
	}

	if err := validate.ValidateJobId(*verifyID); err != nil {
		os.Exit(1)
	}

	log.L = log.WithScope(log.Scope{VerifyID: *verifyID})

	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		log.Error(err, "verify: failed to get server URL")
		os.Exit(1)
	}
	uri, err := agent.ParseURI(serverUrl.Value)
	if err != nil {
		log.Error(err, "verify: failed to parse URI")
		os.Exit(1)
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		log.Error(err, "verify: failed to get TLS config")
		os.Exit(1)
	}

	address := fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), conf.ARPCServerPort)
	headers := http.Header{}
	headers.Add("X-PBS-Plus-VerifyID", *verifyID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	winquit.SimulateSigTermOnQuit(done)

	var wg sync.WaitGroup

	wg.Go(func() {
		defer log.Info("verify: arpc session handler shutting down")
		log.Info("verify: attempting connection")

		session, err := arpc.ConnectToServer(ctx, address, headers, tlsConfig)
		if err != nil {
			if strings.Contains(err.Error(), "(code 403)") {
				log.Error(err, "verify: authorization failed, shutting down")
			} else {
				log.Error(err, "verify: connection failed")
			}
			cancel()
			return
		}
		defer session.Close()

		router := arpc.NewRouter()
		router.Handle("verify_chunk_file", agentverification.VerifyChunkFileHandler)
		session.SetRouter(router)
		log.Info("verify: session ready, serving")
		if err := session.Serve(); err != nil {
			log.Warn("verify: ARPC session ended", "error", err.Error())
		}
	})

	go func() {
		sig := <-done
		log.Info(fmt.Sprintf("verify: received signal %v", sig))
		cancel()
	}()

	wg.Wait()
	log.Info("verify: finished")
	os.Exit(0)
}

// VerifyStartHandler is the ARPC handler that forks a verification worker
func VerifyStartHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData agentverification.VerifyStartReq
	if err := cbor.Unmarshal(req.Payload, &reqData); err != nil {
		return arpc.Response{}, err
	}

	pid, err := ExecVerification(reqData.VerifyID)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Message: fmt.Sprintf("%d", pid)}, nil
}
