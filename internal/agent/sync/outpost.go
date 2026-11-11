package sync

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/cli"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/outpostwire"
)

// outpostStartupGrace is how long a fresh child gets to fail its bind before the start counts as successful.
const outpostStartupGrace = 1500 * time.Millisecond

type outpostChild struct {
	cmd  *exec.Cmd
	done chan struct{}
}

var (
	outpostMu       sync.Mutex
	outpostChildren = map[string]*outpostChild{}
)

func stopOutpostChild(name string) {
	outpostMu.Lock()
	child := outpostChildren[name]
	delete(outpostChildren, name)
	outpostMu.Unlock()
	if child == nil {
		return
	}
	if err := child.cmd.Process.Kill(); err != nil {
		log.Warn("outpost: killing child", "outpost", name, "error", err.Error())
	}
	select {
	case <-child.done:
	case <-time.After(5 * time.Second):
		log.Warn("outpost: child did not exit", "outpost", name)
	}
}

// OutpostStartHandler always replaces a running child: the server only asks when it has no live tunnel.
func OutpostStartHandler(req *arpc.Request) (arpc.Response, error) {
	var in outpostwire.StartReq
	if err := cbor.Unmarshal(req.Payload, &in); err != nil {
		return arpc.Response{}, err
	}
	if err := outpostwire.ValidateName(in.Name); err != nil {
		return arpc.Response{}, err
	}
	if err := outpostwire.ValidateListenAddr(in.ListenAddr); err != nil {
		return arpc.Response{}, err
	}
	stopOutpostChild(in.Name)

	cmd, err := cli.ExecOutpost(in.Name, in.ListenAddr)
	if err != nil {
		return arpc.Response{}, err
	}
	child := &outpostChild{cmd: cmd, done: make(chan struct{})}
	go func() {
		if err := cmd.Wait(); err != nil {
			log.Warn("outpost: child exited", "outpost", in.Name, "error", err.Error())
		}
		close(child.done)
		outpostMu.Lock()
		if outpostChildren[in.Name] == child {
			delete(outpostChildren, in.Name)
		}
		outpostMu.Unlock()
	}()
	outpostMu.Lock()
	outpostChildren[in.Name] = child
	outpostMu.Unlock()

	select {
	case <-child.done:
		if cmd.ProcessState.ExitCode() == outpostwire.ExitListenFailed {
			return arpc.Response{}, fmt.Errorf("cannot listen on %s: address in use or not permitted", in.ListenAddr)
		}
		return arpc.Response{}, errors.New("outpost process exited during startup")
	case <-time.After(outpostStartupGrace):
	}
	return arpc.Response{Status: 200, Message: fmt.Sprintf("%d", cmd.Process.Pid)}, nil
}

func OutpostStopHandler(req *arpc.Request) (arpc.Response, error) {
	var in outpostwire.StopReq
	if err := cbor.Unmarshal(req.Payload, &in); err != nil {
		return arpc.Response{}, err
	}
	if err := outpostwire.ValidateName(in.Name); err != nil {
		return arpc.Response{}, err
	}
	stopOutpostChild(in.Name)
	return arpc.Response{Status: 200, Message: "stopped"}, nil
}
