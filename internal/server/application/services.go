//go:build linux

package application

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	sessions "github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"
)

type BackupService struct{ db *coredb.Store }

func NewBackupService(db *coredb.Store) *BackupService { return &BackupService{db: db} }

func (s *BackupService) ListBackups() ([]coredb.Backup, error) {
	backups, err := s.db.GetAllBackups()
	if err != nil {
		return nil, err
	}
	for i, b := range backups {
		if size, err := ResolveTargetSize(b.Target); err == nil {
			backups[i].Target.VolumeTotalBytes = size.VolumeTotalBytes
			backups[i].Target.VolumeUsedBytes = size.VolumeUsedBytes
			backups[i].Target.VolumeFreeBytes = size.VolumeFreeBytes
		}
		switch {
		case b.Target.IsAgent():
			if sess := sessions.GetSessionARPCFS(b.GetStreamID()); sess != nil {
				backups[i].CurrentStats = jobStatsFromVFS(sess.GetStats())
			}
		case b.Target.IsS3():
			if sess := sessions.GetSessionS3FS(b.GetStreamID()); sess != nil {
				backups[i].CurrentStats = jobStatsFromVFS(sess.GetStats())
			}
		}
	}
	return backups, nil
}

func (s *BackupService) GetBackup(id string) (coredb.Backup, error) { return s.db.GetBackup(id) }
func (s *BackupService) CreateBackup(b coredb.Backup) error         { return s.db.CreateBackup(nil, b) }
func (s *BackupService) UpdateBackup(b coredb.Backup) error         { return s.db.UpdateBackup(nil, b) }
func (s *BackupService) DeleteBackup(id string) error               { return s.db.DeleteBackup(nil, id) }

func jobStatsFromVFS(stats vfs.VFSStats) coredb.JobStats {
	return coredb.JobStats{
		CurrentFileCount:   int(stats.FilesAccessed),
		CurrentFolderCount: int(stats.FoldersAccessed),
		CurrentBytesTotal:  int(stats.TotalBytes),
		CurrentBytesSpeed:  int(stats.ByteReadSpeed),
		CurrentFilesSpeed:  int(stats.FileAccessSpeed),
		StatCacheHits:      int(stats.StatCacheHits),
	}
}

type RestoreService struct{ db *coredb.Store }

func NewRestoreService(db *coredb.Store) *RestoreService { return &RestoreService{db: db} }

func (s *RestoreService) GetAllRestores() ([]coredb.Restore, error)    { return s.db.GetAllRestores() }
func (s *RestoreService) GetRestore(id string) (coredb.Restore, error) { return s.db.GetRestore(id) }
func (s *RestoreService) CreateRestore(r coredb.Restore) error         { return s.db.CreateRestore(nil, r) }
func (s *RestoreService) UpdateRestore(r coredb.Restore) error         { return s.db.UpdateRestore(nil, r) }
func (s *RestoreService) DeleteRestore(id string) error                { return s.db.DeleteRestore(nil, id) }

type ExclusionService struct{ db *coredb.Store }

func NewExclusionService(db *coredb.Store) *ExclusionService { return &ExclusionService{db: db} }

func (s *ExclusionService) GetAllGlobalExclusions() ([]coredb.Exclusion, error) {
	return s.db.GetAllGlobalExclusions()
}
func (s *ExclusionService) GetExclusion(path string) (*coredb.Exclusion, error) {
	return s.db.GetExclusion(path)
}
func (s *ExclusionService) CreateExclusion(e coredb.Exclusion) error {
	return s.db.CreateExclusion(nil, e)
}
func (s *ExclusionService) UpdateExclusion(e coredb.Exclusion) error {
	return s.db.UpdateExclusion(nil, e)
}
func (s *ExclusionService) DeleteExclusion(path string) error { return s.db.DeleteExclusion(nil, path) }

type AgentHostService struct{ db *coredb.Store }

func NewAgentHostService(db *coredb.Store) *AgentHostService { return &AgentHostService{db: db} }

func (s *AgentHostService) GetAgentHost(hostname string) (coredb.AgentHost, error) {
	return s.db.GetAgentHost(hostname)
}
func (s *AgentHostService) CreateAgentHost(tx *coredb.Transaction, h coredb.AgentHost) error {
	return s.db.CreateAgentHost(tx, h)
}
func (s *AgentHostService) UpdateAgentHost(tx *coredb.Transaction, h coredb.AgentHost) error {
	return s.db.UpdateAgentHost(tx, h)
}
func (s *AgentHostService) DeleteAgentHost(hostname string) error {
	return s.db.DeleteAgentHost(nil, hostname)
}

type TokenService struct{ db *coredb.Store }

func NewTokenService(db *coredb.Store) *TokenService { return &TokenService{db: db} }

func (s *TokenService) GetAllTokens() ([]coredb.AgentToken, error)    { return s.db.GetAllTokens(false) }
func (s *TokenService) GetToken(id string) (coredb.AgentToken, error) { return s.db.GetToken(id) }
func (s *TokenService) CreateToken(d time.Duration, c string) error   { return s.db.CreateToken(d, c) }
func (s *TokenService) RevokeToken(t coredb.AgentToken) error         { return s.db.RevokeToken(t) }

type ScriptService struct{ db *coredb.Store }

func NewScriptService(db *coredb.Store) *ScriptService { return &ScriptService{db: db} }

func (s *ScriptService) GetAllScripts() ([]coredb.Script, error)      { return s.db.GetAllScripts() }
func (s *ScriptService) GetScript(path string) (coredb.Script, error) { return s.db.GetScript(path) }
func (s *ScriptService) CreateScript(sc coredb.Script) error          { return s.db.CreateScript(nil, sc) }
func (s *ScriptService) UpdateScript(sc coredb.Script) error          { return s.db.UpdateScript(nil, sc) }
func (s *ScriptService) DeleteScript(path string) error               { return s.db.DeleteScript(nil, path) }

type TargetService struct {
	db           *coredb.Store
	agentsMgr    *arpc.AgentsManager
	statusCache  *safemap.Map[string, StatusEntry]
	statusSem    chan struct{}
	statusFlight singleflight.Group
	agentFlight  singleflight.Group
}

func NewTargetService(db *coredb.Store, agentsMgr *arpc.AgentsManager) *TargetService {
	return &TargetService{
		db:          db,
		agentsMgr:   agentsMgr,
		statusCache: safemap.New[string, StatusEntry](),
		statusSem:   make(chan struct{}, statusProbeConcurrency),
	}
}

func (s *TargetService) GetAllTargets() ([]coredb.Target, error)      { return s.db.GetAllTargets() }
func (s *TargetService) GetTarget(name string) (coredb.Target, error) { return s.db.GetTarget(name) }
func (s *TargetService) CreateTarget(tx *coredb.Transaction, t coredb.Target) error {
	return s.db.CreateTarget(tx, t)
}
func (s *TargetService) UpdateTarget(tx *coredb.Transaction, t coredb.Target) error {
	return s.db.UpdateTarget(tx, t)
}
func (s *TargetService) DeleteTarget(tx *coredb.Transaction, name string) error {
	return s.db.DeleteTarget(tx, name)
}
func (s *TargetService) UpsertTarget(tx *coredb.Transaction, t coredb.Target) error {
	return s.db.UpsertTarget(tx, t)
}
func (s *TargetService) AddS3Secret(name, secret string) error {
	return s.db.AddS3Secret(nil, name, secret)
}
func (s *TargetService) AddDatabasePassword(name, password string) error {
	return s.db.AddDatabasePassword(nil, name, password)
}
func (s *TargetService) NewTransaction() (*coredb.Transaction, error) {
	return s.db.NewTransaction()
}

type TargetSizeResult struct {
	VolumeTotalBytes int `json:"volume_total_bytes"`
	VolumeUsedBytes  int `json:"volume_used_bytes"`
	VolumeFreeBytes  int `json:"volume_free_bytes"`
}

// ResolveTargetSize returns stored remote metadata or local filesystem capacity without scanning target contents.
func ResolveTargetSize(target coredb.Target) (TargetSizeResult, error) {
	result := TargetSizeResult{
		VolumeTotalBytes: target.VolumeTotalBytes,
		VolumeUsedBytes:  target.VolumeUsedBytes,
		VolumeFreeBytes:  target.VolumeFreeBytes,
	}
	if !target.IsLocal() {
		return result, nil
	}

	var stat unix.Statfs_t
	if err := unix.Statfs(target.Path, &stat); err != nil {
		return result, err
	}
	blockSize := uint64(stat.Bsize)
	result.VolumeTotalBytes = int(stat.Blocks * blockSize)
	result.VolumeUsedBytes = int((stat.Blocks - stat.Bfree) * blockSize)
	result.VolumeFreeBytes = int(stat.Bfree * blockSize)
	return result, nil
}

type TargetStatusResult struct {
	TargetSizeResult
	Index            int
	AgentVersion     string
	ConnectionStatus bool
	Error            error
}

// boolPtr gives StatusEntry a tri-state wire contract: nil means never
// probed (the UI shows "Checking..."), non-nil is the probe verdict.
//
//go:fix inline
func boolPtr(b bool) *bool { return new(b) }

// StatusEntry is a cached target status snapshot served to API clients.
type StatusEntry struct {
	ConnectionStatus *bool  `json:"connection_status,omitempty"`
	AgentVersion     string `json:"agent_version"`
	TargetSizeResult
	LastError string    `json:"last_error"`
	CheckedAt time.Time `json:"checked_at"`
}

func (s *TargetService) CheckStatus(ctx context.Context, targets []coredb.Target, checkStatus bool, timeout time.Duration, concurrency int) []TargetStatusResult {
	results := make([]TargetStatusResult, len(targets))
	if concurrency < 1 {
		concurrency = 20
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, target := range targets {
		wg.Add(1)
		go func(idx int, tgt coredb.Target) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					results[idx] = TargetStatusResult{Index: idx, Error: fmt.Errorf("status probe panic: %v", r)}
				}
			}()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = TargetStatusResult{Index: idx, Error: ctx.Err()}
				return
			}

			size, sizeErr := ResolveTargetSize(tgt)
			result := TargetStatusResult{TargetSizeResult: size, Index: idx}
			if !checkStatus {
				results[idx] = result
				return
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			switch {
			case tgt.IsAgent():
				sess, ok := s.agentSession(tgt.GetHostname())
				if !ok {
					break
				}
				result = probeAgentTarget(timeoutCtx, sess, tgt, idx)
				result.TargetSizeResult = size
			case tgt.IsLocal():
				result.Error = sizeErr
				result.ConnectionStatus = sizeErr == nil
			case tgt.IsS3():
				result.Error = probeS3Target(timeoutCtx, tgt)
				result.ConnectionStatus = result.Error == nil
			case tgt.IsDatabase():
				result.Error = probeTCP(timeoutCtx, net.JoinHostPort(tgt.DatabaseHost, strconv.Itoa(tgt.DatabasePort)))
				result.ConnectionStatus = result.Error == nil
			default:
				result.Error = fmt.Errorf("unsupported target type %q", tgt.Type)
			}
			results[idx] = result
		}(i, target)
	}

	wg.Wait()
	return results
}

const (
	// statusRevalidateAfter is the cache TTL: entries older than this are
	// served stale while a coalesced background probe refreshes them
	// (stale-while-revalidate / ISR semantics).
	statusRevalidateAfter  = 10 * time.Second
	statusProbeTimeout     = 5 * time.Second
	statusProbeConcurrency = 64
)

// revalidate probes one target and refreshes its cache entry. singleflight
// coalesces concurrent refreshes of the same target so overlapping requests
// share one probe. The timeout budget covers the semaphore wait as well as
// the probe: when the fleet is busy the revalidate defers to the next poll
// instead of queueing behind slow targets.
func (s *TargetService) revalidate(name string) {
	if s.db == nil {
		return
	}
	_, _, _ = s.statusFlight.Do(name, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), statusProbeTimeout)
		defer cancel()
		select {
		case s.statusSem <- struct{}{}:
			defer func() { <-s.statusSem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		tgt, err := s.GetTarget(name)
		if err != nil {
			return nil, err
		}
		targets := []coredb.Target{tgt}
		s.storeStatusResults(targets, s.CheckStatus(ctx, targets, true, statusProbeTimeout, 1))
		return nil, nil
	})
}

// revalidateAgent probes every stale target behind one agent in a single
// request-response; the agent checks drives concurrently on its side.
func (s *TargetService) revalidateAgent(hostname string, targets []coredb.Target) {
	if s.db == nil {
		return
	}
	_, _, _ = s.agentFlight.Do(hostname, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), statusProbeTimeout)
		defer cancel()
		select {
		case s.statusSem <- struct{}{}:
			defer func() { <-s.statusSem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		sess, ok := s.agentSession(hostname)
		if !ok {
			results := make([]TargetStatusResult, len(targets))
			for i := range targets {
				results[i] = TargetStatusResult{Index: i}
			}
			s.storeStatusResults(targets, results)
			return nil, nil
		}

		drives := make([]fswire.TargetStatusReq, len(targets))
		for i, tgt := range targets {
			drives[i] = fswire.TargetStatusReq{Drive: tgt.VolumeID}
		}
		var resp fswire.TargetStatusBatchResp
		err := sess.Call(ctx, "target_status_batch", &fswire.TargetStatusBatchReq{Drives: drives}, &resp)
		switch {
		case err == nil:
			s.storeStatusResults(targets, agentBatchResults(targets, resp, sess.GetVersion()))
		case isTimeoutErr(err):
			// wedged agent: no verdict for any drive, last known stands
			results := make([]TargetStatusResult, len(targets))
			for i := range targets {
				results[i] = TargetStatusResult{Index: i, AgentVersion: sess.GetVersion(), Error: err}
			}
			s.storeStatusResults(targets, results)
		default:
			// older agent without batch support: one request per drive
			results := make([]TargetStatusResult, len(targets))
			for i, tgt := range targets {
				results[i] = probeAgentTarget(ctx, sess, tgt, i)
			}
			s.storeStatusResults(targets, results)
		}
		return nil, nil
	})
}

// agentBatchResults maps one batch response onto per-target results; nil
// Reachable (or a missing drive) is a non-verdict, false is an explicit
// failure, true is reachable.
func agentBatchResults(targets []coredb.Target, resp fswire.TargetStatusBatchResp, version string) []TargetStatusResult {
	results := make([]TargetStatusResult, len(targets))
	for i, tgt := range targets {
		size, _ := ResolveTargetSize(tgt)
		r := TargetStatusResult{Index: i, TargetSizeResult: size, AgentVersion: version}
		st, ok := resp.Drives[tgt.VolumeID]
		switch {
		case !ok || st.Reachable == nil:
			r.Error = fmt.Errorf("drive check gave no verdict: %w", os.ErrDeadlineExceeded)
		case !*st.Reachable:
			r.Error = errors.New(st.Message)
		default:
			r.ConnectionStatus = true
		}
		results[i] = r
	}
	return results
}

// probeAgentTarget is the single-drive wire protocol (legacy fallback).
func probeAgentTarget(ctx context.Context, sess targetStatusSession, tgt coredb.Target, idx int) TargetStatusResult {
	result := TargetStatusResult{Index: idx, AgentVersion: sess.GetVersion()}
	respMsg, err := sess.CallMessage(ctx, "target_status", &fswire.TargetStatusReq{Drive: tgt.VolumeID})
	result.Error = err
	if err == nil && strings.HasPrefix(respMsg, "reachable") {
		result.ConnectionStatus = true
		if v, ok := strings.CutPrefix(respMsg, "reachable|"); ok {
			result.AgentVersion = v
		}
	}
	return result
}

func (s *TargetService) agentSession(hostname string) (targetStatusSession, bool) {
	if quic, ok := s.agentsMgr.GetQuicPipe(hostname); ok {
		return quic, true
	}
	if stream, ok := s.agentsMgr.GetStreamPipe(hostname); ok {
		return stream, true
	}
	return nil, false
}

func (s *TargetService) storeStatusResults(targets []coredb.Target, results []TargetStatusResult) {
	now := time.Now()
	for _, result := range results {
		if result.Index < 0 || result.Index >= len(targets) {
			continue
		}
		tgt := targets[result.Index]
		prior, hasPrior := s.statusCache.Get(tgt.Name)
		if isTimeoutErr(result.Error) && hasPrior {
			continue
		}
		entry := foldStatus(tgt, result, prior)
		entry.ConnectionStatus = new(result.ConnectionStatus)
		if isTimeoutErr(result.Error) {
			entry.ConnectionStatus = prior.ConnectionStatus
		}
		entry.CheckedAt = now
		s.statusCache.Set(tgt.Name, entry)
	}
}

func foldStatus(tgt coredb.Target, result TargetStatusResult, prior StatusEntry) StatusEntry {
	entry := StatusEntry{
		AgentVersion:     result.AgentVersion,
		TargetSizeResult: result.TargetSizeResult,
	}
	if entry.AgentVersion == "" {
		entry.AgentVersion = prior.AgentVersion
	}
	if entry.AgentVersion == "" {
		entry.AgentVersion = tgt.AgentVersion
	}
	if entry.VolumeTotalBytes == 0 && entry.VolumeUsedBytes == 0 && entry.VolumeFreeBytes == 0 {
		entry.TargetSizeResult = prior.TargetSizeResult
	}
	if entry.VolumeTotalBytes == 0 && entry.VolumeUsedBytes == 0 && entry.VolumeFreeBytes == 0 {
		entry.TargetSizeResult = TargetSizeResult{
			VolumeTotalBytes: tgt.VolumeTotalBytes,
			VolumeUsedBytes:  tgt.VolumeUsedBytes,
			VolumeFreeBytes:  tgt.VolumeFreeBytes,
		}
	}
	if result.Error != nil {
		entry.LastError = result.Error.Error()
	}
	return entry
}

// isTimeoutErr reports whether err is deadline-class: a timed-out probe
// carries no verdict, so the previous status must stand.
func isTimeoutErr(err error) bool {
	return err != nil &&
		(errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, os.ErrDeadlineExceeded) ||
			os.IsTimeout(err))
}

// GetStatusEntries serves cached statuses immediately; entries missing or
// older than statusRevalidateAfter get a coalesced background probe while
// the stale value is returned (stale-while-revalidate / ISR semantics).
func (s *TargetService) GetStatusEntries(targets []coredb.Target) map[string]StatusEntry {
	entries := make(map[string]StatusEntry, len(targets))
	staleOthers := make([]string, 0, len(targets))
	staleAgents := make(map[string][]coredb.Target)
	probe := func(tgt coredb.Target) {
		if tgt.IsAgent() {
			host := tgt.GetHostname()
			staleAgents[host] = append(staleAgents[host], tgt)
		} else {
			staleOthers = append(staleOthers, tgt.Name)
		}
	}
	for _, tgt := range targets {
		if entry, ok := s.statusCache.Get(tgt.Name); ok {
			entries[tgt.Name] = entry
			if time.Since(entry.CheckedAt) >= statusRevalidateAfter {
				probe(tgt)
			}
			continue
		}
		probe(tgt)
		result := TargetStatusResult{}
		var verdict *bool
		if tgt.IsAgent() {
			if s.agentsMgr.IsOnline(tgt.GetHostname()) {
				result.ConnectionStatus = true
				verdict = new(true)
				result.AgentVersion = s.agentSessionVersion(tgt.GetHostname())
			}
		} else if tgt.IsLocal() {
			size, err := ResolveTargetSize(tgt)
			result.TargetSizeResult = size
			result.ConnectionStatus = err == nil
			verdict = new(err == nil)
			result.Error = err
		}
		entry := foldStatus(tgt, result, StatusEntry{})
		entry.ConnectionStatus = verdict
		entry.CheckedAt = time.Now()
		entries[tgt.Name] = entry
	}
	for host, tgts := range staleAgents {
		go s.revalidateAgent(host, tgts)
	}
	for _, name := range staleOthers {
		go s.revalidate(name)
	}
	s.evictOrphans(targets)
	return entries
}

func (s *TargetService) OverlayStatus(targets []coredb.Target) {
	entries := s.GetStatusEntries(targets)
	for i := range targets {
		entry, ok := entries[targets[i].Name]
		if !ok {
			continue
		}
		if entry.ConnectionStatus != nil {
			targets[i].ConnectionStatus = *entry.ConnectionStatus
		}
		targets[i].AgentVersion = entry.AgentVersion
		targets[i].VolumeTotalBytes = entry.VolumeTotalBytes
		targets[i].VolumeUsedBytes = entry.VolumeUsedBytes
		targets[i].VolumeFreeBytes = entry.VolumeFreeBytes
	}
}

// No length shortcut: an under-populated cache can still hold orphans.
func (s *TargetService) evictOrphans(targets []coredb.Target) {
	live := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		live[t.Name] = struct{}{}
	}
	s.statusCache.ForEach(func(name string, _ StatusEntry) bool {
		if _, ok := live[name]; !ok {
			s.statusCache.Del(name)
		}
		return true
	})
}

func (s *TargetService) agentSessionVersion(hostname string) string {
	if q, ok := s.agentsMgr.GetQuicPipe(hostname); ok {
		return q.GetVersion()
	}
	if sp, ok := s.agentsMgr.GetStreamPipe(hostname); ok {
		return sp.GetVersion()
	}
	return ""
}

func probeS3Target(ctx context.Context, target coredb.Target) error {
	info := target.S3Info
	if info == nil {
		var err error
		info, err = coredb.ParseS3Url(target.Path)
		if err != nil {
			return err
		}
	}

	port := "80"
	if info.UseSSL {
		port = "443"
	}
	address := info.Endpoint
	serverName := strings.Trim(address, "[]")
	if host, _, err := net.SplitHostPort(address); err == nil {
		serverName = host
	} else {
		address = net.JoinHostPort(serverName, port)
	}

	if !info.UseSSL {
		return probeTCP(ctx, address)
	}
	dialer := tls.Dialer{
		NetDialer: &net.Dialer{},
		Config:    &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12},
	}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

func probeTCP(ctx context.Context, address string) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

type targetStatusSession interface {
	callSession
	Call(ctx context.Context, method string, payload any, out any) error
	GetVersion() string
}

type callSession interface {
	CallMessage(ctx context.Context, method string, payload any) (string, error)
}

type PushUpdateResult struct {
	Hostname string `json:"hostname"`
	Updated  bool   `json:"updated"`
	Message  string `json:"message"`
}

func (s *TargetService) PushUpdate(ctx context.Context, hostnames []string, timeout time.Duration) []PushUpdateResult {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	if len(hostnames) == 0 {
		targets, err := s.db.GetAllTargets()
		if err != nil {
			return nil
		}
		seen := make(map[string]struct{})
		for _, t := range targets {
			if !t.IsAgent() {
				continue
			}
			h := t.GetHostname()
			if _, ok := seen[h]; ok {
				continue
			}
			seen[h] = struct{}{}
			hostnames = append(hostnames, h)
		}
	}

	results := make([]PushUpdateResult, len(hostnames))
	sem := make(chan struct{}, 20)
	var wg sync.WaitGroup

	for i, host := range hostnames {
		wg.Add(1)
		go func(idx int, hostname string) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					results[idx] = PushUpdateResult{
						Hostname: hostname,
						Message:  fmt.Sprintf("panic: %v", r),
					}
				}
			}()

			sem <- struct{}{}
			defer func() { <-sem }()

			result := PushUpdateResult{Hostname: hostname}

			var sess callSession
			if qSess, ok := s.agentsMgr.GetQuicPipe(hostname); ok {
				sess = qSess
			} else if tSess, ok := s.agentsMgr.GetStreamPipe(hostname); ok {
				sess = tSess
			} else {
				result.Message = "agent not connected"
				results[idx] = result
				return
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			respMsg, err := sess.CallMessage(timeoutCtx, "update", nil)
			if err != nil {
				result.Message = err.Error()
			} else {
				result.Updated = true
				result.Message = respMsg
			}
			results[idx] = result
		}(i, host)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}

	return results
}

type VerificationService struct{ db *coredb.Store }

func NewVerificationService(db *coredb.Store) *VerificationService {
	return &VerificationService{db: db}
}

func (s *VerificationService) ListVerificationJobs() ([]coredb.VerificationJob, error) {
	return s.db.GetAllVerificationJobs()
}
func (s *VerificationService) GetVerificationJob(id string) (coredb.VerificationJob, error) {
	return s.db.GetVerificationJob(id)
}
func (s *VerificationService) CreateVerificationJob(j coredb.VerificationJob) error {
	return s.db.CreateVerificationJob(nil, j)
}
func (s *VerificationService) UpdateVerificationJob(j coredb.VerificationJob) error {
	return s.db.UpdateVerificationJob(nil, j)
}
func (s *VerificationService) DeleteVerificationJob(id string) error {
	return s.db.DeleteVerificationJob(nil, id)
}
func (s *VerificationService) GetVerificationResults(jobID string) ([]coredb.VerificationResult, error) {
	return s.db.GetVerificationResults(jobID)
}
func (s *VerificationService) GetLatestVerificationResult(jobID string) (coredb.VerificationResult, error) {
	return s.db.GetLatestVerificationResult(jobID)
}
func (s *VerificationService) CreateVerificationResult(r *coredb.VerificationResult) error {
	return s.db.CreateVerificationResult(r)
}
func (s *VerificationService) UpdateVerificationResult(r coredb.VerificationResult) error {
	return s.db.UpdateVerificationResult(r)
}

func (s *VerificationService) GetAllVerificationResults() ([]coredb.VerificationResult, error) {
	jobs, err := s.db.GetAllVerificationJobs()
	if err != nil {
		return nil, err
	}
	var all []coredb.VerificationResult
	for _, j := range jobs {
		results, err := s.db.GetVerificationResults(j.ID)
		if err != nil {
			continue
		}
		all = append(all, results...)
	}
	return all, nil
}
