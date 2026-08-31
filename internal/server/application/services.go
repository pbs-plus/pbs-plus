//go:build linux

package application

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
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
	VolumeTotalBytes int
	VolumeUsedBytes  int
	VolumeFreeBytes  int
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
	ConnectionStatus *bool  `json:"ConnectionStatus,omitempty"`
	AgentVersion     string `json:"AgentVersion"`
	TargetSizeResult
	LastError string    `json:"LastError"`
	CheckedAt time.Time `json:"CheckedAt"`
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
			result := TargetStatusResult{TargetSizeResult: size, Index: idx, AgentVersion: "N/A"}
			if !checkStatus {
				results[idx] = result
				return
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			switch {
			case tgt.IsAgent():
				var sess targetStatusSession
				if quic, ok := s.agentsMgr.GetQuicPipe(tgt.GetHostname()); ok {
					sess = quic
				} else if stream, ok := s.agentsMgr.GetStreamPipe(tgt.GetHostname()); ok {
					sess = stream
				} else {
					break
				}
				result.AgentVersion = sess.GetVersion()
				respMsg, err := sess.CallMessage(timeoutCtx, "target_status", &fswire.TargetStatusReq{Drive: tgt.VolumeID})
				result.Error = err
				if err == nil && strings.HasPrefix(respMsg, "reachable") {
					result.ConnectionStatus = true
					if parts := strings.Split(respMsg, "|"); len(parts) > 1 {
						result.AgentVersion = parts[1]
					}
				}
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

func (s *TargetService) storeStatusResults(targets []coredb.Target, results []TargetStatusResult) {
	now := time.Now()
	for _, result := range results {
		if result.Index < 0 || result.Index >= len(targets) {
			continue
		}
		entry := StatusEntry{
			ConnectionStatus: new(result.ConnectionStatus),
			AgentVersion:     result.AgentVersion,
			TargetSizeResult: result.TargetSizeResult,
			CheckedAt:        now,
		}
		if result.Error != nil {
			entry.LastError = result.Error.Error()
		}
		s.statusCache.Set(targets[result.Index].Name, entry)
	}
}

// GetStatusEntries serves cached statuses immediately; entries missing or
// older than statusRevalidateAfter get a coalesced background probe while
// the stale value is returned (stale-while-revalidate / ISR semantics).
func (s *TargetService) GetStatusEntries(targets []coredb.Target) map[string]StatusEntry {
	entries := make(map[string]StatusEntry, len(targets))
	stale := make([]string, 0, len(targets))
	for _, tgt := range targets {
		if entry, ok := s.statusCache.Get(tgt.Name); ok {
			entries[tgt.Name] = entry
			if time.Since(entry.CheckedAt) >= statusRevalidateAfter {
				stale = append(stale, tgt.Name)
			} else if tgt.IsAgent() && entry.ConnectionStatus != nil &&
				*entry.ConnectionStatus != s.agentsMgr.IsOnline(tgt.GetHostname()) {
				// cached verdict disagrees with the live session map: reprobe now
				stale = append(stale, tgt.Name)
			}
			continue
		}
		stale = append(stale, tgt.Name)
		entry := StatusEntry{AgentVersion: "N/A"}
		if tgt.IsAgent() {
			if s.agentsMgr.IsOnline(tgt.GetHostname()) {
				entry.ConnectionStatus = new(true)
				entry.AgentVersion = s.agentSessionVersion(tgt.GetHostname())
			}
		} else if tgt.IsLocal() {
			size, err := ResolveTargetSize(tgt)
			entry.TargetSizeResult = size
			entry.ConnectionStatus = new(err == nil)
			if err != nil {
				entry.LastError = err.Error()
			}
			entry.CheckedAt = time.Now()
		}
		entries[tgt.Name] = entry
	}
	for _, name := range stale {
		go s.revalidate(name)
	}
	s.evictOrphans(targets)
	return entries
}

// evictOrphans drops cache entries for targets no longer in the database.
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
	return "N/A"
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
