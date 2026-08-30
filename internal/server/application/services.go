//go:build linux

package application

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	sessions "github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

type BackupService struct{ db *coredb.Store }

func NewBackupService(db *coredb.Store) *BackupService { return &BackupService{db: db} }

func (s *BackupService) ListBackups() ([]coredb.Backup, error) {
	backups, err := s.db.GetAllBackups()
	if err != nil {
		return nil, err
	}
	for i, b := range backups {
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
	db          *coredb.Store
	agentsMgr   *arpc.AgentsManager
	statusCache map[string]TargetStatusResult
	statusMu    sync.RWMutex
	refreshing  bool
	refreshMu   sync.Mutex
}

func NewTargetService(db *coredb.Store, agentsMgr *arpc.AgentsManager) *TargetService {
	return &TargetService{
		db:          db,
		agentsMgr:   agentsMgr,
		statusCache: make(map[string]TargetStatusResult),
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
func (s *TargetService) NewTransaction() (*coredb.Transaction, error) {
	return s.db.NewTransaction()
}

type TargetStatusResult struct {
	Index            int
	AgentVersion     string
	ConnectionStatus bool
	Error            error
}

func (s *TargetService) CheckStatus(ctx context.Context, targets []coredb.Target, checkStatus bool, timeout time.Duration) []TargetStatusResult {
	results := make([]TargetStatusResult, len(targets))
	sem := make(chan struct{}, 20)
	var wg sync.WaitGroup

	for i, target := range targets {
		wg.Add(1)
		go func(idx int, tgt coredb.Target) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					results[idx] = TargetStatusResult{
						Index:            idx,
						ConnectionStatus: false,
					}
				}
			}()

			sem <- struct{}{}
			defer func() { <-sem }()

			result := TargetStatusResult{Index: idx}
			if !tgt.IsAgent() {
				results[idx] = result
				return
			}
			arpcSess, ok := s.agentsMgr.GetQuicPipe(tgt.GetHostname())
			if !ok {
				arpcSessTcp, tcpOk := s.agentsMgr.GetStreamPipe(tgt.GetHostname())
				if !tcpOk {
					results[idx] = result
					return
				}
				result.AgentVersion = arpcSessTcp.GetVersion()
				if checkStatus {
					timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()
					respMsg, err := arpcSessTcp.CallMessage(timeoutCtx, "target_status",
						&fswire.TargetStatusReq{Drive: tgt.VolumeID})
					if err == nil && strings.HasPrefix(respMsg, "reachable") {
						result.ConnectionStatus = true
						if parts := strings.Split(respMsg, "|"); len(parts) > 1 {
							result.AgentVersion = parts[1]
						}
					} else if err != nil {
						result.Error = err
					}
				}
				results[idx] = result
				return
			}
			result.AgentVersion = arpcSess.GetVersion()
			if checkStatus {
				timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				respMsg, err := arpcSess.CallMessage(timeoutCtx, "target_status",
					&fswire.TargetStatusReq{Drive: tgt.VolumeID})
				if err == nil && strings.HasPrefix(respMsg, "reachable") {
					result.ConnectionStatus = true
					if parts := strings.Split(respMsg, "|"); len(parts) > 1 {
						result.AgentVersion = parts[1]
					}
				} else if err != nil {
					result.Error = err
				}
			}
			results[idx] = result
		}(i, target)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return results
	case <-done:
		return results
	}
}

func (s *TargetService) GetCachedStatuses() map[string]TargetStatusResult {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	out := make(map[string]TargetStatusResult, len(s.statusCache))
	maps.Copy(out, s.statusCache)
	return out
}

func (s *TargetService) RefreshStatuses() {
	s.refreshMu.Lock()
	if s.refreshing {
		s.refreshMu.Unlock()
		return
	}
	s.refreshing = true
	s.refreshMu.Unlock()

	go func() {
		defer func() {
			s.refreshMu.Lock()
			s.refreshing = false
			s.refreshMu.Unlock()
		}()

		targets, err := s.db.GetAllTargets()
		if err != nil {
			return
		}

		agentTargets := make([]coredb.Target, 0)
		for _, t := range targets {
			if t.IsAgent() {
				agentTargets = append(agentTargets, t)
			}
		}

		if len(agentTargets) == 0 {
			return
		}

		results := s.CheckStatus(context.Background(), agentTargets, true, 5*time.Second)

		s.statusMu.Lock()
		for _, r := range results {
			key := agentTargets[r.Index].Name
			s.statusCache[key] = r
		}
		s.statusMu.Unlock()
	}()
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
