//go:build linux

package application

import (
	"context"
	"strings"
	"sync"
	"time"

	reqTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	sessions "github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

// --- BackupService ---

type BackupService struct{ db *database.Database }

func NewBackupService(db *database.Database) *BackupService { return &BackupService{db: db} }

func (s *BackupService) ListBackups() ([]database.Backup, error) {
	backups, err := s.db.GetAllBackups()
	if err != nil {
		return nil, err
	}
	for i, b := range backups {
		switch b.Target.Type {
		case database.TargetTypeAgent:
			if sess := sessions.GetSessionARPCFS(b.GetStreamID()); sess != nil {
				backups[i].CurrentStats = jobStatsFromVFS(sess.GetStats())
			}
		case database.TargetTypeS3:
			if sess := sessions.GetSessionS3FS(b.GetStreamID()); sess != nil {
				backups[i].CurrentStats = jobStatsFromVFS(sess.GetStats())
			}
		}
	}
	return backups, nil
}

func (s *BackupService) GetBackup(id string) (database.Backup, error) { return s.db.GetBackup(id) }
func (s *BackupService) CreateBackup(b database.Backup) error         { return s.db.CreateBackup(nil, b) }
func (s *BackupService) UpdateBackup(b database.Backup) error         { return s.db.UpdateBackup(nil, b) }
func (s *BackupService) DeleteBackup(id string) error                 { return s.db.DeleteBackup(nil, id) }
func (s *BackupService) GetAllQueuedBackups() ([]database.Backup, error) {
	return s.db.GetAllQueuedBackups()
}

func jobStatsFromVFS(stats vfs.VFSStats) database.JobStats {
	return database.JobStats{
		CurrentFileCount:   int(stats.FilesAccessed),
		CurrentFolderCount: int(stats.FoldersAccessed),
		CurrentBytesTotal:  int(stats.TotalBytes),
		CurrentBytesSpeed:  int(stats.ByteReadSpeed),
		CurrentFilesSpeed:  int(stats.FileAccessSpeed),
		StatCacheHits:      int(stats.StatCacheHits),
	}
}

// --- RestoreService ---

type RestoreService struct{ db *database.Database }

func NewRestoreService(db *database.Database) *RestoreService { return &RestoreService{db: db} }

func (s *RestoreService) GetAllRestores() ([]database.Restore, error)    { return s.db.GetAllRestores() }
func (s *RestoreService) GetRestore(id string) (database.Restore, error) { return s.db.GetRestore(id) }
func (s *RestoreService) CreateRestore(r database.Restore) error         { return s.db.CreateRestore(nil, r) }
func (s *RestoreService) UpdateRestore(r database.Restore) error         { return s.db.UpdateRestore(nil, r) }
func (s *RestoreService) DeleteRestore(id string) error                  { return s.db.DeleteRestore(nil, id) }

// --- ExclusionService ---

type ExclusionService struct{ db *database.Database }

func NewExclusionService(db *database.Database) *ExclusionService { return &ExclusionService{db: db} }

func (s *ExclusionService) GetAllGlobalExclusions() ([]database.Exclusion, error) {
	return s.db.GetAllGlobalExclusions()
}
func (s *ExclusionService) GetExclusion(path string) (*database.Exclusion, error) {
	return s.db.GetExclusion(path)
}
func (s *ExclusionService) CreateExclusion(e database.Exclusion) error {
	return s.db.CreateExclusion(nil, e)
}
func (s *ExclusionService) UpdateExclusion(e database.Exclusion) error {
	return s.db.UpdateExclusion(nil, e)
}
func (s *ExclusionService) DeleteExclusion(path string) error { return s.db.DeleteExclusion(nil, path) }

// --- AgentHostService ---

type AgentHostService struct{ db *database.Database }

func NewAgentHostService(db *database.Database) *AgentHostService { return &AgentHostService{db: db} }

func (s *AgentHostService) GetAgentHost(hostname string) (database.AgentHost, error) {
	return s.db.GetAgentHost(hostname)
}
func (s *AgentHostService) CreateAgentHost(tx *database.Transaction, h database.AgentHost) error {
	return s.db.CreateAgentHost(tx, h)
}
func (s *AgentHostService) UpdateAgentHost(tx *database.Transaction, h database.AgentHost) error {
	return s.db.UpdateAgentHost(tx, h)
}
func (s *AgentHostService) DeleteAgentHost(hostname string) error {
	return s.db.DeleteAgentHost(nil, hostname)
}

// --- TokenService ---

type TokenService struct{ db *database.Database }

func NewTokenService(db *database.Database) *TokenService { return &TokenService{db: db} }

func (s *TokenService) GetAllTokens() ([]database.AgentToken, error)    { return s.db.GetAllTokens(false) }
func (s *TokenService) GetToken(id string) (database.AgentToken, error) { return s.db.GetToken(id) }
func (s *TokenService) CreateToken(d time.Duration, c string) error     { return s.db.CreateToken(d, c) }
func (s *TokenService) RevokeToken(t database.AgentToken) error         { return s.db.RevokeToken(t) }

// --- ScriptService ---

type ScriptService struct{ db *database.Database }

func NewScriptService(db *database.Database) *ScriptService { return &ScriptService{db: db} }

func (s *ScriptService) GetAllScripts() ([]database.Script, error)      { return s.db.GetAllScripts() }
func (s *ScriptService) GetScript(path string) (database.Script, error) { return s.db.GetScript(path) }
func (s *ScriptService) CreateScript(sc database.Script) error          { return s.db.CreateScript(nil, sc) }
func (s *ScriptService) UpdateScript(sc database.Script) error          { return s.db.UpdateScript(nil, sc) }
func (s *ScriptService) DeleteScript(path string) error                 { return s.db.DeleteScript(nil, path) }

// --- TargetService ---

type TargetService struct {
	db        *database.Database
	agentsMgr *arpc.AgentsManager
}

func NewTargetService(db *database.Database, agentsMgr *arpc.AgentsManager) *TargetService {
	return &TargetService{db: db, agentsMgr: agentsMgr}
}

func (s *TargetService) GetAllTargets() ([]database.Target, error)      { return s.db.GetAllTargets() }
func (s *TargetService) GetTarget(name string) (database.Target, error) { return s.db.GetTarget(name) }
func (s *TargetService) CreateTarget(tx *database.Transaction, t database.Target) error {
	return s.db.CreateTarget(tx, t)
}
func (s *TargetService) UpdateTarget(tx *database.Transaction, t database.Target) error {
	return s.db.UpdateTarget(tx, t)
}
func (s *TargetService) DeleteTarget(tx *database.Transaction, name string) error {
	return s.db.DeleteTarget(tx, name)
}
func (s *TargetService) UpsertTarget(tx *database.Transaction, t database.Target) error {
	return s.db.UpsertTarget(tx, t)
}
func (s *TargetService) AddS3Secret(name, secret string) error {
	return s.db.AddS3Secret(nil, name, secret)
}
func (s *TargetService) NewTransaction() (*database.Transaction, error) {
	return s.db.NewTransaction()
}

type TargetStatusResult struct {
	Index            int
	AgentVersion     string
	ConnectionStatus bool
	Error            error
}

func (s *TargetService) CheckStatus(ctx context.Context, targets []database.Target, checkStatus bool, timeout time.Duration) []TargetStatusResult {
	results := make([]TargetStatusResult, len(targets))
	sem := make(chan struct{}, 20)
	var wg sync.WaitGroup

	for i, target := range targets {
		wg.Add(1)
		go func(idx int, tgt database.Target) {
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
						&reqTypes.TargetStatusReq{Drive: tgt.VolumeID})
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
			if !ok {
				results[idx] = result
				return
			}
			result.AgentVersion = arpcSess.GetVersion()
			if checkStatus {
				timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				respMsg, err := arpcSess.CallMessage(timeoutCtx, "target_status",
					&reqTypes.TargetStatusReq{Drive: tgt.VolumeID})
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

// Ensure fmt is used (needed by sprintf patterns elsewhere)
