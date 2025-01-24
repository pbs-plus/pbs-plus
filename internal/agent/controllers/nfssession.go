//go:build windows

package controllers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/nfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

type NFSSessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*NFSSessionData
	filepath string
}

type NFSSessionData struct {
	Drive         string    `json:"drive"`
	StartTime     time.Time `json:"start_time"`
	ExcludedPaths []string  `json:"excluded_paths_patterns"`
	PartialFiles  []string  `json:"partial_files_patterns"`
}

func regexpToPatterns(regexps []*regexp.Regexp) []string {
	patterns := make([]string, len(regexps))
	for i, r := range regexps {
		patterns[i] = r.String()
	}
	return patterns
}

var (
	store *NFSSessionStore
	once  sync.Once
)

func GetNFSSessionStore() *NFSSessionStore {
	once.Do(func() {
		execPath, err := os.Executable()
		if err != nil {
			panic(err)
		}
		storePath := filepath.Join(filepath.Dir(execPath), "nfssessions.json")
		store = &NFSSessionStore{
			sessions: make(map[string]*NFSSessionData),
			filepath: storePath,
		}
		store.load()
	})
	return store
}

func (s *NFSSessionStore) HasSessions() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions) > 0
}

func (s *NFSSessionStore) load() {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.filepath)
	if err != nil {
		if !os.IsNotExist(err) {
			syslog.L.Errorf("Error reading session store: %v", err)
		}
		return
	}

	if err := json.Unmarshal(data, &s.sessions); err != nil {
		syslog.L.Errorf("Error unmarshaling session store: %v", err)
	}
}

func (s *NFSSessionStore) save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := json.MarshalIndent(s.sessions, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.filepath, data, 0644)
}

func (s *NFSSessionStore) Store(drive string, session *nfs.NFSSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sessionData := &NFSSessionData{
		Drive:         drive,
		StartTime:     time.Now(),
		ExcludedPaths: regexpToPatterns(session.ExcludedPaths),
		PartialFiles:  regexpToPatterns(session.PartialFiles),
	}

	s.sessions[drive] = sessionData
	nfsSessions.Store(drive, session)

	return s.save()
}

func (s *NFSSessionStore) Load(drive string) (*NFSSessionData, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, ok := s.sessions[drive]
	return session, ok
}

func (s *NFSSessionStore) Delete(drive string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sessions, drive)
	nfsSessions.Delete(drive)

	return s.save()
}
