package pxarmount

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"
)

// Journal is the SQLite-backed authoritative record of all mutations.
// It replaces the previous CBOR transaction log + in-memory overlay approach.
//
// Schema:
//
//	CREATE TABLE entries (
//	  path TEXT PRIMARY KEY,
//	  state TEXT NOT NULL,
//	  mode INTEGER,
//	  uid INTEGER,
//	  gid INTEGER,
//	  size INTEGER,
//	  mtime_ns INTEGER,
//	  ctime_ns INTEGER,
//	  has_data INTEGER NOT NULL DEFAULT 0,
//	  target TEXT
//	);
//	CREATE TABLE xattrs (
//	  path TEXT,
//	  name TEXT,
//	  value BLOB,
//	  PRIMARY KEY (path, name)
//	);
type Journal struct {
	db *sql.DB
	mu sync.Mutex // serializes write transactions
}

// OpenJournal opens or creates the journal database in dir.
func OpenJournal(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create journal dir: %w", err)
	}

	dbPath := filepath.Join(dir, "journal.db")
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open journal db: %w", err)
	}

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate journal: %w", err)
	}

	return &Journal{db: db}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS entries (
			path TEXT PRIMARY KEY,
			parent_path TEXT NOT NULL DEFAULT '',
			name TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL,
			mode INTEGER,
			uid INTEGER,
			gid INTEGER,
			size INTEGER,
			mtime_ns INTEGER,
			ctime_ns INTEGER,
			has_data INTEGER NOT NULL DEFAULT 0,
			target TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(parent_path, name);
		CREATE TABLE IF NOT EXISTS xattrs (
			path TEXT,
			name TEXT,
			value BLOB,
			PRIMARY KEY (path, name)
		);
	`)
	return err
}

// Close closes the journal database.
func (j *Journal) Close() error {
	return j.db.Close()
}

// --- transactional helpers ---

// tx executes fn within a single SQLite transaction.
func (j *Journal) tx(fn func(tx *sql.Tx) error) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	tx, err := j.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// --- entry CRUD ---

// GetEntry returns the journal entry for a path, or nil if not found.
func (j *Journal) GetEntry(path string) (*JournalEntry, error) {
	row := j.db.QueryRow(`
		SELECT state, mode, uid, gid, size, mtime_ns, ctime_ns, has_data, target
		FROM entries WHERE path = ?`, path)
	je := &JournalEntry{Path: path}
	var state string
	var mode, uid, gid, hasData sql.NullInt64
	var size, mtimeNs, ctimeNs sql.NullInt64
	var target sql.NullString

	err := row.Scan(&state, &mode, &uid, &gid, &size, &mtimeNs, &ctimeNs, &hasData, &target)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	je.State = EntryState(state)
	if mode.Valid {
		je.Mode = uint32(mode.Int64)
	}
	if uid.Valid {
		je.UID = uint32(uid.Int64)
	}
	if gid.Valid {
		je.GID = uint32(gid.Int64)
	}
	if size.Valid {
		je.Size = uint64(size.Int64)
	}
	if mtimeNs.Valid {
		je.MtimeNs = mtimeNs.Int64
	}
	if ctimeNs.Valid {
		je.CtimeNs = ctimeNs.Int64
	}
	if hasData.Valid {
		je.HasData = hasData.Int64 != 0
	}
	if target.Valid {
		je.Target = target.String
	}
	return je, nil
}

// UpsertEntry inserts or replaces a journal entry within a transaction.
func upsertEntry(tx *sql.Tx, je *JournalEntry) error {
	hasData := 0
	if je.HasData {
		hasData = 1
	}
	parentPath, name := splitParent(je.Path)
	_, err := tx.Exec(`
		INSERT OR REPLACE INTO entries (path, parent_path, name, state, mode, uid, gid, size, mtime_ns, ctime_ns, has_data, target)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		je.Path, parentPath, name, string(je.State), je.Mode, je.UID, je.GID,
		int64(je.Size), je.MtimeNs, je.CtimeNs, hasData, je.Target)
	return err
}

// DeleteEntry removes the journal entry for a path within a transaction.
func deleteEntryTx(tx *sql.Tx, path string) error {
	if _, err := tx.Exec(`DELETE FROM entries WHERE path = ?`, path); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM xattrs WHERE path = ?`, path); err != nil {
		return err
	}
	return nil
}

// --- mutation operations ---

// MarkModified records a metadata modification.
func (j *Journal) MarkModified(je *JournalEntry) error {
	je.State = StateModified
	return j.tx(func(tx *sql.Tx) error {
		return upsertEntry(tx, je)
	})
}

// MarkNew records a newly created entry.
func (j *Journal) MarkNew(je *JournalEntry) error {
	je.State = StateNew
	return j.tx(func(tx *sql.Tx) error {
		return upsertEntry(tx, je)
	})
}

// MarkWhiteout records a deletion of an immutable entry.
func (j *Journal) MarkWhiteout(path string) error {
	return j.tx(func(tx *sql.Tx) error {
		return upsertEntry(tx, &JournalEntry{Path: path, State: StateWhiteout})
	})
}

// MarkOpaque marks a directory as opaque (immutable contents hidden).
func (j *Journal) MarkOpaque(path string, je *JournalEntry) error {
	je.State = StateOpaque
	return j.tx(func(tx *sql.Tx) error {
		return upsertEntry(tx, je)
	})
}

// RemoveEntry removes a non-immutable entry entirely from the journal.
func (j *Journal) RemoveEntry(path string) error {
	return j.tx(func(tx *sql.Tx) error {
		return deleteEntryTx(tx, path)
	})
}

// SetHasData marks that an entry now has data in the mutable dir.
func (j *Journal) SetHasData(path string) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE entries SET has_data = 1 WHERE path = ?`, path)
		return err
	})
}

// Rename renames source to dest in the journal within a single transaction.
// If dest has an immutable counterpart, it becomes a whiteout.
func (j *Journal) Rename(oldPath, newPath string, destHasImmutable bool) error {
	return j.tx(func(tx *sql.Tx) error {
		je, err := getEntryTx(tx, oldPath)
		if err != nil {
			return err
		}
		if je == nil {
			return fmt.Errorf("rename source %q not in journal", oldPath)
		}

		// Remove old entry.
		if err := deleteEntryTx(tx, oldPath); err != nil {
			return err
		}

		// If dest has an immutable counterpart, mark it whiteout first.
		if destHasImmutable {
			if err := upsertEntry(tx, &JournalEntry{Path: newPath, State: StateWhiteout}); err != nil {
				return err
			}
		}

		// Move xattrs.
		if _, err := tx.Exec(`UPDATE xattrs SET path = ? WHERE path = ?`, newPath, oldPath); err != nil {
			return err
		}

		// Insert at new path.
		je.Path = newPath
		return upsertEntry(tx, je)
	})
}

// RenameImmutable records a whiteout at oldPath and a modified entry at newPath.
// Used when renaming a purely immutable entry without materializing.
func (j *Journal) RenameImmutable(oldPath, newPath string, je *JournalEntry) error {
	return j.tx(func(tx *sql.Tx) error {
		if err := upsertEntry(tx, &JournalEntry{Path: oldPath, State: StateWhiteout}); err != nil {
			return err
		}
		je.Path = newPath
		je.State = StateModified
		je.HasData = false
		return upsertEntry(tx, je)
	})
}

// --- xattr operations ---

// GetXAttr gets a single xattr value for a path.
func (j *Journal) GetXAttr(path, name string) ([]byte, error) {
	var val []byte
	err := j.db.QueryRow(`SELECT value FROM xattrs WHERE path = ? AND name = ?`, path, name).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return val, err
}

// ListXAttrs returns all xattr names for a path.
func (j *Journal) ListXAttrs(path string) ([]string, error) {
	rows, err := j.db.Query(`SELECT name FROM xattrs WHERE path = ?`, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// SetXAttr writes an xattr within a transaction.
func (j *Journal) SetXAttr(path, name string, value []byte) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO xattrs (path, name, value) VALUES (?, ?, ?)`,
			path, name, value)
		return err
	})
}

// RemoveXAttr deletes an xattr within a transaction.
func (j *Journal) RemoveXAttr(path, name string) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM xattrs WHERE path = ? AND name = ?`, path, name)
		return err
	})
}

// --- readdir helpers ---

// ListDir returns all journal entries that are immediate children of parentDir.
// Uses the idx_entries_parent index for efficient lookup.
func (j *Journal) ListDir(parentDir string) ([]JournalEntry, error) {
	rows, err := j.db.Query(`
		SELECT path, state, mode, uid, gid, size, mtime_ns, ctime_ns, has_data, target
		FROM entries WHERE parent_path = ? ORDER BY name`, parentDir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []JournalEntry
	for rows.Next() {
		var je JournalEntry
		var state string
		var mode, uid, gid, hasData sql.NullInt64
		var size, mtimeNs, ctimeNs sql.NullInt64
		var target sql.NullString

		if err := rows.Scan(&je.Path, &state, &mode, &uid, &gid, &size, &mtimeNs, &ctimeNs, &hasData, &target); err != nil {
			return nil, err
		}

		je.State = EntryState(state)
		if mode.Valid {
			je.Mode = uint32(mode.Int64)
		}
		if uid.Valid {
			je.UID = uint32(uid.Int64)
		}
		if gid.Valid {
			je.GID = uint32(gid.Int64)
		}
		if size.Valid {
			je.Size = uint64(size.Int64)
		}
		if mtimeNs.Valid {
			je.MtimeNs = mtimeNs.Int64
		}
		if ctimeNs.Valid {
			je.CtimeNs = ctimeNs.Int64
		}
		if hasData.Valid {
			je.HasData = hasData.Int64 != 0
		}
		if target.Valid {
			je.Target = target.String
		}
		entries = append(entries, je)
	}
	return entries, rows.Err()
}

// IsOpaque returns true if the path has an opaque journal entry.
func (j *Journal) IsOpaque(path string) (bool, error) {
	var state string
	err := j.db.QueryRow(`SELECT state FROM entries WHERE path = ? AND state = 'opaque'`, path).Scan(&state)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// --- internal helpers ---

func getEntryTx(tx *sql.Tx, path string) (*JournalEntry, error) {
	row := tx.QueryRow(`
		SELECT state, mode, uid, gid, size, mtime_ns, ctime_ns, has_data, target
		FROM entries WHERE path = ?`, path)
	je := &JournalEntry{Path: path}
	var state string
	var mode, uid, gid, hasData sql.NullInt64
	var size, mtimeNs, ctimeNs sql.NullInt64
	var target sql.NullString

	err := row.Scan(&state, &mode, &uid, &gid, &size, &mtimeNs, &ctimeNs, &hasData, &target)
	if err != nil {
		return nil, err
	}

	je.State = EntryState(state)
	if mode.Valid {
		je.Mode = uint32(mode.Int64)
	}
	if uid.Valid {
		je.UID = uint32(uid.Int64)
	}
	if gid.Valid {
		je.GID = uint32(gid.Int64)
	}
	if size.Valid {
		je.Size = uint64(size.Int64)
	}
	if mtimeNs.Valid {
		je.MtimeNs = mtimeNs.Int64
	}
	if ctimeNs.Valid {
		je.CtimeNs = ctimeNs.Int64
	}
	if hasData.Valid {
		je.HasData = hasData.Int64 != 0
	}
	if target.Valid {
		je.Target = target.String
	}
	return je, nil
}

// AllEntries returns all entries in the journal for commit walking.
func (j *Journal) AllEntries() ([]JournalEntry, error) {
	rows, err := j.db.Query(`
		SELECT path, state, mode, uid, gid, size, mtime_ns, ctime_ns, has_data, target
		FROM entries ORDER BY path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []JournalEntry
	for rows.Next() {
		var je JournalEntry
		var state string
		var mode, uid, gid, hasData sql.NullInt64
		var size, mtimeNs, ctimeNs sql.NullInt64
		var target sql.NullString

		if err := rows.Scan(&je.Path, &state, &mode, &uid, &gid, &size, &mtimeNs, &ctimeNs, &hasData, &target); err != nil {
			return nil, err
		}
		je.State = EntryState(state)
		if mode.Valid {
			je.Mode = uint32(mode.Int64)
		}
		if uid.Valid {
			je.UID = uint32(uid.Int64)
		}
		if gid.Valid {
			je.GID = uint32(gid.Int64)
		}
		if size.Valid {
			je.Size = uint64(size.Int64)
		}
		if mtimeNs.Valid {
			je.MtimeNs = mtimeNs.Int64
		}
		if ctimeNs.Valid {
			je.CtimeNs = ctimeNs.Int64
		}
		if hasData.Valid {
			je.HasData = hasData.Int64 != 0
		}
		if target.Valid {
			je.Target = target.String
		}
		entries = append(entries, je)
	}
	return entries, rows.Err()
}

// AllXAttrs returns all xattrs grouped by path for commit.
func (j *Journal) AllXAttrs() (map[string]map[string][]byte, error) {
	rows, err := j.db.Query(`SELECT path, name, value FROM xattrs ORDER BY path, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]map[string][]byte)
	for rows.Next() {
		var path, name string
		var value []byte
		if err := rows.Scan(&path, &name, &value); err != nil {
			return nil, err
		}
		if result[path] == nil {
			result[path] = make(map[string][]byte)
		}
		result[path][name] = value
	}
	return result, rows.Err()
}

// Clear truncates all tables in the journal.
func (j *Journal) Clear() error {
	return j.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM xattrs`); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM entries`); err != nil {
			return err
		}
		return nil
	})
}

// splitParent returns (parent_path, name) for a given path.
// "/" returns ("", ""). "/foo" returns ("/", "foo").
// "/foo/bar" returns ("/foo", "bar").
func splitParent(path string) (string, string) {
	if path == "/" || path == "" {
		return "", ""
	}
	idx := len(path) - 1
	for idx >= 0 && path[idx] != '/' {
		idx--
	}
	if idx <= 0 {
		return "/", path[1:]
	}
	return path[:idx], path[idx+1:]
}
