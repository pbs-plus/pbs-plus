package pxarmount

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	_ "modernc.org/sqlite"
)

// Node kinds stored in the journal graph.
const (
	NodeDir     uint8 = 0
	NodeFile    uint8 = 1
	NodeSymlink uint8 = 2
)

// GraphNode is a filesystem entry in the journal's inode graph.
type GraphNode struct {
	ID         int64
	Kind       uint8
	Mode       uint32
	UID        uint32
	GID        uint32
	Size       uint64
	MtimeNs    int64
	CtimeNs    int64
	HasData    bool
	SymlinkTgt string
	RedirectTo string // pxar path for lazy materialization
	Opaque     bool   // if true, don't merge pxar children
}

// GraphEdge is a parent→child name binding.
type GraphEdge struct {
	ParentID int64
	Name     string
	ChildID  int64
}

// schemaVersion is the current journal schema version.
// Increment when making non-trivial schema changes. OpenJournal
// will run migrations automatically — analogous to ext4's feature
// compatibility flags checked at mount time.
const schemaVersion = 1

// Journal is the SQLite-backed inode graph for the mutable overlay.
//
// Schema:
//
//	meta(key, value)                  — stores schema_version, etc.
//	nodes(id, kind, mode, uid, gid, size, mtime_ns, ctime_ns, has_data,
//	      symlink_tgt, redirect_to, opaque)
//	edges(parent_id, name, child_id)  PK(parent_id, name)
//	xattrs(node_id, name, value)      PK(node_id, name)
//	whiteouts(parent_id, name)        PK(parent_id, name)
//
// Root node (id=1) always exists with redirect_to='/'.
// Renames are O(1): just update the edge row. No descendants touched.
//
// Durability follows the filesystem journaling model: writes are
// committed to the WAL immediately but checkpoints (which fsync the
// main DB) are deferred to a background goroutine. This avoids an
// fsync per transaction while maintaining crash consistency — the WAL
// is replayed automatically on recovery. Explicit Sync() forces a
// full checkpoint for durability-critical paths (commit, fsync).
type Journal struct {
	db *sql.DB
	mu sync.Mutex // serializes write transactions

	// Periodic checkpoint state. Modeled after ext4's commit=5s:
	// mutations accumulate in the WAL, a background goroutine
	// checkpoints every checkpointInterval. Sync() forces an
	// immediate checkpoint (like fsync).
	ckptDone    chan struct{}
	ckptClose   chan struct{}
	ckptPending *xsync.Counter // approximate WAL size; incremented per tx

	// checkpointInterval controls how often the background
	// checkpoint goroutine runs. Analogous to ext4's commit= mount option.
	// Atomic for safe concurrent reads from the ticker goroutine.
	checkpointInterval atomic.Int64 // nanoseconds
}

// OpenJournal opens or creates the journal database in dir.
// Performs crash recovery analogous to ext4's journal replay:
//  1. WAL auto-replay (SQLite handles this)
//  2. Schema version check and migration
//  3. Root node integrity verification
//  4. Orphan edge cleanup (dangling references from partial tx)
func OpenJournal(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create journal dir: %w", err)
	}

	dbPath := filepath.Join(dir, "journal.db")
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open journal db: %w", err)
	}

	// Verify the database is accessible and not corrupt.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping journal db: %w", err)
	}

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	// Integrity check: like ext4's ext4_check_descriptors() at mount time.
	// Catches corrupt pages from torn writes that WAL replay couldn't fix.
	var ok string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&ok); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("integrity check: %w", err)
	}
	if ok != "ok" {
		_ = db.Close()
		return nil, fmt.Errorf("integrity check failed: %s", ok)
	}

	// Create or migrate schema.
	if err := initSchema(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	if err := migrateSchema(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}

	// Verify root node survived any prior crash — analogous to ext4's
	// ext4_orphan_cleanup() which runs at mount time.
	var rootCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM nodes WHERE id = 1`).Scan(&rootCount); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("verify root node: %w", err)
	}
	if rootCount == 0 {
		if _, err := db.Exec(
			`INSERT INTO nodes (id, kind, mode, redirect_to) VALUES (1, 0, 16877, '/')`); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("recreate root node: %w", err)
		}
	}

	// Clean up orphan edges: edges pointing to non-existent nodes.
	// This can happen if a process crash occurs between node creation
	// and edge insertion in a compound operation. Like ext4's
	// ext4_orphan_cleanup().
	if _, err := db.Exec(`
		DELETE FROM edges
		WHERE child_id NOT IN (SELECT id FROM nodes)`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("clean orphan edges: %w", err)
	}

	j := &Journal{db: db, ckptPending: xsync.NewCounter()}
	j.startCheckpointLoop()
	return j, nil
}

// initSchema creates the journal database tables.
func initSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		) WITHOUT ROWID;
		INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '` + fmt.Sprint(schemaVersion) + `');

		CREATE TABLE IF NOT EXISTS nodes (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			kind        INTEGER NOT NULL,
			mode        INTEGER NOT NULL DEFAULT 0,
			uid         INTEGER NOT NULL DEFAULT 0,
			gid         INTEGER NOT NULL DEFAULT 0,
			size        INTEGER NOT NULL DEFAULT 0,
			mtime_ns    INTEGER NOT NULL DEFAULT 0,
			ctime_ns    INTEGER NOT NULL DEFAULT 0,
			has_data    INTEGER NOT NULL DEFAULT 0,
			symlink_tgt TEXT,
			redirect_to TEXT,
			opaque      INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS edges (
			parent_id   INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
			name        TEXT NOT NULL,
			child_id    INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
			PRIMARY KEY (parent_id, name)
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS idx_edges_child ON edges(child_id);
		CREATE TABLE IF NOT EXISTS xattrs (
			node_id     INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
			name        TEXT NOT NULL,
			value       BLOB NOT NULL,
			PRIMARY KEY (node_id, name)
		) WITHOUT ROWID;
		CREATE TABLE IF NOT EXISTS whiteouts (
			parent_id   INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
			name        TEXT NOT NULL,
			PRIMARY KEY (parent_id, name)
		) WITHOUT ROWID;
		INSERT OR IGNORE INTO nodes (id, kind, mode, redirect_to) VALUES (1, 0, 16877, '/');
	`)
	return err
}

// migrateSchema applies schema migrations. Analogous to ext4's
// feature compatibility check + online resize — runs once at open.
// Future schema changes should add cases here.
func migrateSchema(db *sql.DB) error {
	var ver string
	if err := db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&ver); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	// No migrations yet — schema is at version 1.
	// Example future migration:
	//   if ver == "1" {
	//       _, err := db.Exec(`ALTER TABLE nodes ADD COLUMN new_col ...`)
	//       _, err = db.Exec(`UPDATE meta SET value = '2' WHERE key = 'schema_version'`)
	//   }
	_ = ver
	return nil
}

// VerifyIntegrity runs a full database consistency check.
// Analogous to ext4's e2fsck -n (read-only check):
//   - SQLite integrity_check pragma
//   - Foreign key constraint verification
//   - Root node existence
//   - No orphan edges (edges without matching nodes)
//   - No orphan xattrs (xattrs without matching nodes)
func (j *Journal) VerifyIntegrity() error {
	// 1. SQLite built-in integrity check.
	var ok string
	if err := j.db.QueryRow("PRAGMA integrity_check").Scan(&ok); err != nil {
		return fmt.Errorf("integrity_check: %w", err)
	}
	if ok != "ok" {
		return fmt.Errorf("integrity_check failed: %s", ok)
	}

	// 2. Foreign key violation check.
	rows, err := j.db.Query("PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("foreign_key_check: %w", err)
	}
	for rows.Next() {
		var table, from, to string
		var rowid, fkid int64
		_ = rows.Scan(&table, &rowid, &from, &to, &fkid)
		_ = rows.Close()
		return fmt.Errorf("foreign key violation: %s row %d references %s.%s", table, rowid, to, from)
	}
	_ = rows.Close()

	// 3. Root node must exist.
	var rootCount int
	if err := j.db.QueryRow(`SELECT COUNT(*) FROM nodes WHERE id = 1`).Scan(&rootCount); err != nil {
		return fmt.Errorf("root node check: %w", err)
	}
	if rootCount == 0 {
		return fmt.Errorf("root node missing")
	}

	// 4. No orphan edges.
	var orphanEdges int
	if err := j.db.QueryRow(`
		SELECT COUNT(*) FROM edges e
		WHERE e.parent_id NOT IN (SELECT id FROM nodes)
		   OR e.child_id NOT IN (SELECT id FROM nodes)`).Scan(&orphanEdges); err != nil {
		return fmt.Errorf("orphan edge check: %w", err)
	}
	if orphanEdges > 0 {
		return fmt.Errorf("%d orphan edges detected", orphanEdges)
	}

	// 5. No orphan xattrs.
	var orphanXattrs int
	if err := j.db.QueryRow(`
		SELECT COUNT(*) FROM xattrs x
		WHERE x.node_id NOT IN (SELECT id FROM nodes)`).Scan(&orphanXattrs); err != nil {
		return fmt.Errorf("orphan xattr check: %w", err)
	}
	if orphanXattrs > 0 {
		return fmt.Errorf("%d orphan xattrs detected", orphanXattrs)
	}

	return nil
}

func (j *Journal) Close() error {
	// Signal checkpoint goroutine to stop, then wait for it.
	if j.ckptClose != nil {
		close(j.ckptClose)
	}
	if j.ckptDone != nil {
		<-j.ckptDone
	}

	// Final checkpoint to flush everything before closing.
	j.mu.Lock()
	_ = j.checkpoint()
	j.mu.Unlock()
	return j.db.Close()
}

// tx executes fn within a single SQLite transaction.
// The commit is durable in the WAL (survives process crash; SQLite
// replays the WAL on next open). Periodic background checkpointing
// moves WAL contents to the main DB file — analogous to ext4's
// commit=5s journal flush.
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
	if err := tx.Commit(); err != nil {
		return err
	}

	// Track pending WAL frames for the checkpoint goroutine.
	j.ckptPending.Add(1)

	// Eager checkpoint if enough frames have accumulated.
	if j.ckptPending.Value() >= 256 {
		_ = j.checkpoint()
	}

	return nil
}

// Sync forces a full WAL checkpoint (TRUNCATE), making all committed
// transactions durable in the main DB file. Call this from FUSE's
// Fsync path and the commit path — analogous to ext4's fsync().
func (j *Journal) Sync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.checkpoint()
}

// checkpoint moves WAL contents into the main DB and truncates the WAL.
// Caller must hold j.mu.
func (j *Journal) checkpoint() error {
	_, err := j.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	j.ckptPending.Reset()
	return err
}

// startCheckpointLoop launches the background periodic checkpoint goroutine.
func (j *Journal) startCheckpointLoop() {
	if j.checkpointInterval.Load() <= 0 {
		j.checkpointInterval.Store(int64(5 * time.Second))
	}
	j.ckptDone = make(chan struct{})
	j.ckptClose = make(chan struct{})
	go func() {
		defer close(j.ckptDone)
		ticker := time.NewTicker(time.Duration(j.checkpointInterval.Load()))
		defer ticker.Stop()
		for {
			select {
			case <-j.ckptClose:
				return
			case <-ticker.C:
				if j.ckptPending.Value() > 0 {
					j.mu.Lock()
					_ = j.checkpoint()
					j.mu.Unlock()
				}
			}
		}
	}()
}

// --- Node CRUD ---

// GetNode returns the node by ID, or nil if not found.
func (j *Journal) GetNode(id int64) (*GraphNode, error) {
	row := j.db.QueryRow(`
		SELECT kind, mode, uid, gid, size, mtime_ns, ctime_ns, has_data,
		       symlink_tgt, redirect_to, opaque
		FROM nodes WHERE id = ?`, id)
	return scanNode(row, id)
}

// scanNode scans a single database row into a GraphNode.
func scanNode(row interface{ Scan(...any) error }, id int64) (*GraphNode, error) {
	n := &GraphNode{ID: id}
	var kind, mode, uid, gid, hasData, opaque sql.NullInt64
	var size, mtimeNs, ctimeNs sql.NullInt64
	var symlinkTgt, redirectTo sql.NullString

	err := row.Scan(&kind, &mode, &uid, &gid, &size, &mtimeNs, &ctimeNs,
		&hasData, &symlinkTgt, &redirectTo, &opaque)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if kind.Valid {
		n.Kind = uint8(kind.Int64)
	}
	if mode.Valid {
		n.Mode = uint32(mode.Int64)
	}
	if uid.Valid {
		n.UID = uint32(uid.Int64)
	}
	if gid.Valid {
		n.GID = uint32(gid.Int64)
	}
	if size.Valid {
		n.Size = uint64(size.Int64)
	}
	if mtimeNs.Valid {
		n.MtimeNs = mtimeNs.Int64
	}
	if ctimeNs.Valid {
		n.CtimeNs = ctimeNs.Int64
	}
	if hasData.Valid {
		n.HasData = hasData.Int64 != 0
	}
	if symlinkTgt.Valid {
		n.SymlinkTgt = symlinkTgt.String
	}
	if redirectTo.Valid {
		n.RedirectTo = redirectTo.String
	}
	if opaque.Valid {
		n.Opaque = opaque.Int64 != 0
	}
	return n, nil
}

// createNodeTx inserts a new node within a transaction and returns its ID.
func createNodeTx(tx *sql.Tx, n *GraphNode) (int64, error) {
	hasData := 0
	if n.HasData {
		hasData = 1
	}
	opaque := 0
	if n.Opaque {
		opaque = 1
	}
	res, err := tx.Exec(`
		INSERT INTO nodes (kind, mode, uid, gid, size, mtime_ns, ctime_ns,
		                   has_data, symlink_tgt, redirect_to, opaque)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		n.Kind, n.Mode, n.UID, n.GID, n.Size, n.MtimeNs, n.CtimeNs,
		hasData, n.SymlinkTgt, n.RedirectTo, opaque)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// updateNodeTx updates a node within a transaction.
func updateNodeTx(tx *sql.Tx, n *GraphNode) error {
	hasData := 0
	if n.HasData {
		hasData = 1
	}
	opaque := 0
	if n.Opaque {
		opaque = 1
	}
	_, err := tx.Exec(`
		UPDATE nodes SET kind=?, mode=?, uid=?, gid=?, size=?,
		                 mtime_ns=?, ctime_ns=?, has_data=?,
		                 symlink_tgt=?, redirect_to=?, opaque=?
		WHERE id=?`,
		n.Kind, n.Mode, n.UID, n.GID, n.Size,
		n.MtimeNs, n.CtimeNs, hasData,
		n.SymlinkTgt, n.RedirectTo, opaque, n.ID)
	return err
}

// UpdateNode updates a node's metadata.
func (j *Journal) UpdateNode(n *GraphNode) error {
	return j.tx(func(tx *sql.Tx) error {
		return updateNodeTx(tx, n)
	})
}

// SetHasData marks that a node now has data in the mutable dir.
func (j *Journal) SetHasData(nodeID int64) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE nodes SET has_data = 1 WHERE id = ?`, nodeID)
		return err
	})
}

// --- Edge CRUD ---

// ListEdges returns all edges under a parent.
func (j *Journal) ListEdges(parentID int64) ([]GraphEdge, error) {
	rows, err := j.db.Query(
		`SELECT name, child_id FROM edges WHERE parent_id = ? ORDER BY name`, parentID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var edges []GraphEdge
	for rows.Next() {
		var e GraphEdge
		if err := rows.Scan(&e.Name, &e.ChildID); err != nil {
			return nil, err
		}
		e.ParentID = parentID
		edges = append(edges, e)
	}
	return edges, rows.Err()
}

// --- Whiteout CRUD ---

// AddWhiteout records that a pxar entry at (parentID, name) is deleted.
func (j *Journal) AddWhiteout(parentID int64, name string) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR IGNORE INTO whiteouts (parent_id, name) VALUES (?, ?)`, parentID, name)
		return err
	})
}

// ListWhiteouts returns all whiteout names under a parent.
func (j *Journal) ListWhiteouts(parentID int64) ([]string, error) {
	rows, err := j.db.Query(`SELECT name FROM whiteouts WHERE parent_id = ?`, parentID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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

// --- XAttr Operations ---

// GetXAttr returns the value of an extended attribute, or nil if not found.
func (j *Journal) GetXAttr(nodeID int64, name string) ([]byte, error) {
	var val []byte
	err := j.db.QueryRow(
		`SELECT value FROM xattrs WHERE node_id = ? AND name = ?`, nodeID, name).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return val, err
}

// ListXAttrs returns all extended attribute names for a node.
func (j *Journal) ListXAttrs(nodeID int64) ([]string, error) {
	rows, err := j.db.Query(`SELECT name FROM xattrs WHERE node_id = ?`, nodeID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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

func (j *Journal) SetXAttr(nodeID int64, name string, value []byte) error {
// SetXAttr upserts an extended attribute value.
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO xattrs (node_id, name, value) VALUES (?, ?, ?)`,
			nodeID, name, value)
		return err
	})
}

func (j *Journal) RemoveXAttr(nodeID int64, name string) error {
	return j.tx(func(tx *sql.Tx) error {
// RemoveXAttr deletes an extended attribute.
		_, err := tx.Exec(`DELETE FROM xattrs WHERE node_id = ? AND name = ?`, nodeID, name)
		return err
	})
}

// --- Path Resolution ---

// ResolvePath walks the edge graph from root to find a path.
// Returns:
//   - nodeID: the journal node ID at the final component (0 if not in journal)
//   - pxarPath: the pxar source path for the remaining/entire path
//   - fellOffAt: the node ID where we fell off the graph (for whiteout checks)
//   - remaining: the remaining path components after falling off
//
// If nodeID != 0, the path is fully in the journal.
// If nodeID == 0, the path is partially or fully in pxar.
//
// All queries execute within a single read transaction, providing
// snapshot consistency across the entire walk — analogous to ext4's
// i_mutex on parent directories during lookup.
func (j *Journal) ResolvePath(path string) (nodeID int64, pxarPath string, fellOffAt int64, remaining string, err error) {
	if path == "/" || path == "" {
		return 1, "/", 0, "", nil
	}

	// Snapshot read: In SQLite WAL mode, BEGIN provides a consistent
	// view without blocking concurrent writers. This is the same
	// approach ext4 uses — directory lookups hold i_mutex so renames
	// can't interleave with lookups.
	tx, txErr := j.db.Begin()
	if txErr != nil {
		return 0, "", 0, "", fmt.Errorf("begin snapshot: %w", txErr)
	}
	defer func() { _ = tx.Rollback() }() // read-only: rollback is free

	// Zero-allocation path walker. Walk path segments by slicing the
	// original string — no []string allocation, no strings.Join.
	curID := int64(1)
	var pxarPrefix strings.Builder
	pxarPrefix.WriteByte('/')
	pos := 1 // start after leading /

	for pos < len(path) {
		// Find next segment.
		end := pos
		for end < len(path) && path[end] != '/' {
			end++
		}
		part := path[pos:end]

		// Look up edge within the snapshot.
		var childID int64
		err = tx.QueryRow(
			`SELECT child_id FROM edges WHERE parent_id = ? AND name = ?`,
			curID, part).Scan(&childID)
		if err == sql.ErrNoRows {
			// Check for whiteout within the same snapshot.
			var wo int
			werr := tx.QueryRow(
				`SELECT 1 FROM whiteouts WHERE parent_id = ? AND name = ?`,
				curID, part).Scan(&wo)
			if werr != nil && werr != sql.ErrNoRows {
				return 0, "", 0, "", werr
			}
			if werr == nil {
				return 0, "", 0, "", nil // whiteout
			}
			// Fell off graph — remaining is from current position.
			return 0, pxarPrefix.String() + path[pos-1:], curID, path[pos:], nil
		}
		if err != nil {
			return 0, "", 0, "", err
		}

		curID = childID

		// Track pxar redirect within the snapshot.
		var redirectTo sql.NullString
		if err := tx.QueryRow(
			`SELECT redirect_to FROM nodes WHERE id = ?`, curID).Scan(&redirectTo); err != nil {
			return 0, "", 0, "", err
		}
		if redirectTo.Valid && redirectTo.String != "" {
			pxarPrefix.Reset()
			pxarPrefix.WriteString(redirectTo.String)
		} else {
			pxarPrefix.WriteByte('/')
			pxarPrefix.WriteString(part)
		}

		pos = end + 1
	}

	return curID, pxarPrefix.String(), 0, "", nil
}

// --- Batch Operations for Commit ---

// AllXAttrs returns all xattrs grouped by node ID.
func (j *Journal) AllXAttrs() (map[int64]map[string][]byte, error) {
	rows, err := j.db.Query(`SELECT node_id, name, value FROM xattrs ORDER BY node_id, name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[int64]map[string][]byte)
	for rows.Next() {
		var nodeID int64
		var name string
		var value []byte
		if err := rows.Scan(&nodeID, &name, &value); err != nil {
			return nil, err
		}
		if result[nodeID] == nil {
			result[nodeID] = make(map[string][]byte)
		}
		result[nodeID][name] = value
	}
	return result, rows.Err()
}

// EnsureNodePath atomically creates a journal node and all intermediate
// edges/nodes for the given path in a single transaction.
// If whiteout is true and the entry has a pxar counterpart, a whiteout is added.
// This is the efficient, atomic replacement for the multi-transaction
// ensureNode path walk.
func (j *Journal) EnsureNodePath(path string, n *GraphNode, whiteout bool) (int64, error) {
	var nodeID int64
	err := j.tx(func(tx *sql.Tx) error {
		id, err := createNodeTx(tx, n)
		if err != nil {
			return err
		}
		nodeID = id

		// Walk path components from root.
		parts := splitPath(path)
		curParentID := int64(1) // root

		for i, name := range parts {
			if name == "" {
				continue
			}

			// Check if edge already exists.
			var childID int64
			err := tx.QueryRow(
				`SELECT child_id FROM edges WHERE parent_id = ? AND name = ?`,
				curParentID, name).Scan(&childID)
			if err != nil && err != sql.ErrNoRows {
				return err
			}

			if err == nil {
				// Edge exists, follow it.
				curParentID = childID
				continue
			}

			// No edge — clean up any stale whiteout.
			_, _ = tx.Exec(`DELETE FROM whiteouts WHERE parent_id = ? AND name = ?`, curParentID, name)

			if i == len(parts)-1 {
				// Final component — link to our node.
				if _, err := tx.Exec(
					`INSERT OR REPLACE INTO edges (parent_id, name, child_id) VALUES (?, ?, ?)`,
					curParentID, name, nodeID); err != nil {
					return err
				}
				if whiteout {
					if _, err := tx.Exec(
						`INSERT OR IGNORE INTO whiteouts (parent_id, name) VALUES (?, ?)`,
						curParentID, name); err != nil {
						return err
					}
				}
			} else {
				// Intermediate directory — create redirect node.
				var intermediatePath strings.Builder
				intermediatePath.WriteByte('/')
				intermediatePath.WriteString(parts[0])
				for j := 1; j <= i; j++ {
					intermediatePath.WriteByte('/')
					intermediatePath.WriteString(parts[j])
				}
				intermediate := &GraphNode{
					Kind:       NodeDir,
					Mode:       syscall.S_IFDIR | 0o755,
					UID:        n.UID,
					GID:        n.GID,
					MtimeNs:    n.MtimeNs,
					CtimeNs:    n.CtimeNs,
					RedirectTo: intermediatePath.String(),
				}
				midID, err := createNodeTx(tx, intermediate)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(
					`INSERT OR REPLACE INTO edges (parent_id, name, child_id) VALUES (?, ?, ?)`,
					curParentID, name, midID); err != nil {
					return err
				}
				curParentID = midID
			}
		}
		return nil
	})
	return nodeID, err
}

// Clear truncates all tables (keeps root node).
func (j *Journal) Clear() error {
	return j.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM whiteouts`); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM xattrs`); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM edges`); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM nodes WHERE id != 1`); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE nodes SET redirect_to = '/' WHERE id = 1`)
		return err
	})
}

// --- Compound Atomic Operations ---

// DeleteEdgeAndNode atomically removes an edge and its target node.
// CASCADE handles xattrs; the whiteout is added in the same tx.
func (j *Journal) DeleteEdgeAndNode(parentID int64, name string, nodeID int64, addWhiteout bool) error {
	return j.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM edges WHERE parent_id = ? AND name = ?`, parentID, name); err != nil {
			return err
		}
		if addWhiteout {
			if _, err := tx.Exec(`INSERT OR IGNORE INTO whiteouts (parent_id, name) VALUES (?, ?)`, parentID, name); err != nil {
				return err
			}
		}
		// Node deletion cascades to xattrs.
		if _, err := tx.Exec(`DELETE FROM nodes WHERE id = ?`, nodeID); err != nil {
			return err
		}
		return nil
	})
}

// CreateNodeEdgeAndWhiteout atomically creates a node, links it, and adds
// a whiteout for the pxar entry being shadowed.
func (j *Journal) CreateNodeEdgeAndWhiteout(parentID int64, name string, n *GraphNode, whiteout bool) (int64, error) {
	var id int64
	err := j.tx(func(tx *sql.Tx) error {
		var err error
		id, err = createNodeTx(tx, n)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO edges (parent_id, name, child_id) VALUES (?, ?, ?)`,
			parentID, name, id); err != nil {
			return err
		}
		if whiteout {
			if _, err := tx.Exec(`INSERT OR IGNORE INTO whiteouts (parent_id, name) VALUES (?, ?)`, parentID, name); err != nil {
				return err
			}
		}
		return nil
	})
	return id, err
}

// MoveEdgeAndWhiteout atomically moves an edge and adds whiteouts for both
// old and new pxar locations.
func (j *Journal) MoveEdgeAndWhiteout(oldParent int64, oldName string, newParent int64, newName string, replaceDestNode int64, whiteoutOld, whiteoutNew bool) error {
	return j.tx(func(tx *sql.Tx) error {
		// Remove destination whiteout.
		_, _ = tx.Exec(`DELETE FROM whiteouts WHERE parent_id = ? AND name = ?`, newParent, newName)

		// Handle destination collision.
		if replaceDestNode != 0 {
			if _, err := tx.Exec(`DELETE FROM edges WHERE parent_id = ? AND name = ?`, newParent, newName); err != nil {
				return err
			}
			if _, err := tx.Exec(`DELETE FROM nodes WHERE id = ?`, replaceDestNode); err != nil {
				return err
			}
		}

		// Move source edge.
		res, err := tx.Exec(`UPDATE edges SET parent_id = ?, name = ? WHERE parent_id = ? AND name = ?`,
			newParent, newName, oldParent, oldName)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return fmt.Errorf("move edge: source (%d, %q) not found", oldParent, oldName)
		}

		// Whiteout old location.
		if whiteoutOld {
			if _, err := tx.Exec(`INSERT OR IGNORE INTO whiteouts (parent_id, name) VALUES (?, ?)`, oldParent, oldName); err != nil {
				return err
			}
		}
		// Whiteout new location if it had a pxar entry.
		if whiteoutNew {
			if _, err := tx.Exec(`INSERT OR IGNORE INTO whiteouts (parent_id, name) VALUES (?, ?)`, newParent, newName); err != nil {
				return err
			}
		}
		return nil
	})
}
