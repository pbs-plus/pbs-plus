package pxarmount

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

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

// Journal is the SQLite-backed inode graph for the mutable overlay.
//
// Schema:
//
//	nodes(id, kind, mode, uid, gid, size, mtime_ns, ctime_ns, has_data,
//	      symlink_tgt, redirect_to, opaque)
//	edges(parent_id, name, child_id)  PK(parent_id, name)
//	xattrs(node_id, name, value)      PK(node_id, name)
//	whiteouts(parent_id, name)        PK(parent_id, name)
//
// Root node (id=1) always exists with redirect_to='/'.
// Renames are O(1): just update the edge row. No descendants touched.
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

	if _, err := db.Exec(`
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
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return &Journal{db: db}, nil
}

func (j *Journal) Close() error {
	return j.db.Close()
}

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

// ---------------------------------------------------------------------------
// Node CRUD
// ---------------------------------------------------------------------------

// GetNode returns the node by ID, or nil if not found.
func (j *Journal) GetNode(id int64) (*GraphNode, error) {
	row := j.db.QueryRow(`
		SELECT kind, mode, uid, gid, size, mtime_ns, ctime_ns, has_data,
		       symlink_tgt, redirect_to, opaque
		FROM nodes WHERE id = ?`, id)
	return scanNode(row, id)
}

func getNodeTx(tx *sql.Tx, id int64) (*GraphNode, error) {
	row := tx.QueryRow(`
		SELECT kind, mode, uid, gid, size, mtime_ns, ctime_ns, has_data,
		       symlink_tgt, redirect_to, opaque
		FROM nodes WHERE id = ?`, id)
	return scanNode(row, id)
}

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

// CreateNodeTx inserts a new node within a transaction and returns its ID.
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

// CreateNode inserts a new node and returns its ID.
func (j *Journal) CreateNode(n *GraphNode) (int64, error) {
	var id int64
	err := j.tx(func(tx *sql.Tx) error {
		var err error
		id, err = createNodeTx(tx, n)
		return err
	})
	return id, err
}

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

// DeleteNode deletes a node. CASCADE removes its edges, xattrs.
func (j *Journal) DeleteNode(id int64) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM nodes WHERE id = ?`, id)
		return err
	})
}

// SetHasData marks that a node now has data in the mutable dir.
func (j *Journal) SetHasData(nodeID int64) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE nodes SET has_data = 1 WHERE id = ?`, nodeID)
		return err
	})
}

// ---------------------------------------------------------------------------
// Edge CRUD
// ---------------------------------------------------------------------------

// LookupEdge returns the child node ID for (parent, name), or 0 if not found.
func (j *Journal) LookupEdge(parentID int64, name string) (int64, error) {
	var childID int64
	err := j.db.QueryRow(
		`SELECT child_id FROM edges WHERE parent_id = ? AND name = ?`,
		parentID, name).Scan(&childID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return childID, err
}

// LookupEdgeNode returns the child node for (parent, name), or nil.
func (j *Journal) LookupEdgeNode(parentID int64, name string) (*GraphNode, error) {
	row := j.db.QueryRow(`
		SELECT n.id, n.kind, n.mode, n.uid, n.gid, n.size,
		       n.mtime_ns, n.ctime_ns, n.has_data,
		       n.symlink_tgt, n.redirect_to, n.opaque
		FROM edges e JOIN nodes n ON e.child_id = n.id
		WHERE e.parent_id = ? AND e.name = ?`, parentID, name)

	n := &GraphNode{}
	var kind, mode, uid, gid, hasData, opaque sql.NullInt64
	var size, mtimeNs, ctimeNs sql.NullInt64
	var symlinkTgt, redirectTo sql.NullString

	err := row.Scan(&n.ID, &kind, &mode, &uid, &gid, &size,
		&mtimeNs, &ctimeNs, &hasData, &symlinkTgt, &redirectTo, &opaque)
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

// CreateEdge creates a parent→child binding. OR REPLACE handles re-creation.
func (j *Journal) CreateEdge(parentID int64, name string, childID int64) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO edges (parent_id, name, child_id) VALUES (?, ?, ?)`,
			parentID, name, childID)
		return err
	})
}

// DeleteEdge removes a parent→child binding.
func (j *Journal) DeleteEdge(parentID int64, name string) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM edges WHERE parent_id = ? AND name = ?`, parentID, name)
		return err
	})
}

// MoveEdge renames/moves an edge. This is the O(1) rename operation.
func (j *Journal) MoveEdge(oldParent int64, oldName string, newParent int64, newName string) error {
	return j.tx(func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE edges SET parent_id = ?, name = ? WHERE parent_id = ? AND name = ?`,
			newParent, newName, oldParent, oldName)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return fmt.Errorf("move edge: source edge (%d, %q) not found", oldParent, oldName)
		}
		return nil
	})
}

// ListEdges returns all edges under a parent.
func (j *Journal) ListEdges(parentID int64) ([]GraphEdge, error) {
	rows, err := j.db.Query(
		`SELECT name, child_id FROM edges WHERE parent_id = ? ORDER BY name`, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

// ---------------------------------------------------------------------------
// Whiteout CRUD
// ---------------------------------------------------------------------------

func (j *Journal) AddWhiteout(parentID int64, name string) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR IGNORE INTO whiteouts (parent_id, name) VALUES (?, ?)`, parentID, name)
		return err
	})
}

func (j *Journal) RemoveWhiteout(parentID int64, name string) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM whiteouts WHERE parent_id = ? AND name = ?`, parentID, name)
		return err
	})
}

func (j *Journal) IsWhiteout(parentID int64, name string) (bool, error) {
	var count int
	err := j.db.QueryRow(
		`SELECT 1 FROM whiteouts WHERE parent_id = ? AND name = ?`, parentID, name).Scan(&count)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (j *Journal) ListWhiteouts(parentID int64) ([]string, error) {
	rows, err := j.db.Query(`SELECT name FROM whiteouts WHERE parent_id = ?`, parentID)
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

// ---------------------------------------------------------------------------
// Xattr operations
// ---------------------------------------------------------------------------

func (j *Journal) GetXAttr(nodeID int64, name string) ([]byte, error) {
	var val []byte
	err := j.db.QueryRow(
		`SELECT value FROM xattrs WHERE node_id = ? AND name = ?`, nodeID, name).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return val, err
}

func (j *Journal) ListXAttrs(nodeID int64) ([]string, error) {
	rows, err := j.db.Query(`SELECT name FROM xattrs WHERE node_id = ?`, nodeID)
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

func (j *Journal) SetXAttr(nodeID int64, name string, value []byte) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO xattrs (node_id, name, value) VALUES (?, ?, ?)`,
			nodeID, name, value)
		return err
	})
}

func (j *Journal) RemoveXAttr(nodeID int64, name string) error {
	return j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM xattrs WHERE node_id = ? AND name = ?`, nodeID, name)
		return err
	})
}

// ---------------------------------------------------------------------------
// Path resolution: walk the edge graph component-by-component
// ---------------------------------------------------------------------------

// ResolvePath walks the edge graph from root to find a path.
// Returns:
//   - nodeID: the journal node ID at the final component (0 if not in journal)
//   - pxarPath: the pxar source path for the remaining/entire path
//   - fellOffAt: the node ID where we fell off the graph (for whiteout checks)
//   - remaining: the remaining path components after falling off
//
// If nodeID != 0, the path is fully in the journal.
// If nodeID == 0, the path is partially or fully in pxar.
func (j *Journal) ResolvePath(path string) (nodeID int64, pxarPath string, fellOffAt int64, remaining string, err error) {
	if path == "/" || path == "" {
		return 1, "/", 0, "", nil
	}

	parts := strings.Split(strings.Trim(path, "/"), "/")
	curID := int64(1)
	pxarPrefix := "/"

	for i, part := range parts {
		if part == "" {
			continue
		}

		// Check whiteout at this level
		isWO, err := j.IsWhiteout(curID, part)
		if err != nil {
			return 0, "", 0, "", err
		}
		if isWO {
			return 0, "", 0, "", nil // whiteout = ENOENT, pxarPath intentionally empty
		}

		// Look up edge
		var childID int64
		err = j.db.QueryRow(
			`SELECT child_id FROM edges WHERE parent_id = ? AND name = ?`,
			curID, part).Scan(&childID)
		if err == sql.ErrNoRows {
			// Fell off graph — remaining path is in pxar
			rem := strings.Join(parts[i:], "/")
			var pp string
			if pxarPrefix == "/" {
				pp = "/" + rem
			} else {
				pp = pxarPrefix + "/" + rem
			}
			return 0, pp, curID, rem, nil
		}
		if err != nil {
			return 0, "", 0, "", err
		}

		curID = childID

		// Update pxar prefix from this node's redirect_to
		var redirectTo sql.NullString
		if err := j.db.QueryRow(
			`SELECT redirect_to FROM nodes WHERE id = ?`, curID).Scan(&redirectTo); err != nil {
			return 0, "", 0, "", err
		}
		if redirectTo.Valid && redirectTo.String != "" {
			pxarPrefix = redirectTo.String
		} else {
			pxarPrefix = "/" + strings.Join(parts[:i+1], "/")
		}
	}

	return curID, pxarPrefix, 0, "", nil
}

// ReconstructPath walks edges backward from nodeID to reconstruct its full path.
func (j *Journal) ReconstructPath(nodeID int64) (string, error) {
	if nodeID == 1 {
		return "/", nil
	}
	var components []string
	curID := nodeID
	for curID != 1 {
		var parentID int64
		var name string
		err := j.db.QueryRow(
			`SELECT parent_id, name FROM edges WHERE child_id = ? LIMIT 1`, curID).Scan(&parentID, &name)
		if err != nil {
			return "", fmt.Errorf("reconstruct path for node %d: %w", nodeID, err)
		}
		components = append(components, name)
		curID = parentID
	}
	for i, k := 0, len(components)-1; i < k; i, k = i+1, k-1 {
		components[i], components[k] = components[k], components[i]
	}
	return "/" + strings.Join(components, "/"), nil
}

// ---------------------------------------------------------------------------
// Batch operations for commit
// ---------------------------------------------------------------------------

// AllNodes returns all nodes (except root) for commit walking.
func (j *Journal) AllNodes() ([]*GraphNode, error) {
	rows, err := j.db.Query(`
		SELECT id, kind, mode, uid, gid, size, mtime_ns, ctime_ns,
		       has_data, symlink_tgt, redirect_to, opaque
		FROM nodes WHERE id != 1 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []*GraphNode
	for rows.Next() {
		n := &GraphNode{}
		var kind, mode, uid, gid, hasData, opaque sql.NullInt64
		var size, mtimeNs, ctimeNs sql.NullInt64
		var symlinkTgt, redirectTo sql.NullString
		if err := rows.Scan(&n.ID, &kind, &mode, &uid, &gid, &size,
			&mtimeNs, &ctimeNs, &hasData, &symlinkTgt, &redirectTo, &opaque); err != nil {
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
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

// AllXAttrs returns all xattrs grouped by node ID.
func (j *Journal) AllXAttrs() (map[int64]map[string][]byte, error) {
	rows, err := j.db.Query(`SELECT node_id, name, value FROM xattrs ORDER BY node_id, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

// AllWhiteouts returns all whiteouts grouped by parent node ID.
func (j *Journal) AllWhiteouts() (map[int64][]string, error) {
	rows, err := j.db.Query(`SELECT parent_id, name FROM whiteouts ORDER BY parent_id, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[int64][]string)
	for rows.Next() {
		var parentID int64
		var name string
		if err := rows.Scan(&parentID, &name); err != nil {
			return nil, err
		}
		result[parentID] = append(result[parentID], name)
	}
	return result, rows.Err()
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
