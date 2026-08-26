package pxarmount

import (
	"os"
	"path/filepath"
	"sort"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func (fs *MutableFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	fs.debugf("Lookup: parent=%d name=%q", header.NodeId, name)
	if name == JournalDir {
		return fuse.ENOENT
	}

	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, name)

	re, status := fs.resolve(childPath)
	if status != fuse.OK {
		fs.debugf("Lookup: resolve(%q)=%s", childPath, status)
		return status
	}

	ino := fs.pathToIno(childPath, re.IsDir)
	fillResolvedEntryOut(ino, re, out)
	return fuse.OK
}

func (fs *MutableFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	path := fs.inodeToPath(input.NodeId)
	fs.debugf("GetAttr: ino=%d path=%q", input.NodeId, path)
	if path == "" && input.NodeId != RootInode {
		fs.debugf("GetAttr: ENOENT (no path for ino %d)", input.NodeId)
		return fuse.ENOENT
	}
	if path == "" {
		path = "/"
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		fs.debugf("GetAttr: resolve(%q) failed: %s", path, status)
		return status
	}

	// stale journal value (journal is only updated on Flush/Close).
	if re.DataIsMut && !re.IsDir {
		if st, err := os.Stat(fs.mutablePath(path)); err == nil {
			re.Size = uint64(st.Size())
			re.MtimeNs = st.ModTime().UnixNano()
			re.CtimeNs = re.MtimeNs
		}
	}

	fillResolvedAttrOut(re, out)
	fs.debugf("GetAttr: ok mode=0%o isDir=%v", out.Mode, re.IsDir)
	return fuse.OK
}

func (fs *MutableFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *MutableFS) ReleaseDir(input *fuse.ReleaseIn) {}

func (fs *MutableFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

func (fs *MutableFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fs.debugf("ReadDir: ino=%d offset=%d", input.NodeId, input.Offset)
	return fs.readDirImpl(input, out, false)
}

func (fs *MutableFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDirImpl(input, out, true)
}

func (fs *MutableFS) readDirImpl(input *fuse.ReadIn, out *fuse.DirEntryList, plus bool) fuse.Status {
	parentPath := fs.inodeToPath(input.NodeId)
	if parentPath == "" && input.NodeId != RootInode {
		return fuse.ENOENT
	}
	if parentPath == "" {
		parentPath = "/"
	}

	// Resolve the parent to find its journal node and pxar source.
	re, status := fs.resolve(parentPath)
	if status != fuse.OK && status != fuse.ENOENT {
		return status
	}

	var parentNodeID int64
	var pxarDirPath string
	isOpaque := false

	if re != nil {
		if re.Node != nil {
			parentNodeID = re.Node.ID
			isOpaque = re.Node.Opaque
			if re.Node.RedirectTo != "" {
				pxarDirPath = re.Node.RedirectTo
			} else {
				pxarDirPath = parentPath
			}
		} else if re.PxarNode != nil {
			pxarDirPath = parentPath
		}
	}
	if pxarDirPath == "" && parentPath == "/" {
		pxarDirPath = "/"
	}

	var pxarEntries []dirEntrySlim
	if !isOpaque && pxarDirPath != "" {
		pxarNode := fs.findPxarNode(pxarDirPath)
		if pxarNode != nil && pxarNode.isDir {
			var rerr error
			pxarEntries, rerr = fs.pxar.ReadDirRaw(pxarNode.inode)
			if rerr != nil {
				fs.debugf("ReadDir: pxar readdir %q err: %v", pxarDirPath, rerr)
			}
		}
	}

	edgeNames := make(map[string]int64)
	whiteoutNames := make(map[string]bool)
	if parentNodeID != 0 {
		var edges []GraphEdge
		var wos []string
		if e, err := fs.journal.ListEdges(parentNodeID); err != nil {
			fs.debugf("ReadDir: list edges %d err: %v", parentNodeID, err)
		} else {
			edges = e
			for _, e := range edges {
				edgeNames[e.Name] = e.ChildID
			}
		}
		if w, err := fs.journal.ListWhiteouts(parentNodeID); err != nil {
			fs.debugf("ReadDir: list whiteouts %d err: %v", parentNodeID, err)
		} else {
			wos = w
			for _, w := range wos {
				whiteoutNames[w] = true
			}
		}
	}

	type mergedEntry struct {
		name  string
		ino   uint64
		mode  uint32
		isDir bool
	}
	var merged []mergedEntry

	for _, pe := range pxarEntries {
		if whiteoutNames[pe.name] {
			continue
		}
		if _, ok := edgeNames[pe.name]; ok {
			continue
		}
		childPath := joinPath(parentPath, pe.name)
		ino := fs.pathToIno(childPath, pe.isDir)
		merged = append(merged, mergedEntry{
			name: pe.name, ino: ino, mode: pe.mode, isDir: pe.isDir,
		})
	}

	// Go map iteration is randomized; without sorting, a multi-call
	// readdir (small buffer) would see different entry order on each
	// call, causing duplicates and missing entries via offset resume.
	edgeNamesSorted := make([]string, 0, len(edgeNames))
	for name := range edgeNames {
		edgeNamesSorted = append(edgeNamesSorted, name)
	}
	sort.Strings(edgeNamesSorted)

	for _, name := range edgeNamesSorted {
		nodeID := edgeNames[name]
		// Edges take priority over whiteouts  -  if there's a journal node,
		// it's always visible.
		node, err := fs.journal.GetNode(nodeID)
		if err != nil {
			log.Error(err, "")
		}
		if node == nil {
			continue
		}
		childPath := joinPath(parentPath, name)
		isDir := node.Kind == NodeDir
		ino := fs.pathToIno(childPath, isDir)
		merged = append(merged, mergedEntry{
			name: name, ino: ino, mode: node.Mode, isDir: isDir,
		})
	}

	if input.Offset == 0 {
		dirMode := fs.dirModeForPath(parentPath)
		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: dirMode})
			if eo != nil {
				fs.fillEntryOutForPath(parentPath, eo)
			}
		} else {
			out.AddDirEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: dirMode})
		}
	}

	if input.Offset <= 1 {
		parentIno, parentMode := fs.getParentInfo(parentPath)
		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
			if eo != nil {
				pp := filepath.Dir(parentPath)
				if pp == "." {
					pp = "/"
				}
				fs.fillEntryOutForPath(pp, eo)
			}
		} else {
			out.AddDirEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
		}
	}

	start := max(int(input.Offset)-2, 0)
	for i := start; i < len(merged); i++ {
		de := fuse.DirEntry{Name: merged[i].name, Ino: merged[i].ino, Mode: merged[i].mode}
		if plus {
			eo := out.AddDirLookupEntry(de)
			if eo == nil {
				break
			}
			fs.fillEntryOutForPath(joinPath(parentPath, merged[i].name), eo)
		} else {
			if !out.AddDirEntry(de) {
				break
			}
		}
	}
	return fuse.OK
}
