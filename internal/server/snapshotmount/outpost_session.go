//go:build linux

package snapshotmount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/pxarmount"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
	"github.com/pbs-plus/pbs-plus/internal/server/systemd"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
)

// ShareName derives the outpost share name for a session: a sanitized
// snapshot descriptor plus a short key hash so it stays unique and within
// the NFS handle budget.
func ShareName(s Session) string {
	if s.ShareName != "" {
		return s.ShareName
	}
	raw := fmt.Sprintf("%s-%s-%s", s.BackupType, s.BackupID, s.BackupTime)
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	suffix := crypto.SHA256Hex([]byte(s.ServiceKey))[:8]
	name := strings.Trim(b.String(), "-")
	if max := outpost.MaxShareName - len(suffix) - 1; len(name) > max {
		name = name[:max]
	}
	return name + "-" + suffix
}

func ensureShareNameFree(outpostName, share, key string) error {
	sessions, err := ListSessions()
	if err != nil {
		return fmt.Errorf("listing sessions: %w", err)
	}
	for _, s := range sessions {
		if s.Outpost == outpostName && s.ServiceKey != key && strings.EqualFold(ShareName(s), share) {
			return fmt.Errorf("share name %q is already in use on outpost %q", share, outpostName)
		}
	}
	return nil
}

// attachOutpostSession builds the snapshot stack in-process and attaches it
// as a share of the session's outpost.
func attachOutpostSession(s Session, parsedTime time.Time) error {
	o, found, err := outpost.LoadOutpost(s.Outpost)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("outpost %q does not exist", s.Outpost)
	}
	_ = o

	dsInfo, err := cli.GetDatastoreInfo(s.Datastore)
	if err != nil {
		return err
	}
	storeRoot := dsInfo.Path
	if storeRoot == "" {
		return fmt.Errorf("invalid datastore configuration")
	}

	dirTime := DirTime(parsedTime)
	_, ppxarPath, _, err := proxmox.BuildPxarPaths(storeRoot, s.Namespace, s.BackupType, s.BackupID, dirTime, s.FileName)
	if err != nil {
		return err
	}

	if s.Mode == ModeRW {
		if err := os.MkdirAll(s.OverlayDir, 0o700); err != nil {
			return fmt.Errorf("create overlay dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(s.SocketPath), 0o755); err != nil {
			return fmt.Errorf("create socket dir: %w", err)
		}
	}

	reader, err := openSplitReader(storeRoot, s.FileName, dirTime, s.Namespace, s.BackupType, s.BackupID)
	if err != nil {
		return err
	}

	stack, err := pxarmount.BuildStack(pxarmount.MountConfig{
		PBSStore:      storeRoot,
		Reader:        reader,
		OrigPpxarDidx: ppxarPath,
		BackingDir:    s.OverlayDir,
		SocketPath:    s.SocketPath,
		Namespace:     s.Namespace,
	})
	if err != nil {
		return err
	}

	share := ShareName(s)
	if err := outpost.Attach(s.Outpost, outpost.Attachment{
		Name:     share,
		ReadOnly: s.Mode == ModeRO,
		FS:       pxarmount.NewNFSFilesystem(stack.Raw, s.Mode == ModeRO),
		Release:  stack.Close,
	}); err != nil {
		stack.Close()
		return err
	}
	return nil
}

// OutpostShareMountPath is the private FUSE mount backing a VFS outpost share; /run state, rebuilt on boot.
func OutpostShareMountPath(key string) string {
	return filepath.Join("/var/run/pbs-plus-mounts", "shares", key)
}

// mountOutpostSession attaches a snapshot to an outpost as a share instead
// of mounting it locally.
func mountOutpostSession(ctx context.Context, task *tasklog.WorkerTask, in jobs.SnapshotMountInput, parsedTime time.Time, key, mode string) (Session, error) {
	if in.MountPath != "" {
		return Session{}, fmt.Errorf("mount_path cannot be combined with an outpost")
	}
	if err := outpost.ValidateShareName(in.ShareName); err != nil {
		return Session{}, err
	}
	if in.ShareName != "" {
		if err := ensureShareNameFree(in.Outpost, in.ShareName, key); err != nil {
			return Session{}, err
		}
	}
	o, found, err := outpost.LoadOutpost(in.Outpost)
	if err != nil {
		return Session{}, err
	}
	if !found {
		return Session{}, fmt.Errorf("outpost %q does not exist", in.Outpost)
	}
	if o.Type == outpost.TypeSamba {
		return mountVFSOutpostSession(ctx, task, in, parsedTime, key, mode)
	}

	session := Session{
		Datastore:  in.Datastore,
		Namespace:  in.Namespace,
		BackupType: in.BackupType,
		BackupID:   in.BackupID,
		BackupTime: in.BackupTime,
		FileName:   in.FileName,
		Mode:       mode,
		Outpost:    in.Outpost,
		ShareName:  in.ShareName,
		ServiceKey: key,
		CreatedAt:  time.Now().Unix(),
	}
	if mode == ModeRW {
		dsInfo, err := cli.GetDatastoreInfo(in.Datastore)
		if err != nil {
			return Session{}, err
		}
		if dsInfo.Path == "" {
			return Session{}, fmt.Errorf("invalid datastore configuration")
		}
		session.OverlayDir = OverlayDir(dsInfo.Path, key)
		session.SocketPath = SocketPath(key)
	}

	if err := attachOutpostSession(session, parsedTime); err != nil {
		return Session{}, err
	}
	session.Endpoint = outpost.EndpointOf(in.Outpost, ShareName(session))
	if err := SaveSession(session); err != nil {
		outpost.Detach(in.Outpost, ShareName(session))
		return Session{}, fmt.Errorf("persist mount session: %w", err)
	}
	task.LogString(fmt.Sprintf("attached %s/%s as %s on outpost %s (%s)",
		in.BackupType, in.BackupID, ShareName(session), in.Outpost, session.Endpoint))
	return session, nil
}

func unmountOutpostSession(ctx context.Context, task *tasklog.WorkerTask, session Session, in jobs.SnapshotUnmountInput) error {
	share := ShareName(session)
	outpost.Detach(session.Outpost, share)
	if task != nil {
		task.LogString("detached " + share + " from outpost " + session.Outpost)
	}
	if session.MountPoint != "" {
		if err := systemd.StopMountService(ctx, session.ServiceName()); err != nil {
			log.Error(err, "")
		}
		if IsMounted(session.MountPoint) {
			if err := UnmountPath(session.MountPoint); err != nil {
				log.Error(err, "")
			}
		}
		if err := os.RemoveAll(session.MountPoint); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
	}
	if session.Mode == ModeRW && !in.Force {
		removeSessionSockets(session)
		task.LogString("uncommitted changes preserved in " + session.OverlayDir)
		task.LogString("remount read-write to restore them, or unmount with force to discard")
		return nil
	}
	cleanupSessionFiles(session)
	return nil
}

var lookupOutpostUserIDs = pxarmount.LookupUserIDs

func sambaOwnershipArgs(outpostName string) ([]string, error) {
	o, found, err := outpost.LoadOutpost(outpostName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("outpost %q does not exist", outpostName)
	}
	if o.Type != outpost.TypeSamba || o.ForceUser == "" {
		return nil, nil
	}
	uid, gid, err := lookupOutpostUserIDs(o.ForceUser)
	if err != nil {
		return nil, fmt.Errorf("resolve samba force user %q: %w", o.ForceUser, err)
	}
	return []string{
		"--acl-owner", strconv.FormatUint(uint64(uid), 10),
		"--acl-group", strconv.FormatUint(uint64(gid), 10),
		"--force-acl-owner", "--force-acl-group",
	}, nil
}

// mountVFSOutpostSession mounts the snapshot privately via pxar-mount and hands the path to a VFS outpost driver.
func mountVFSOutpostSession(ctx context.Context, task *tasklog.WorkerTask, in jobs.SnapshotMountInput, parsedTime time.Time, key, mode string) (Session, error) {
	var ownershipArgs []string
	if mode == ModeRW {
		var err error
		ownershipArgs, err = sambaOwnershipArgs(in.Outpost)
		if err != nil {
			return Session{}, err
		}
	}

	dsInfo, err := cli.GetDatastoreInfo(in.Datastore)
	if err != nil {
		return Session{}, err
	}
	pbsStoreRoot := dsInfo.Path
	if pbsStoreRoot == "" {
		return Session{}, errors.New("invalid datastore configuration")
	}

	mountPoint := OutpostShareMountPath(key)
	mpxarPath, ppxarPath, isMetadataSplit, err := proxmox.BuildPxarPaths(pbsStoreRoot, in.Namespace, in.BackupType, in.BackupID, DirTime(parsedTime), in.FileName)
	if err != nil {
		return Session{}, err
	}
	args := []string{"--pbs-store", pbsStoreRoot}
	if isMetadataSplit {
		args = append(args, "--mpxar-didx", mpxarPath, "--ppxar-didx", ppxarPath)
	} else {
		args = append(args, "--ppxar-didx", ppxarPath)
	}

	session := Session{
		Datastore:  in.Datastore,
		Namespace:  in.Namespace,
		BackupType: in.BackupType,
		BackupID:   in.BackupID,
		BackupTime: in.BackupTime,
		FileName:   in.FileName,
		Mode:       mode,
		Outpost:    in.Outpost,
		ShareName:  in.ShareName,
		MountPoint: mountPoint,
		ServiceKey: key,
		CreatedAt:  time.Now().Unix(),
	}
	if mode == ModeRW {
		args = append(args, ownershipArgs...)
		session.OverlayDir = OverlayDir(pbsStoreRoot, key)
		session.SocketPath = SocketPath(key)
		if err := os.MkdirAll(session.OverlayDir, 0o700); err != nil {
			return Session{}, fmt.Errorf("create overlay dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(session.SocketPath), 0o755); err != nil {
			return Session{}, fmt.Errorf("create socket dir: %w", err)
		}
		args = append(args,
			"--passthrough", session.OverlayDir,
			"--socket", session.SocketPath,
			"--options", "rw,allow_other,default_permissions",
		)
	}
	args = append(args, mountPoint)

	session, err = startSession(ctx, session, mountPoint, true, args)
	if err != nil {
		return Session{}, err
	}

	share := ShareName(session)
	if err := outpost.Attach(in.Outpost, outpost.Attachment{Name: share, ReadOnly: mode == ModeRO, Path: mountPoint}); err != nil {
		rollbackVFSMount(ctx, session)
		return Session{}, err
	}
	session.Endpoint = outpost.EndpointOf(in.Outpost, share)
	if err := SaveSession(session); err != nil {
		outpost.Detach(in.Outpost, share)
		rollbackVFSMount(ctx, session)
		return Session{}, fmt.Errorf("persist mount session: %w", err)
	}
	if task != nil {
		task.LogString(fmt.Sprintf("attached %s/%s as %s on outpost %s (%s)",
			in.BackupType, in.BackupID, share, in.Outpost, session.Endpoint))
	}
	return session, nil
}

func rollbackVFSMount(ctx context.Context, session Session) {
	if err := systemd.StopMountService(context.WithoutCancel(ctx), session.ServiceName()); err != nil {
		log.Error(err, "")
	}
	if IsMounted(session.MountPoint) {
		if err := UnmountPath(session.MountPoint); err != nil {
			log.Error(err, "")
		}
	}
	if err := os.RemoveAll(session.MountPoint); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}
	if err := DeleteSession(session.ServiceKey); err != nil {
		log.Error(err, "")
	}
}

func ReattachOutposts(ctx context.Context) {
	reattachOutposts(ctx, "")
}

func ReattachOutpost(ctx context.Context, name string) {
	reattachOutposts(ctx, name)
}

func reattachOutposts(ctx context.Context, only string) {
	sessions, err := ListSessions()
	if err != nil {
		log.Error(err, "reattach outposts: listing sessions")
		return
	}
	for _, s := range sessions {
		if s.Outpost == "" {
			continue
		}
		if only != "" && s.Outpost != only {
			continue
		}
		parsed, err := time.Parse(time.RFC3339, s.BackupTime)
		if err != nil {
			log.Error(fmt.Errorf("parse backup time %q: %w", s.BackupTime, err), "reattach outpost "+s.Outpost)
			continue
		}
		share := ShareName(s)
		if s.MountPoint != "" {
			if IsMounted(s.MountPoint) {
				if err := outpost.Attach(s.Outpost, outpost.Attachment{Name: share, ReadOnly: s.Mode == ModeRO, Path: s.MountPoint}); err != nil {
					log.Error(err, "reattaching outpost share "+share)
				}
				continue
			}
			in := jobs.SnapshotMountInput{
				Datastore:  s.Datastore,
				Namespace:  s.Namespace,
				BackupType: s.BackupType,
				BackupID:   s.BackupID,
				BackupTime: s.BackupTime,
				FileName:   s.FileName,
				Mode:       s.Mode,
				Outpost:    s.Outpost,
				ShareName:  s.ShareName,
			}
			if _, err := mountVFSOutpostSession(ctx, nil, in, parsed, s.ServiceKey, s.Mode); err != nil {
				log.Error(err, "reattaching outpost share "+share)
			}
			continue
		}
		if err := attachOutpostSession(s, parsed); err != nil {
			log.Error(err, "reattaching outpost share "+share)
		}
	}
}

func openSplitReader(storeRoot, fileName, dirTime, namespace, backupType, backupID string) (*transfer.SplitReader, error) {
	mpxarPath, ppxarPath, isSplit, err := proxmox.BuildPxarPaths(storeRoot, namespace, backupType, backupID, dirTime, fileName)
	if err != nil {
		return nil, err
	}
	store, err := datastore.NewChunkStore(storeRoot)
	if err != nil {
		return nil, fmt.Errorf("opening chunk store: %w", err)
	}
	source := datastore.NewChunkStoreSource(store)
	if isSplit {
		metaData, err := os.ReadFile(mpxarPath)
		if err != nil {
			return nil, fmt.Errorf("reading metadata index: %w", err)
		}
		payloadData, err := os.ReadFile(ppxarPath)
		if err != nil {
			return nil, fmt.Errorf("reading payload index: %w", err)
		}
		return transfer.NewSplitReader(metaData, payloadData, source)
	}
	idxData, err := os.ReadFile(ppxarPath)
	if err != nil {
		return nil, fmt.Errorf("reading index: %w", err)
	}
	return transfer.NewSplitReader(idxData, nil, source)
}
