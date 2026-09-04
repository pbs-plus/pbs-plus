# Outposts

Outposts are serving endpoints on the PBS server that expose mounted snapshots as network shares. Instead of mounting an archive at a local path on the server, you attach it to an outpost and clients reach it over NFS or SMB.

Outposts are managed on the **Snapshots** page, **Outposts** tab. Two outpost types exist:

| Type    | Serves             | Implementation                                     |
| ------- | ------------------ | -------------------------------------------------- |
| `nfs`   | NFSv3 (in-process) | Built into `pbs-plus`; only needs a listen address |
| `samba` | SMB                | System Samba (`smbd`); see prerequisites below     |

## Creating an Outpost

**Outposts** tab, **Add**. Names are lowercase alphanumerics and dashes, max 32 characters.

### NFS

| Field          | Description                                             |
| -------------- | ------------------------------------------------------- |
| Listen Address | `host:port` the NFSv3 server binds, e.g. `0.0.0.0:2049` |

The NFSv3 implementation serves with null authentication: any client that can reach the listen address can mount any share. Restrict access at the network layer (firewall, dedicated interface).

### Samba

Requires Samba installed and `smbd` running, with one `include` line added to `smb.conf` `[global]`:

```ini
include = /var/lib/pbs-plus/outposts/samba-<name>.conf
```

PBS Plus rewrites that file on every attach/detach and reloads `smbd` via `smbcontrol`.

| Field        | Description                                                                                                                    |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| Allow Guests | Anonymous access, no password. Mutually exclusive with Valid Users.                                                            |
| Valid Users  | Comma-separated users/groups allowed to connect. Either this or Allow Guests is required.                                      |
| Force User   | All file operations run as this account.                                                                                       |
| Hosts Allow  | Comma-separated IPs/prefixes allowed to connect.                                                                               |
| Browseable   | List share names when clients enumerate the server. Shares are hidden by default; hidden shares stay fully accessible by name. |

Domain principals (`DOMAIN\user`, `user@REALM`, `@DOMAIN\group`) require the PBS host to be joined to the domain first (`net ads join -U administrator`).

Outpost configurations persist as JSON under `/var/lib/pbs-plus/outposts/`.

## Attaching Snapshots

Mount a snapshot from a datastore tab (or via a Mount Profile) and select an **Outpost** instead of a **Mount Path**; the two are mutually exclusive. Optionally set a **Share Name**; otherwise one is generated from the snapshot descriptor plus a short key hash (e.g. `vm-100-2025-08-29t10-00-00z-a1b2c3d4`).

Share names accept letters, digits, `.`, `_`, `-` up to 46 characters; `global`, `homes`, and `printers` are reserved.

The session appears in **Active Mounts** with its outpost and client endpoint:

- NFS: `nfs://<listen-addr>/<share>`
- Samba: `smb://<hostname>/<share>`

### Read/Write Behavior

- **Read-only** shares serve the snapshot as-is. Samba read-only shares preserve backed-up ownership.
- **Read/write** shares use the same overlay journal as local mounts (inside the datastore at `.pbs-plus/mount-overlays/`); changes are committed back to PBS through the normal mount session workflow.
  - NFS: the archive is served directly from the in-process mount stack.
  - Samba: the archive is mounted privately via `pxar-mount` under `/var/run/pbs-plus-mounts/shares/<key>` and the path is shared through Samba. With **Force User** set, pxar ownership is mapped to that account's uid/gid (resolved through NSS/winbind) while source modes and ACL checks are retained.

Unmounting follows the Active Mounts rules: read/write sessions unmount while keeping uncommitted changes in the overlay (remount to restore them, or force-unmount to discard).

## Lifecycle

- Outposts start automatically on server boot; sessions with an outpost are reattached after restart (read/write Samba sessions are re-mounted from their persisted definitions).
- Editing an outpost restarts its endpoint if it was running.
- An outpost with attached shares cannot be deleted; detach (unmount) its sessions first.

## API

Outposts are exposed at `/api2/extjs/config/d2d-outposts` (list/create) and `/api2/extjs/config/d2d-outposts/{name}` (get/update/delete).
