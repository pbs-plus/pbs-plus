# Mount profile directory layouts

Mount Profiles supports an optional custom directory layout for read-only local mounts, NFS outposts and S3 snapshot-export outposts. All three use the same planner. Existing profiles without a layout retain their current directory names and newest-per-group selection.

In the profile editor, enable **Custom directory layout**, configure the fields and click **Preview paths** before saving. Preview uses the real snapshots and resolver but does not mount or detach anything. It displays the first 100 generated paths. These settings do not change backup names or data stored in PBS. They do not apply to writable S3 backup repositories or Samba exports.

## Templates and selection

Paths are relative to the local mount root or the outpost's shared export. For S3, the shared export is a bucket and the generated directories are object-key prefixes within it.

| Token       | Meaning                                                  |
| ----------- | -------------------------------------------------------- |
| `{ns}`      | Full namespace path                                      |
| `{ns.rel}`  | Namespace relative to the profile's parent               |
| `{ns.leaf}` | Last namespace component                                 |
| `{type}`    | PBS backup type                                          |
| `{id}`      | PBS backup ID                                            |
| `{archive}` | Selected archive's base name without its index extension |
| `{time}`    | Snapshot timestamp with colons replaced by hyphens       |
| `{resolve}` | A single folder name returned by the HTTP resolver       |

For example, `{ns.rel}/{type}-{id}` preserves a separate directory for each backup group. `{resolve}/{ns.leaf}` groups namespaces into externally supplied folders.

Selection defaults to **Newest per backup group**. **Newest across groups per namespace** selects the newest mountable snapshot across backup IDs and types within each namespace, based on PBS snapshot time, not file modification time. A profile still mounts the selected snapshot's first mountable archive, like the existing batch implementation; it does not merge separate archives.

Generated paths must be clean relative paths. Absolute paths, traversal, hidden components and overlapping paths are rejected before reconciliation changes any mounts. **Windows-safe names** also substitutes reserved characters in directory names and prefixes reserved device names. Case-insensitive collisions are rejected in this mode. Naming collisions are errors, not implicit merges or overwrites.

## HTTP resolver contract

The optional resolver is an administrator-configured HTTP(S) endpoint. It is called by the PBS server during preview and reconciliation, never during individual file reads or S3 object requests. Private-network endpoints are supported; only trusted administrators should configure them.

Requests use `POST` with `Content-Type: application/json`:

```json
{
  "datastore": "archive",
  "namespace": "Media/D-100",
  "group": {
    "namespace": "Media/D-100",
    "backup_type": "host",
    "backup_id": "scanner-a"
  },
  "backup_time": "2026-01-01T00:00:00Z",
  "archive": "root.mpxar.didx"
}
```

Return HTTP 200 and JSON:

```json
{ "values": ["Project A", "Project B"] }
```

Each value is one folder-name token, not an arbitrary path. With `{resolve}/{ns.leaf}`, this exposes the same snapshot under `Project A/D-100` and `Project B/D-100`. Outposts share one snapshot filesystem across all aliases. Local aliases are relative symlinks to one mounted source.

Return `{"values": []}` when no grouping is known. This uses the fallback template, defaulting to `Uncategorized/{ns.leaf}`. Fallback cannot reference `{resolve}`. Timeouts, failed requests, redirects, invalid responses and path collisions are errors: existing mounts are not replaced using a fallback on these failures. Mount-now reports planning failures in its task log.

An optional **Bearer token variable** names an environment variable in the PBS server process. The token itself is never saved in the profile or returned by the API. Authenticated endpoints must use HTTPS. Redirects are not followed.

Resolver calls are limited to five seconds each, 64 KiB per response and 64 values per snapshot. Planning is limited to two minutes, 32768 output paths and 8 MiB of generated path text. Results are held only for the current plan and refreshed with the profile's normal schedule; there is no unbounded cache. If this is too slow for a large profile, make the resolver serve cached metadata.

## Profile API

Create and update accept a `layout` form field containing JSON:

```json
{
  "template": "{resolve}/{ns.leaf}",
  "fallback": "Uncategorized/{ns.leaf}",
  "selection": "namespace",
  "resolver_url": "https://metadata.example/paths",
  "resolver_token_env": "MOUNT_LAYOUT_TOKEN",
  "windows_names": true
}
```

Omitting `layout` on update preserves the existing configuration. Sending an empty value clears it. `POST /api2/extjs/config/d2d-mount-profiles/preview` accepts the same profile form fields and returns the planned snapshot metadata and paths without persisting a profile.

Changing a layout schedules stale read-only mounts for removal first; replacement mounts are queued on a subsequent reconciliation pass to avoid path collisions while unmount tasks are still running.
