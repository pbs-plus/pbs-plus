# Naming and structure conventions

These rules are mechanical. A name either satisfies them or it does not, and
CI checks the ones a linter can express. See `.golangci.yml`.

## Packages

- The package clause matches the directory name exactly. The only exception is
  `package main` under `cmd/`.
- Lowercase, singular, no underscores, no hyphens, no camelCase.
- A package name states the concept it owns. These names state nothing and are
  banned outright:

  `types`, `store`, `common`, `shared`, `util`, `utils`, `helper`, `helpers`,
  `misc`, `base`, `core`, `lib`, `data`

- **A package that needs an import alias is misnamed.** An alias at a call site
  means the package name did not survive contact with a consumer. Fix the
  package, do not add the alias. The one accepted alias is the blank import
  (`_ "modernc.org/sqlite"`).
- Two packages that would need aliases to coexist in one file are two packages
  with the same name. Rename one after the domain it serves: `coredb`, `jobdb`,
  `mtfdb`, not three packages called `store`.

## Directories

- `cmd/<entrypoint>` holds one `package main` each. The directory name is
  kebab-case and names the entry point. It matches the produced binary minus
  any distribution prefix, so `cmd/agent` may ship as `pbs-plus-agent`, but
  `cmd/pbs_plus` shipping `pbs-plus` is a violation.
- `internal/` holds every non-exported package. A directory containing
  subpackages holds no `.go` files of its own; it is a namespace, not a
  dumping ground.
- Generated code lives in its own leaf package named `<domain>query`, written
  by `sqlc generate` from `sqlc.yaml`. Never hand-edit it.

## Files

- A file is named for the domain concept it holds, never for the Go kind it
  holds. `target.go`, not `types.go`. `job_status.go`, not `structs.go`.
- Multi-word file names separate words with an underscore: `agent_host.go`,
  `mount_unit.go`, `encode_path.go`. This matches the existing tree and the
  shape `sqlc` generates.
- A domain's model and its queries live in one file, named for the domain in
  the singular: `backup.go` holds both `Backup` and the backup queries. A
  `backup.go`/`backups.go` pair is ambiguous by construction.
- Banned file names: `types.go`, `helpers.go`, `shared.go`, `common.go`,
  `misc.go`, `util.go`, `utils.go`, and any `_`-suffixed variant of them.
- Two file names are permitted to name a Go kind rather than a concept, because
  Go tooling and convention already give them meaning:
  - `errors.go` - the package's sentinel errors and error types.
  - `doc.go` - the package doc comment when it is too long to sit on a
    declaration.
- Platform variants use the build-constraint suffix Go already understands:
  `_linux.go`, `_windows.go`, `_unix.go`. Do not encode the platform in the
  concept name.
- A test file is named after the file it tests. Split a source file, split its
  test file the same way.

## Types

- A type name is not required to be unique across the repository. It is
  required to be unambiguous once qualified. `conf.Config` and `mtls.Config`
  are fine.
- Deliberate parallel shapes that were audited and KEEP their names:
  `coredb.Store`/`jobdb.Store`/`mtfdb.Store` (one per database, qualified by
  domain), `jobrpc.Service`/`mountrpc.Service`, `arpcfs.Node`/`s3fs.Node`,
  `arpcfs.FileHandle`/`s3fs.FileHandle`, `<pkg>.Config`, and every
  `<domain>` vs `<domain>query` model pair (sqlc row mirrors its domain
  model - the package suffix disambiguates).
- `mtls.TokenManager` was a pure delegation wrapper around
  `crypto.TokenManager` and was deleted rather than renamed. Wrappers that
  add nothing are the worst naming problem: two names for one thing.
- `changer.Changer` was renamed `changer.Device` (stutter).
- `snapshots.SnapshotManager` stays: the package exports a `Manager`
  singleton, so call sites say `snapshots.Manager` with no stutter.
- A type name IS wrong when the qualified name still fails to say which of two
  things it is, or when the same name models different things on both sides of
  the agent/server boundary and both get imported into one file.
- No stutter: `coredb.DB`, not `coredb.CoreDB`; `app.App` is accepted for a
  package's single root type.
- `gopls rename` is the only sanctioned mechanism for renaming an exported
  identifier. It refuses rather than produce a broken build. Hand-editing call
  sites does not have that property.
- After any rename of a struct field, grep struct tags and `text/template`
  references. `gopls` guards compilation, not reflection.

## Nesting

- Prefer a deep tree over a flat file list, but a directory is a package in
  Go: nest only where the boundary is real. Before splitting a package,
  measure (a) how many unexported identifiers would cross the boundary and be
  forced to export, and (b) whether most methods hang off one receiver type -
  methods on a type cannot live in another package.
- Audited outcome: `web/api` nests into per-domain packages (measured cost:
  two identifiers); `coredb` stays flat (94 of 108 methods are on `*Store`);
  `pxarmount`, `pxar`, `agentfs` stay flat (double-digit unexported
  crossings); `internal/` top level stays flat by consumer group (any umbrella
  name for the shared half would be `common` in disguise, and `internal/tape`
  would collide with `proxmox/tape`).
- Subpackage names end in their role when the bare domain is taken:
  `backupapi`, `targetapi` - never an import alias.

## Commits during a restructure

- A commit is structural or behavioral, never both.
- A move and an optimization touching the same code are two commits, in that
  order.
- Stage explicit paths. Never `git add -A` in a tree with unrelated work in it.
