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
  holds. `target.go`, not `types.go`. `backupstatus.go`, not `structs.go`.
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

## Commits during a restructure

- A commit is structural or behavioral, never both.
- A move and an optimization touching the same code are two commits, in that
  order.
- Stage explicit paths. Never `git add -A` in a tree with unrelated work in it.
