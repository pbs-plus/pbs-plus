#!/usr/bin/env bash
set -u
fail=0
err() { echo "CONVENTION VIOLATION: $*" >&2; fail=1; }

while IFS= read -r d; do
  name=$(basename "$d")
  case "$name" in
    types|store|common|shared|util|utils|helpers|misc|base|core|lib|data)
      err "banned package name: $d" ;;
  esac
done < <(find internal -type d 2>/dev/null)

for f in $(find internal -name '*.go' -not -name '*_test.go'); do
  pkg=$(grep -m1 '^package ' "$f" | awk '{print $2}')
  dir=$(basename "$(dirname "$f")")
  [ "$pkg" = "$dir" ] || err "$f: package '$pkg' != dir '$dir'"
done

bad=$(find internal -name '*.go' | grep -EI '/(types|helpers|shared|common|misc|utils?|util)([_a-z]*)?\.go$' | grep -Ev '/(errors|doc)\.go$')
[ -z "$bad" ] || err "kind-named files: $bad"

aliases=$(grep -rhoE '[a-z][a-zA-Z0-9_]+ "github\.com/pbs-plus/pbs-plus/[a-z0-9/_-]+"' --include='*.go' internal cmd |
  awk '{a=$1; p=$2; gsub(/"/,"",p); n=split(p,x,"/"); if (a != x[n]) print a" <- "p}')
[ -z "$aliases" ] || err "internal import aliases: $aliases"

bad=$(ls cmd | grep -E '_|[A-Z]')
[ -z "$bad" ] || err "cmd dirs must be kebab-case: $bad"

gen=$(grep -E 'package: ".*(query)"' sqlc.yaml | grep -vE '(corequery|jobquery|mtfquery)')
[ -z "$gen" ] || err "sqlc package naming drift in sqlc.yaml"

gofmt -l internal cmd | grep . && err "unformatted files (gofmt -l)"

exit $fail
