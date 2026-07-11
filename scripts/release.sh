#!/bin/bash
#
# Deterministic release gate for the dataframe multi-package project.
#
# What it does (and deliberately does NOT do): it verifies that a chosen set of
# packages can be published as a coherent set, then stops. It never uploads. The
# final step prints the exact `cabal upload --publish` commands in dependency
# order for you to run.
#
# The decisive check is step 6: it resolves the *post-publish world* — the
# packages being released come from freshly built sdists, everything else comes
# from the real Hackage index. This is what catches the class of failure that
# took down dataframe-2.1.0.2 (a satellite's bound excluded a sibling that the
# meta required), which a plain `cabal build all` over the local tree cannot
# see, because locally every package resolves to its in-repo version.
#
# Usage:
#   scripts/release.sh                                   # report pending packages, then stop
#   scripts/release.sh dataframe-operations dataframe    # verify exactly this set
#   PROJECT=cabal.project scripts/release.sh <pkgs...>   # build/test against a different project
#   SKIP_BUILD=1 scripts/release.sh <pkgs...>            # skip the slow build+test
#
# Env:
#   PROJECT     cabal project file for build/test/sdist  (default: cabal.project.ci)
#   SKIP_BUILD  if set, skip `cabal build all` / `cabal test all`

set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
PROJECT="${PROJECT:-cabal.project.ci}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

note()  { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m  ok\033[0m %s\n' "$*"; }
die()   { printf '\033[1;31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

# Every publishable package: the meta (./dataframe.cabal) plus each satellite.
# Globbed so a new satellite is picked up automatically; the glob only matches
# top-level satellite cabals, not nested dist-newstyle vendored ones.
CABALS=( "$ROOT/dataframe.cabal" "$ROOT"/dataframe-*/dataframe-*.cabal )

# bash 3.2 (macOS) mishandles heredocs nested inside $(...), so the Python
# helpers are written to files up front and invoked by path.
cat > "$WORK/inventory.py" <<'PY'
# For each cabal path arg, print: name<TAB>ver<TAB>path<TAB>ahead<TAB>onhackage
import json, re, sys, urllib.request, urllib.error

def field(path, key):
    with open(path) as f:
        for line in f:
            m = re.match(rf'^\s*{key}\s*:\s*(\S+)', line, re.I)
            if m: return m.group(1).strip()
    return None

def published(name):
    url = f"https://hackage.haskell.org/package/{name}/preferred"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return set(json.load(r).get("normal-version", [])), True
    except urllib.error.HTTPError as e:
        return set(), (e.code != 404)
    except Exception:
        return set(), True  # network error: assume exists, don't mislabel as new

for path in sys.argv[1:]:
    name, ver = field(path, "name"), field(path, "version")
    if not name or not ver: continue
    vers, exists = published(name)
    print(f"{name}\t{ver}\t{path}\t{'yes' if ver not in vers else 'no'}\t{'yes' if exists else 'no'}")
PY

cat > "$WORK/bounds.py" <<'PY'
# Assert every inter-package dependency admits the local version of its sibling.
import re, sys

def pv(s): return tuple(int(x) for x in s.split('.'))
def caret_upper(v):
    t = list(v) + [0, 0]; return (t[0], t[1] + 1)

local = {}
for path in sys.argv[1:]:
    name = ver = None
    with open(path) as f:
        for line in f:
            m = re.match(r'^\s*name\s*:\s*(\S+)', line, re.I)
            if m: name = m.group(1)
            m = re.match(r'^\s*version\s*:\s*(\S+)', line, re.I)
            if m: ver = m.group(1)
    if name and ver: local[name] = ver

dep_re = re.compile(r'(dataframe[-a-z]*)(?::[a-z-]+)?\s*(\^>=|>=)\s*([0-9.]+)(?:\s*&&\s*<\s*([0-9.]+))?')
bad = []
for path in sys.argv[1:]:
    text = open(path).read()
    for m in dep_re.finditer(text):
        dep, op, lo, hi = m.groups()
        if dep not in local: continue
        v = pv(local[dep])
        if op == '^>=':
            lobound, hibound = pv(lo), caret_upper(pv(lo))
        else:
            lobound, hibound = pv(lo), (pv(hi) if hi else None)
        if v < lobound or (hibound and v >= hibound):
            bad.append(f"  {path.split('/')[-1]}: requires {dep} {m.group(0).split(dep,1)[1].strip()} "
                       f"but local {dep} is {local[dep]}")
if bad:
    print("Version-bound conflicts:"); print("\n".join(sorted(set(bad)))); sys.exit(1)
print("  all inter-package bounds admit the local sibling versions")
PY

cat > "$WORK/probedeps.py" <<'PY'
# Read RELEASE_SET (name<TAB>ver<TAB>path lines) on stdin; print probe deps.
import sys
lines = [l for l in sys.stdin.read().splitlines() if l.strip()]
names = [l.split("\t")[0] for l in lines]
vers = {l.split("\t")[0]: l.split("\t")[1] for l in lines}
if "dataframe" in names:
    print(f"dataframe == {vers['dataframe']}")
else:
    print(", ".join(f"{n} == {vers[n]}" for n in names))
PY

cat > "$WORK/topo.py" <<'PY'
# argv[1] = RELEASE_SET text; argv[2:] = all cabal paths. Print upload order.
import re, sys
rel = {}
for line in sys.argv[1].strip().splitlines():
    n, v, p = line.split("\t"); rel[n] = (v, p)
dep_re = re.compile(r'(dataframe[-a-z]*)(?::[a-z-]+)?\s*\^?>=')
deps = {n: set() for n in rel}
for n, (v, p) in rel.items():
    for m in dep_re.finditer(open(p).read()):
        d = m.group(1)
        if d in rel and d != n: deps[n].add(d)
order, seen = [], set()
def visit(n):
    if n in seen: return
    seen.add(n)
    for d in sorted(deps[n]): visit(d)
    order.append(n)
for n in sorted(rel): visit(n)
print()
for n in order:
    print(f"    cabal upload --publish dist-newstyle/sdist/{n}-{rel[n][0]}.tar.gz")
PY

# ---------------------------------------------------------------------------
# 1. Determine the release set.
#    No args  -> print pending packages (local version ahead of Hackage), stop.
#    Args     -> release exactly those packages.
# ---------------------------------------------------------------------------
note "1. Determining release set"
INVENTORY="$(python3 "$WORK/inventory.py" "${CABALS[@]}")"

if [ "$#" -eq 0 ]; then
    printf '\nNo release set given. Pending packages (local version ahead of Hackage):\n\n'
    echo "$INVENTORY" | awk -F'\t' '$4=="yes" && $5=="yes"{printf "    %-26s %s   (update to published package)\n",$1,$2}'
    echo "$INVENTORY" | awk -F'\t' '$4=="yes" && $5=="no" {printf "    %-26s %s   (NEVER published — first upload)\n",$1,$2}'
    printf '\nRe-run with the explicit set you intend to release, e.g.:\n'
    printf '    scripts/release.sh dataframe-operations dataframe-parquet dataframe-th dataframe\n\n'
    exit 0
fi

RELEASE_SET=""
for want in "$@"; do
    line="$(echo "$INVENTORY" | awk -F'\t' -v n="$want" '$1==n{print $1"\t"$2"\t"$3}')"
    [ -n "$line" ] || die "unknown package '$want' (not a local dataframe package)"
    RELEASE_SET="${RELEASE_SET}${line}
"
done
RELEASE_SET="$(printf '%s' "$RELEASE_SET" | sed '/^[[:space:]]*$/d')"
echo "$RELEASE_SET" | while IFS=$'\t' read -r n v _; do printf '    release: %-26s %s\n' "$n" "$v"; done

# ---------------------------------------------------------------------------
# 2. Bound invariants (fast, runs before anything slow).
# ---------------------------------------------------------------------------
note "2. Checking inter-package version-bound consistency"
python3 "$WORK/bounds.py" "${CABALS[@]}" || die "bound invariants violated (see above)"
ok "bound invariants hold"

# ---------------------------------------------------------------------------
# 3. Clean build + full test suite (proves the new local set compiles & is green).
# ---------------------------------------------------------------------------
if [ -n "${SKIP_BUILD:-}" ]; then
    note "3. Build + test  (SKIPPED via SKIP_BUILD)"
else
    note "3. Clean build + test  (--project-file=$PROJECT)"
    cabal --project-file="$PROJECT" build all 2>&1 | tail -3 || die "build failed"
    cabal --project-file="$PROJECT" test  all 2>&1 | tail -8 || die "tests failed"
    ok "build + tests green"
fi

# ---------------------------------------------------------------------------
# 4. cabal check + 5. sdist for each release-set package.
# ---------------------------------------------------------------------------
note "4/5. cabal check + sdist for each release-set package"
SDIST_DIR="$WORK/sdist"
mkdir -p "$SDIST_DIR"
# One `sdist all` avoids the meta's lib/exe target ambiguity; then assert each
# release-set tarball is present (missing => PROJECT doesn't list that package).
cabal sdist all --project-file="$PROJECT" --output-directory "$SDIST_DIR" >/dev/null 2>&1 \
    || die "cabal sdist all failed"
while IFS=$'\t' read -r name ver path; do
    [ -n "$name" ] || continue
    ( cd "$(dirname "$path")" && cabal check ) || die "cabal check failed for $name"
    [ -f "$SDIST_DIR/${name}-${ver}.tar.gz" ] \
        || die "sdist ${name}-${ver}.tar.gz not produced — is $name listed in $PROJECT?"
    ok "checked + sdist'd $name-$ver"
done <<<"$RELEASE_SET"

# ---------------------------------------------------------------------------
# 6. Post-publish resolution simulation. THE gate that catches the 2.1.0.2
#    failure: release-set packages come from the fresh sdists, everything else
#    resolves from the live Hackage index.
# ---------------------------------------------------------------------------
note "6. Post-publish resolution simulation (release set local, rest from Hackage)"
SIM="$WORK/sim"
mkdir -p "$SIM/pkgs"
PKG_LINES=""
while IFS=$'\t' read -r name ver _; do
    [ -n "$name" ] || continue
    tar -xzf "$SDIST_DIR/${name}-${ver}.tar.gz" -C "$SIM/pkgs"
    PKG_LINES="${PKG_LINES}    pkgs/${name}-${ver}/
"
done <<<"$RELEASE_SET"

PROBE_DEPS="$(printf '%s\n' "$RELEASE_SET" | python3 "$WORK/probedeps.py")"
cat > "$SIM/probe.cabal" <<EOF
cabal-version: 3.0
name: probe
version: 0
build-type: Simple
library
  default-language: Haskell2010
  build-depends: base, $PROBE_DEPS
EOF
cat > "$SIM/cabal.project" <<EOF
packages: .
$PKG_LINES
EOF

( cd "$SIM" && cabal build --dry-run probe 2>&1 ) | tee "$SIM/plan.txt" | grep -E "dataframe|Error|Could not" || true
if grep -qiE "could not resolve|Error:" "$SIM/plan.txt"; then
    die "post-publish resolution FAILED — publishing this set would break 'cabal install'"
fi
ok "post-publish world resolves cleanly"

# ---------------------------------------------------------------------------
# 7. Topological upload order + commands. STOP here (no upload).
# ---------------------------------------------------------------------------
note "7. Upload order (dependencies first) — run these yourself, then tag the release"
python3 "$WORK/topo.py" "$RELEASE_SET" "${CABALS[@]}"
printf '\n    (sdists are regenerated by `cabal sdist all`; the paths above are under dist-newstyle/sdist/)\n'

note "Verification complete. Nothing was uploaded."
