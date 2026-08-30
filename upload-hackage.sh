#!/usr/bin/env bash
# Build sdists for every dataframe package and upload them to Hackage in
# topological (deps-first) order. Versions are read straight out of each
# package's .cabal file, so this never needs manual version-string edits.
#
# Auth is interactive: cabal will prompt for your Hackage username/password,
# or set a token in ~/.config/cabal/config.
#
# Usage:
#   ./upload-hackage.sh                     # sdist + upload ALL packages as CANDIDATES
#   ./upload-hackage.sh --publish            # sdist + PUBLISH all (irreversible on Hackage)
#   ./upload-hackage.sh --only core,csv,arrow  # restrict to a subset (still topo-ordered)
#   ./upload-hackage.sh --only core,csv --publish
set -euo pipefail

SDIST="dist-newstyle/sdist"

# Topological order (dependencies first). This is the one thing that isn't
# derived automatically: the dependency graph between packages changes
# rarely, and re-deriving it from build-depends on every release is more
# fragile than reviewing this list by hand when a new inter-package
# dependency is introduced.
#
# "dataframe" is the umbrella package (root dataframe.cabal). The Arrow
# bridge is its own package (dataframe-arrow-bridge) and must go out before
# dataframe-arrow and dataframe-fusion, which both depend on it.
ALL_PKGS=(
  dataframe-core
  dataframe-parsing
  dataframe-operations
  dataframe-csv
  dataframe-th
  dataframe-json
  dataframe-viz
  dataframe-parquet
  dataframe-csv-th
  dataframe-fastcsv
  dataframe-parquet-th
  dataframe-expr-serializer
  dataframe-hasktorch
  dataframe-lazy
  dataframe-arrow-bridge
  dataframe-learn
  dataframe-persistent
  dataframe-fusion
  dataframe-huggingface
  dataframe
  dataframe-arrow
)

# cabal target + .cabal file path for a package name.
cabal_target() {
  [[ "$1" == "dataframe" ]] && echo "." || echo "$1"
}
cabal_file() {
  [[ "$1" == "dataframe" ]] && echo "dataframe.cabal" || echo "$1/$1.cabal"
}
pkg_version() {
  grep -m1 -E '^version:' "$(cabal_file "$1")" | awk '{print $2}'
}

PUBLISH=""
ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --publish) PUBLISH="--publish" ;;
    --only) ONLY="$2"; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
  shift
done

PKGS=("${ALL_PKGS[@]}")
if [[ -n "$ONLY" ]]; then
  IFS=',' read -ra WANTED <<< "$ONLY"
  PKGS=()
  for p in "${ALL_PKGS[@]}"; do
    for w in "${WANTED[@]}"; do
      [[ "$p" == "$w" || "$p" == "dataframe-$w" ]] && PKGS+=("$p")
    done
  done
fi

echo ">>> building sdists for: ${PKGS[*]}"
TARGETS=()
for p in "${PKGS[@]}"; do
  TARGETS+=("$(cabal_target "$p")")
done
cabal sdist "${TARGETS[@]}"

echo ">>> uploading ${#PKGS[@]} packages in topological order ${PUBLISH:+(PUBLISH)}${PUBLISH:-(candidates)}"
for p in "${PKGS[@]}"; do
  v="$(pkg_version "$p")"
  tarball="$SDIST/$p-$v.tar.gz"
  echo ">>> uploading $p-$v $PUBLISH"
  cabal upload $PUBLISH "$tarball"
done
echo "Done. If these were candidates, verify at https://hackage.haskell.org/packages/candidates/ then re-run with --publish."
