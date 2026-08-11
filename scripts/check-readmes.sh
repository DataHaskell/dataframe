#!/bin/bash

# The scripths-backed READMEs are runnable notebooks built against the local
# working tree, but `cabal test` never compiles them. Regenerate each one and
# fail if the result differs from what is checked in — a code change that breaks
# a documented example, or an example never updated after a rename, shows up
# here. That is how a stale typed-schema line shipped in dataframe-learn.
#
# Every ```haskell block shares one session top-to-bottom, so the first broken
# block takes every later block with it.
#
# Each entry is `package:source:output`, both paths relative to the package
# directory, and scripths runs FROM that directory so the notebooks' own data
# paths (./data/chinook.db) resolve. dataframe-persistent renders its README
# from a separate source rather than in place.
#
# Requires scripths on PATH (cabal install scripths). Skips with a notice if it
# is absent so a contributor without it is not blocked.

set -u

cd "$(dirname "$0")/.." || exit 1

if ! command -v scripths >/dev/null 2>&1; then
    echo "note: scripths not on PATH; skipping README checks"
    exit 0
fi

NOTEBOOKS=(
    "dataframe-learn:README.md:README.md"
    "dataframe-viz:README.md:README.md"
    "dataframe-persistent:docs/base_scripts/base_readme.md:README.md"
)

status=0
for entry in "${NOTEBOOKS[@]}"; do
    IFS=: read -r pkg src out <<<"$entry"
    regen=$(mktemp)

    if ! (cd "$pkg" && scripths "$src" -o "$regen") >/dev/null 2>&1; then
        echo "✗ $pkg/$src failed to run"
        status=1
    elif ! diff -q "$pkg/$out" "$regen" >/dev/null; then
        echo "✗ $pkg/$out is stale — regenerate with:"
        echo "    (cd $pkg && scripths $src -o $out)"
        diff "$pkg/$out" "$regen" | head -30
        status=1
    else
        echo "✓ $pkg/$out runs clean and is up to date"
    fi
    rm -f "$regen"
done

exit $status
