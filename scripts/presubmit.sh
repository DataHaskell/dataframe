#!/bin/bash

set -e

# Run from repo root regardless of CWD.
cd "$(dirname "$0")/.."

# format.sh defaults to check mode (fails if anything is unformatted). Run
# ./scripts/format.sh --fix to apply fixes locally.
./scripts/format.sh
./scripts/lint.sh

# Default cabal.project lists every satellite (dataframe-core, dataframe-parsing,
# dataframe-operations, dataframe-csv, dataframe-json, dataframe-parquet,
# dataframe-th, dataframe-csv-th, dataframe-parquet-th, dataframe-viz,
# dataframe-learn, dataframe-lazy) plus the pre-existing dataframe-fastcsv,
# dataframe-arrow, dataframe-persistent, dataframe-hasktorch, dataframe-fusion
# and examples. Build + test everything in one shot.
cabal build all
cabal test all

# Build the meta-package library on its own with -Werror=unused-packages
# (set in dataframe.cabal). Catches the case where a satellite is added to
# build-depends but never re-exported, which haskell-ci would otherwise be
# the first to flag.
cabal build dataframe:lib:dataframe

# Exercise the meta-package's cabal flags so the CPP gates in DataFrame.hs,
# DataFrame.Typed.hs, DataFrame.TH.hs and DataFrame.Typed.TH.hs don't silently
# rot. Use cabal.project.bare (only the dataframe-* packages) so the solver
# isn't tripped by peer satellites whose `buildable:` clauses require CSV /
# Parquet / lazy to be available.

flag_combos=(
    "+no-th"
    "+no-csv"
    "+no-parquet"
    "+no-th +no-csv +no-parquet"
)

for combo in "${flag_combos[@]}"; do
    echo
    echo "=== building dataframe with flags: $combo ==="
    # shellcheck disable=SC2086 # word-splitting the combo is intentional
    cabal --project-file=cabal.project.bare \
          build dataframe:lib:dataframe \
          $(printf -- '-f %s ' $combo)
done
