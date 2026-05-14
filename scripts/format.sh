#!/bin/bash

# Usage:
#   ./scripts/format.sh           # check (fail non-zero if any file needs reformatting)
#   ./scripts/format.sh --fix     # reformat in place

set -e

CHECK_MODE=true
if [[ "${1:-}" == "--fix" ]]; then
    CHECK_MODE=false
fi

if ! command -v fourmolu &> /dev/null; then
    echo -e "\nFourmolu not found. Installing...\n"
    cabal v2-install fourmolu-0.17.0.0 --overwrite-policy=always --force-reinstalls
    echo -e "\nFourmolu installed successfully!"
fi

# Files listed in .fourmolu-ignore contain CPP directives that
# fourmolu's ghc-lib-parser can't handle (Haddock mode parse errors).
# Filter them out *before* invoking fourmolu so we don't spam stderr.
# (Avoid mapfile so this works on macOS's bash 3.2.)
IGNORE=()
if [[ -f .fourmolu-ignore ]]; then
    while IFS= read -r line; do
        case "$line" in
            ''|\#*) ;;
            *) IGNORE+=("$line") ;;
        esac
    done < .fourmolu-ignore
fi

files=()
while IFS= read -r f; do
    skip=false
    for ign in "${IGNORE[@]}"; do
        [[ "$f" == "$ign" ]] && { skip=true; break; }
    done
    $skip || files+=("$f")
done < <(git ls-files '*.hs')

if [[ "$CHECK_MODE" == true ]]; then
    fourmolu --mode check "${files[@]}"
else
    fourmolu --mode inplace "${files[@]}"
fi
