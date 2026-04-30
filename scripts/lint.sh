#!/usr/bin/env bash
set -euo pipefail

# Run HLint on the project
# Usage:
#   ./scripts/lint.sh
#   ./scripts/lint.sh --fix

FIX_MODE=false
if [[ "${1:-}" == "--fix" ]]; then
  FIX_MODE=true
fi

DIRS=("src" "app" "tests" "benchmark" "examples" "dataframe-arrow" "dataframe-hasktorch" "dataframe-persistent" "dataframe-fastcsv" "dataframe-fusion" "ffi")

echo "Running HLint on dataframe project..."
echo "Dirs: ${DIRS[*]}"

is_haskell_file() {
  case "$1" in
    *.hs|*.lhs) return 0 ;;
    *) return 1 ;;
  esac
}

changed_files() {
  git diff --name-only --diff-filter=AMR --cached HEAD -- "${DIRS[@]}" || true
  git diff --name-only --diff-filter=AMR HEAD -- "${DIRS[@]}" || true
}

if [[ "$FIX_MODE" == true ]]; then
  echo "Running in fix mode (only edited files; per-file; continuing on failures)..."
  echo "Applying suggestions where possible..."
  echo

  mapfile -t files < <(changed_files | awk 'NF{print}' | awk '!seen[$0]++')

  if [[ "${#files[@]}" -eq 0 ]]; then
    echo "No edited files detected under: ${DIRS[*]}"
    echo "✓ Nothing to fix."
    exit 0
  fi

  failed=0
  total=0
  linted=0

  for file in "${files[@]}"; do
    if ! is_haskell_file "$file"; then
      continue
    fi
    if [[ ! -f "$file" ]]; then
      continue
    fi

    total=$((total + 1))
    echo "==> hlint --refactor -i: $file"

    if hlint "$file" --refactor --refactor-options="--inplace"; then
      linted=$((linted + 1))
    else
      failed=$((failed + 1))
      echo "!! hlint failed on: $file" >&2
      echo
    fi
  done

  echo
  echo "Fix-mode summary: candidates=${#files[@]} haskell_linted=$total failed=$failed"

  if [[ "$total" -eq 0 ]]; then
    echo "No edited Haskell files found to lint."
    exit 0
  fi

  if [[ "$failed" -eq 0 ]]; then
    echo "✓ HLint fix mode completed."
  else
    echo "✗ HLint fix mode had failures."
    exit 1
  fi

else
  hlint "${DIRS[@]}"
  echo "✓ HLint passed! No issues found."
fi
