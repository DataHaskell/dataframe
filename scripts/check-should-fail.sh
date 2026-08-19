#!/bin/bash

# Type errors that are part of the API contract: each file under
# type-errors/ must FAIL to compile, and fail with the listed message.
# `cabal test` cannot see these, so without this gate they regress silently.
#
# The message check is what makes the gate meaningful — a missing import also
# fails to compile, and would otherwise pass.

set -u

cd "$(dirname "$0")/.." || exit 1

# file:expected-substring
CASES=(
    "ImputePlain.hs:impute needs a nullable column"
)

status=0
for entry in "${CASES[@]}"; do
    file="type-errors/${entry%%:*}"
    expected="${entry#*:}"

    if out=$(cabal exec -v0 -- ghc -fno-code -package dataframe "$file" 2>&1); then
        echo "✗ $file compiled, but it must not"
        status=1
    elif ! grep -qF "$expected" <<<"$out"; then
        echo "✗ $file failed to compile, but not with the expected message"
        echo "    expected substring: $expected"
        sed 's/^/    /' <<<"$out" | head -20
        status=1
    else
        echo "✓ $file rejected with the expected message"
    fi
done

exit $status
