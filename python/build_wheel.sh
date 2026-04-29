#!/usr/bin/env bash
# Build a platform-specific hyrax wheel.
#
# Steps:
#   1. cabal build dataframe-arrow      (produces libdataframe-arrow.{dylib,so})
#   2. copy the lib next to python/hyrax/__init__.py
#   3. python -m build --wheel          (produces python/dist/hyrax-*.whl)
#
# Run from the repo root or from python/. The resulting wheel goes into
# python/dist/.

set -euo pipefail

# Pin the macOS deployment target so the wheel tag advertises a sensible
# minimum (e.g. macosx_11_0_arm64) rather than the build host's actual
# version. Has no effect on Linux.
export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-11.0}"

# Resolve repo root (one level up from this script).
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"

cd "$REPO_ROOT"

cabal build dataframe-arrow

# Find the freshest built shared library.
LIB=$(find dist-newstyle -type f \( -name 'libdataframe-arrow.dylib' -o -name 'libdataframe-arrow.so' \) -print0 \
        | xargs -0 ls -t | head -n 1)

if [[ -z "$LIB" ]]; then
    echo "ERROR: could not locate libdataframe-arrow shared library after build" >&2
    exit 1
fi

cp "$LIB" "$REPO_ROOT/python/hyrax/"

cd "$REPO_ROOT/python"
rm -rf build/ dist/ *.egg-info
python -m build --wheel --no-isolation

# Retag to a sensible minimum macOS version. Python's interpreter on Darwin
# advertises whatever SDK it was built against (often very recent), but our
# bundled dylib only needs macOS 11+. Skipped on Linux.
case "$(uname)" in
    Darwin)
        ARCH=$(uname -m)
        TARGET_TAG="macosx_11_0_${ARCH}"
        for whl in dist/*.whl; do
            python -m wheel tags --platform-tag "$TARGET_TAG" --remove "$whl" || true
        done

        # Bundle every transitive dylib dependency (the GHC runtime + every
        # Haskell package the foreign library links to) into the wheel and
        # rewrite the install names to @loader_path/.dylibs/... so the wheel
        # is self-contained on a machine without GHC installed.
        echo
        mkdir -p dist/wheelhouse
        for whl in dist/*.whl; do
            delocate-wheel --require-archs "$ARCH" -w dist/wheelhouse/ -v "$whl"
        done
        # Replace the originals with the self-contained wheels.
        mv dist/wheelhouse/*.whl dist/
        rmdir dist/wheelhouse
        ;;
    Linux)
        echo
        mkdir -p dist/wheelhouse
        for whl in dist/*.whl; do
            # NOTE: PyPI requires a manylinux-compatible plat tag. If you
            # built on glibc that's newer than manylinux2014's baseline,
            # auditwheel will complain — run this inside a manylinux2014
            # container for PyPI uploads.
            auditwheel repair --plat manylinux2014_"$(uname -m)" -w dist/wheelhouse/ "$whl" \
                || auditwheel repair -w dist/wheelhouse/ "$whl"
        done
        mv dist/wheelhouse/*.whl dist/
        rmdir dist/wheelhouse
        ;;
esac

echo
echo "Built wheels:"
ls -la dist/*.whl
