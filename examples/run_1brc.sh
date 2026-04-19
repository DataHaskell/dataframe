#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRCDIR="$SCRIPT_DIR/1brc"

if [ ! -d "$BRCDIR" ]; then
    git clone https://github.com/mchav/1brc "$BRCDIR"
fi

cd "$BRCDIR"

if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

source ./venv/bin/activate
pip install -r requirements.txt
python createMeasurements.py

cd "$SCRIPT_DIR"
time cabal run -O2 exampels -- one_billion_row_challenge
