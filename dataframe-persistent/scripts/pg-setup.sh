#!/usr/bin/env bash
# Ephemeral PostgreSQL for the runnable README (docs/base_scripts/base_readme.md).
# Spins up a throwaway cluster, loads the artists/albums tables from the SQLite
# fixture, and is idempotent. Tear down with scripts/pg-teardown.sh.
# Run from the dataframe-persistent/ directory (paths are relative to it).
#
# Requires postgres client/server binaries on PATH (e.g. `brew install postgresql@16`
# then add /opt/homebrew/opt/postgresql@16/bin to PATH) and sqlite3.
set -euo pipefail

PGDATA="${PGDATA:-/tmp/df-persistent-pg}"
PGPORT="${PGPORT:-54329}"

pg_ctl -D "$PGDATA" stop >/dev/null 2>&1 || true
rm -rf "$PGDATA"
initdb -D "$PGDATA" -U postgres --locale=C -E UTF-8 >/dev/null 2>&1
pg_ctl -D "$PGDATA" -o "-p $PGPORT" -l /tmp/df-persistent-pg.log -w start >/dev/null 2>&1
createdb -p "$PGPORT" -U postgres chinook

sqlite3 data/chinook.db -csv "SELECT ArtistId,Name FROM artists"        > /tmp/df-artists.csv
sqlite3 data/chinook.db -csv "SELECT AlbumId,Title,ArtistId FROM albums" > /tmp/df-albums.csv

psql -p "$PGPORT" -U postgres chinook -q -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE TABLE artists ("ArtistId" int PRIMARY KEY, "Name" text);
CREATE TABLE albums  ("AlbumId" int PRIMARY KEY, "Title" text NOT NULL, "ArtistId" int NOT NULL);
\copy artists ("ArtistId","Name")            FROM '/tmp/df-artists.csv' CSV
\copy albums  ("AlbumId","Title","ArtistId") FROM '/tmp/df-albums.csv'  CSV
SQL

echo "postgres ready on port $PGPORT (db chinook: artists + albums)"
