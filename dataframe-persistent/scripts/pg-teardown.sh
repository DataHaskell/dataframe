#!/usr/bin/env bash
# Stop and remove the ephemeral PostgreSQL started by docs/pg-setup.sh.
PGDATA="${PGDATA:-/tmp/df-persistent-pg}"
pg_ctl -D "$PGDATA" stop >/dev/null 2>&1 || true
rm -rf "$PGDATA" /tmp/df-artists.csv /tmp/df-albums.csv
echo "postgres stopped and removed"
