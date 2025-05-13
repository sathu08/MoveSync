#!/bin/bash

set -euo pipefail

# === Ensure log directories exist ===
mkdir -p logs/dumps logs/restore dump

# === SOURCE: Azure PostgreSQL ===
SRC_DB=$1
SRC_USER=$2
SRC_PASS=$3
SRC_HOST=$4
SRC_PORT=$5

# === TARGET: Local PostgreSQL ===
DST_DB=$6
DST_USER=$7
DST_PASS=$8
DST_HOST=$9
DST_PORT=${10}

MOD=${11}
DUMP_FILE=${12:-}

# === Timestamp for logs and dump ===
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "🔹 [Step 1/5] Dumping from Azure PostgreSQL..."
if [ "$MOD" == "auto" ]; then
    DUMP_FILE="./dump/pg_dump_${TIMESTAMP}.dump"
    export PGPASSWORD="$SRC_PASS"

    pg_dump -h "$SRC_HOST" -p "$SRC_PORT" -U "$SRC_USER" -d "$SRC_DB" -F c \
        --no-owner --no-privileges --no-acl --verbose \
        -f "$DUMP_FILE" \
        > >(tee "logs/dumps/dump_${TIMESTAMP}_stdout.log") \
        2> >(tee "logs/dumps/dump_${TIMESTAMP}_stderr.log" >&2)

    echo "✅ Dump saved to $DUMP_FILE"
else
    if [ -z "$DUMP_FILE" ]; then
        echo "❌ DUMP_FILE must be provided when MOD is not 'auto'"
        exit 1
    fi
    echo "ℹ️ Using provided dump file: $DUMP_FILE"
fi

echo "🔹 [Step 2/5] Creating target DB if it doesn't exist..."
export PGPASSWORD="$DST_PASS"

# Uncomment this if you want auto-creation of the DB
# if ! psql -h "$DST_HOST" -p "$DST_PORT" -U "$DST_USER" -tAc "SELECT 1 FROM pg_database WHERE datname = '$DST_DB'" | grep -q 1; then
#     createdb -h "$DST_HOST" -p "$DST_PORT" -U "$DST_USER" "$DST_DB"
#     echo "✅ Target DB '$DST_DB' created."
# else
#     echo "ℹ️ Target DB '$DST_DB' already exists."
# fi

echo "🔹 [Step 3/5] Restoring to local PostgreSQL..."
pg_restore -h "$DST_HOST" -p "$DST_PORT" -U "$DST_USER" -d "$DST_DB" -F c -j 4 "$DUMP_FILE" --verbose --no-owner \
    > >(tee "logs/restore/restore_${TIMESTAMP}_stdout.log") \
    2> >(tee "logs/restore/restore_${TIMESTAMP}_stderr.log" >&2)

echo "✅ Restore completed successfully!"

echo "🔹 [Step 4/5] Verifying local DB tables..."
psql -h "$DST_HOST" -p "$DST_PORT" -U "$DST_USER" -d "$DST_DB" -c "\dt"

echo "🔹 [Step 5/5] Migration completed successfully!"
echo "🎉 Migration from Azure to Local PostgreSQL finished!"
