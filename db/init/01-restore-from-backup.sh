#!/bin/sh
# Runs automatically via the official postgres image's initdb hook -- but
# ONLY the first time the data directory is initialized (empty). This covers
# the case where the container is recreated and the data directory is lost
# or starts fresh: if a backup produced by the `backup` service is available,
# it's restored here instead of starting with an empty database.
#
# Named 01- so it runs AFTER 00-create-readonly-role.sh -- a pg_dump backup
# contains GRANT statements referencing the read-only role, but never a
# CREATE ROLE for it (roles are cluster-level, not database-level, so
# pg_dump never includes them). Restoring before that role exists fails on
# those trailing GRANTs -- confirmed by testing. The role script is a no-op
# if WEBAPP_DB_PASSWORD isn't set, so this still works fine without it.
#
# Requires the same host folder used by BACKUP_HOST_PATH to also be mounted
# into this (postgres) service, read-only, at /backups-restore -- see
# docker-compose.yml.

set -e

if [ "${POSTGRES_AUTO_RESTORE_ON_INIT:-true}" != "true" ]; then
    echo "POSTGRES_AUTO_RESTORE_ON_INIT is not 'true' -- starting with an empty database." >&2
    exit 0
fi

RESTORE_FILE="/backups-restore/last/${POSTGRES_DB}-latest.sql.gz"

if [ ! -f "$RESTORE_FILE" ]; then
    echo "No backup found at $RESTORE_FILE -- starting with an empty database." >&2
    exit 0
fi

DECOMPRESS=""
if command -v gunzip >/dev/null 2>&1; then
    DECOMPRESS="gunzip -c"
elif command -v zcat >/dev/null 2>&1; then
    DECOMPRESS="zcat"
else
    echo "WARNING: no gunzip/zcat available in this image -- cannot restore $RESTORE_FILE. Starting with an empty database instead." >&2
    exit 0
fi

echo "Found backup at $RESTORE_FILE -- restoring into database '${POSTGRES_DB}'..."

if $DECOMPRESS "$RESTORE_FILE" | psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"; then
    echo "Restore from $RESTORE_FILE completed successfully."
else
    echo "WARNING: restore from $RESTORE_FILE failed partway through. Database may be partially populated -- check the errors above and consider wiping the data directory to retry." >&2
fi
