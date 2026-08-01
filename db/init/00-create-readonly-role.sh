#!/bin/sh
# Runs automatically via the official postgres image's initdb hook -- but
# ONLY the first time the data volume is initialized (empty pgdata). If this
# is added after the volume already exists, run its contents manually once
# via `docker compose exec postgres psql ...` instead.
#
# Creates a SELECT-only role intended for a future read-only consumer (e.g.
# a web dashboard) -- kept separate from the collector's read/write user so
# a bug in that consumer can never modify or delete collected data.
#
# Named 00- so it runs BEFORE 01-restore-from-backup.sh. This matters: a
# pg_dump backup captures GRANT statements referencing this role, but never
# a CREATE ROLE for it (roles are cluster-level, not database-level, so
# pg_dump never includes them). Restoring into a fresh cluster before this
# role exists would fail on those trailing GRANTs -- confirmed by testing.

set -e

: "${WEBAPP_DB_USER:=lacrosse_readonly}"

if [ -z "$WEBAPP_DB_PASSWORD" ]; then
    echo "WEBAPP_DB_PASSWORD not set -- skipping read-only role creation." >&2
    exit 0
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${WEBAPP_DB_USER}') THEN
            CREATE ROLE ${WEBAPP_DB_USER} LOGIN PASSWORD '${WEBAPP_DB_PASSWORD}';
        END IF;
    END
    \$\$;

    GRANT CONNECT ON DATABASE "${POSTGRES_DB}" TO ${WEBAPP_DB_USER};
    GRANT USAGE ON SCHEMA public TO ${WEBAPP_DB_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${WEBAPP_DB_USER};
    -- Applies to tables created later (by schema.sql or a restored backup)
    -- too, since they're created by the same $POSTGRES_USER role running
    -- this script.
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${WEBAPP_DB_USER};
EOSQL

echo "Read-only role '${WEBAPP_DB_USER}' ready."
