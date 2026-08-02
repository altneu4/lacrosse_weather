"""Collects readings from a La Crosse View account and stores them in PostgreSQL.

Runs forever, polling on an interval. Each cycle:
  1. Logs into the La Crosse View API.
  2. Fetches locations (optionally restricted to one, via LACROSSE_LOCATION_INDEX).
  3. Fetches sensors/readings for each location since the last stored reading
     (with a small overlap window to avoid gaps from slow-reporting sensors).
  4. Upserts locations/devices and inserts readings into Postgres.
  5. If LACROSSE_BACKFILL_FULL_HISTORY is enabled, compares the oldest reading
     currently stored against LACROSSE_BACKFILL_START_DATE and, if there's
     still a gap, pulls one more chunk further back in time.

Step 5 is what makes backfill resumable without any separate state table:
progress is just whatever's already in the `readings` table, so it works
whether backfill is turned on from the first run or enabled months later,
and a restart mid-backfill picks up exactly where it left off.

Always connects to the bundled Postgres service defined in
docker-compose.yml. All configuration comes from environment variables --
see .env.example.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import NamedTuple, Optional

import psycopg
from lacrosse_view import LaCrosse, LaCrosseError, Location, Sensor

import heartbeat

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")


def _env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _bool_env(name: str, default: str = "false") -> bool:
    return _env(name, default).strip().lower() in ("1", "true", "yes", "on")


EMAIL = _env("LACROSSE_EMAIL", required=True)
PASSWORD = _env("LACROSSE_PASSWORD", required=True)

# Passed to psycopg.connect() as discrete keyword arguments rather than a
# concatenated "postgresql://user:password@host/db" URI -- a URI requires
# percent-encoding any reserved characters in the password (*, #, %, !, @,
# etc.), which a plain string concatenation doesn't do. Keyword arguments
# bypass URI parsing entirely, so the password is used exactly as given
# regardless of what characters it contains.
POSTGRES_HOST = _env("POSTGRES_HOST", "postgres")
POSTGRES_PORT = _env("POSTGRES_PORT", "5432")
POSTGRES_USER = _env("POSTGRES_USER", "lacrosse")
POSTGRES_PASSWORD = _env("POSTGRES_PASSWORD", required=True)
POSTGRES_DB = _env("POSTGRES_DB", "lacrosse")

TZ_NAME = _env("LACROSSE_TIMEZONE", "UTC")
INTERVAL_MINUTES = int(_env("COLLECTION_INTERVAL_MINUTES", "15"))
INITIAL_LOOKBACK_MINUTES = int(_env("LACROSSE_INITIAL_LOOKBACK_MINUTES", "60"))
OVERLAP_MINUTES = int(_env("LACROSSE_OVERLAP_MINUTES", "10"))
_location_index = _env("LACROSSE_LOCATION_INDEX", "")
LOCATION_INDEX: Optional[int] = int(_location_index) if _location_index else None

# Full-history backfill: each cycle, if enabled and the oldest stored reading
# is still newer than this date, one more chunk is pulled going further back.
BACKFILL_FULL_HISTORY = _bool_env("LACROSSE_BACKFILL_FULL_HISTORY", "false")
BACKFILL_START_DATE = _env("LACROSSE_BACKFILL_START_DATE", "2000-01-01")
BACKFILL_CHUNK_DAYS = int(_env("LACROSSE_BACKFILL_CHUNK_DAYS", "7"))
# Pause before the backfill's get_sensors() call. Testing showed the API
# returns 401 on a second such call within a few seconds of the forward
# collection's call, regardless of whether it's the same or a different
# login session -- looks like a short-window rate limit on this endpoint.
# Increase if 401s persist.
BACKFILL_DELAY_SECONDS = float(_env("LACROSSE_BACKFILL_DELAY_SECONDS", "30"))
# After this many consecutive backfill attempts make no progress (the API
# keeps returning the same floor regardless of the requested start date),
# stop retrying rather than hammering the API forever for zero benefit.
BACKFILL_STALL_LIMIT = int(_env("LACROSSE_BACKFILL_STALL_LIMIT", "3"))

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "db", "schema.sql")

# In-memory only (not persisted) -- tracks consecutive no-progress backfill
# attempts. Resets on restart; a few redundant attempts after a restart
# before re-detecting the same stall are harmless.
_backfill_stall_count = 0
_backfill_stalled = False


def connect_db(retries: int = 10, delay_seconds: float = 3.0) -> psycopg.Connection:
    """Connect to Postgres, retrying while the DB container is still starting up."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB,
                autocommit=True,
            )
        except psycopg.OperationalError as e:
            last_err = e
            log.warning("Database not ready (attempt %s/%s): %s", attempt, retries, e)
            time.sleep(delay_seconds)
    raise RuntimeError(f"Could not connect to database after {retries} attempts") from last_err


def ensure_schema(conn: psycopg.Connection) -> None:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)


def upsert_location(conn: psycopg.Connection, location: Location) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO locations (id, name) VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """,
            (location.id, location.name),
        )


def upsert_device(conn: psycopg.Connection, sensor: Sensor) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO devices (id, location_id, name, sensor_id, sensor_type)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                location_id = EXCLUDED.location_id,
                name = EXCLUDED.name,
                sensor_id = EXCLUDED.sensor_id,
                sensor_type = EXCLUDED.sensor_type
            """,
            (sensor.device_id, sensor.location.id, sensor.name, sensor.sensor_id, sensor.type),
        )


def get_last_reading_ts(conn: psycopg.Connection) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ts) FROM readings")
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def get_earliest_reading_ts(conn: psycopg.Connection) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(ts) FROM readings")
        row = cur.fetchone()
        return row[0] if row and row[0] else None


class CollectStats(NamedTuple):
    """Outcome of writing a batch of readings.

    `new` and `updated` distinguish rows that didn't exist before from rows
    that did (the API commonly returns a wider window than requested, so
    re-seeing already-stored readings is normal, not a sign of trouble).
    `oldest_ts` is the earliest timestamp among *everything the API
    returned* this call, regardless of new/updated -- useful for seeing how
    wide a window the API actually handed back relative to what was asked
    for.
    """

    new: int = 0
    updated: int = 0
    oldest_ts: Optional[datetime] = None

    def __add__(self, other: "CollectStats") -> "CollectStats":
        oldest = self.oldest_ts
        if other.oldest_ts is not None and (oldest is None or other.oldest_ts < oldest):
            oldest = other.oldest_ts
        return CollectStats(self.new + other.new, self.updated + other.updated, oldest)


def insert_readings(conn: psycopg.Connection, sensor: Sensor) -> CollectStats:
    if not sensor.data:
        return CollectStats()

    rows = []
    for field in sensor.sensor_field_names:
        field_data = sensor.data.get(field)
        if not field_data:
            continue
        unit = field_data.get("unit")
        for value in field_data.get("values", []):
            raw = value.get("s")
            unix_ts = value.get("u")
            if unix_ts is None:
                continue
            ts = datetime.fromtimestamp(float(unix_ts), tz=timezone.utc)
            try:
                numeric = float(raw)
            except (TypeError, ValueError):
                numeric = None
            rows.append((sensor.device_id, field, raw, numeric, unit, ts))

    if not rows:
        return CollectStats()

    oldest_ts = min(row[5] for row in rows)
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO readings (device_id, field, raw_value, value, unit, ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id, field, ts) DO UPDATE SET
                raw_value = EXCLUDED.raw_value,
                value = EXCLUDED.value,
                unit = EXCLUDED.unit
            -- xmax = 0 distinguishes a genuinely new row from a conflict
            -- that took the DO UPDATE path -- a well-known Postgres upsert
            -- idiom (the conflict-updated row's xmax is set to the current
            -- transaction id, not 0, within the same command).
            RETURNING (xmax = 0) AS inserted
            """,
            rows,
            returning=True,
        )
        while True:
            for (inserted,) in cur.fetchall():
                if inserted:
                    new_count += 1
                else:
                    updated_count += 1
            if not cur.nextset():
                break

    return CollectStats(new_count, updated_count, oldest_ts)


async def collect_window(
    api: LaCrosse,
    conn: psycopg.Connection,
    locations: list[Location],
    start_dt: datetime,
    end_dt: datetime,
) -> CollectStats:
    """Fetch and store readings for all given locations within [start_dt, end_dt]."""
    stats = CollectStats()
    for location in locations:
        upsert_location(conn, location)
        sensors = await api.get_sensors(
            location, tz=TZ_NAME, start=start_dt.timestamp(), end=end_dt.timestamp()
        )
        for sensor in sensors:
            upsert_device(conn, sensor)
            stats = stats + insert_readings(conn, sensor)
    return stats


def _parse_backfill_target() -> Optional[datetime]:
    try:
        return datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        log.error(
            "Invalid LACROSSE_BACKFILL_START_DATE %r (expected YYYY-MM-DD), "
            "skipping backfill this cycle",
            BACKFILL_START_DATE,
        )
        return None


async def _logout_quietly(api: LaCrosse) -> None:
    """Best-effort logout. A failed logout doesn't affect data already
    collected, and each session is short-lived and one-shot anyway -- so
    this is a warning, not a cycle failure like a login/collection error.
    """
    try:
        await api.logout()
    except LaCrosseError as e:
        log.warning("La Crosse logout failed (harmless): %s", e)


async def maybe_backfill_older(
    conn: psycopg.Connection, locations: list[Location]
) -> CollectStats:
    """If enabled, pull one more chunk older than the earliest stored reading.

    Compares the earliest reading currently in the database against
    LACROSSE_BACKFILL_START_DATE. If there's still a gap, fetches one chunk
    (LACROSSE_BACKFILL_CHUNK_DAYS wide) immediately before it. Naturally
    throttled to one chunk per collection cycle, and naturally resumable --
    there's no separate progress marker, since the earliest stored reading
    *is* the progress marker. Does nothing until at least one reading exists
    (the regular forward collection in run_once() establishes that first).

    Uses its own login session, and waits LACROSSE_BACKFILL_DELAY_SECONDS
    before calling get_sensors() -- testing showed the API returns 401 on a
    second such call within a few seconds of the forward collection's call,
    even across different sessions, consistent with a short-window rate
    limit on this endpoint rather than a session restriction. `locations` is
    reused from the caller's earlier get_locations() call since it's just
    plain data (id/name), not tied to a particular session.

    If LACROSSE_BACKFILL_STALL_LIMIT consecutive attempts make no progress
    (the earliest stored reading doesn't move), gives up and stops trying
    for the rest of this process's life -- testing showed the API can have
    a real data floor (e.g. ~7 days) that ignores the requested start date
    entirely, and retrying that same window forever would just waste an API
    call and a full delay every cycle for no benefit.
    """
    global _backfill_stall_count, _backfill_stalled

    if not BACKFILL_FULL_HISTORY or _backfill_stalled:
        return CollectStats()

    earliest_before = get_earliest_reading_ts(conn)
    if earliest_before is None:
        return CollectStats()

    target = _parse_backfill_target()
    if target is None or earliest_before <= target:
        return CollectStats()

    window_end = earliest_before
    window_start = max(target, earliest_before - timedelta(days=BACKFILL_CHUNK_DAYS))

    await asyncio.sleep(BACKFILL_DELAY_SECONDS)

    api = LaCrosse()
    await api.login(EMAIL, PASSWORD)
    try:
        stats = await collect_window(api, conn, locations, window_start, window_end)
    finally:
        await _logout_quietly(api)

    log.info(
        "Backfill %s to %s (target: %s): %s new, %s already stored, "
        "oldest returned %s",
        window_start.date(), window_end.date(), target.date(),
        stats.new, stats.updated,
        stats.oldest_ts.isoformat() if stats.oldest_ts else "n/a",
    )

    earliest_after = get_earliest_reading_ts(conn)
    made_progress = earliest_after is not None and earliest_after < earliest_before

    if made_progress:
        _backfill_stall_count = 0
    else:
        _backfill_stall_count += 1
        if _backfill_stall_count >= BACKFILL_STALL_LIMIT:
            _backfill_stalled = True
            log.warning(
                "Backfill made no progress in %s consecutive attempts (stuck "
                "around %s) -- the API doesn't appear to have data before "
                "this point regardless of LACROSSE_BACKFILL_START_DATE (%s). "
                "Pausing further backfill attempts until the collector is "
                "restarted.",
                BACKFILL_STALL_LIMIT, earliest_before.date(), target.date(),
            )

    return stats


async def _collect_forward(conn: psycopg.Connection) -> list[Location]:
    """Log in, do the normal forward collection, log out.

    Returns the account's locations so the caller can reuse them for
    backfill without an extra get_locations() call.
    """
    api = LaCrosse()
    await api.login(EMAIL, PASSWORD)
    try:
        locations = await api.get_locations()
        if LOCATION_INDEX is not None:
            locations = [locations[LOCATION_INDEX]]

        last_ts = get_last_reading_ts(conn)
        if last_ts is not None:
            start_dt = last_ts - timedelta(minutes=OVERLAP_MINUTES)
        else:
            start_dt = datetime.now(timezone.utc) - timedelta(minutes=INITIAL_LOOKBACK_MINUTES)
        end_dt = datetime.now(timezone.utc)

        stats = await collect_window(api, conn, locations, start_dt, end_dt)
        log.info(
            "Processed %s reading(s) across %s location(s): %s new, %s already "
            "stored, oldest returned %s",
            stats.new + stats.updated, len(locations),
            stats.new, stats.updated,
            stats.oldest_ts.isoformat() if stats.oldest_ts else "n/a",
        )
        return locations
    finally:
        await _logout_quietly(api)


async def run_once(conn: psycopg.Connection) -> None:
    locations = await _collect_forward(conn)
    await maybe_backfill_older(conn, locations)


def _watchdog_loop() -> None:
    """Runs on its own thread so it keeps checking even if the asyncio event
    loop is genuinely wedged (a hung network call with no timeout, a
    deadlock) -- exactly the case a heartbeat file is meant to catch, since
    that's the one failure mode the try/except in main_loop() below can't
    help with (the code never gets back around to hitting it).

    Deliberately exits the whole process (not just logs a warning) so
    `restart: unless-stopped` in docker-compose.yml gives it a clean restart.
    docker-compose's own `healthcheck:` only affects reported status, not
    restart behavior -- this is what actually recovers a stuck collector
    under plain Docker/Compose. Under Kubernetes, the equivalent
    livenessProbe would trigger a restart on its own, making this redundant
    there but harmless to leave in.
    """
    threshold = heartbeat.stale_after_seconds()
    while True:
        time.sleep(60)
        age = heartbeat.age_seconds()
        if age is not None and age > threshold:
            log.error(
                "No heartbeat in %.0fs (threshold %.0fs) -- assuming the "
                "main loop is stuck. Exiting so the container restarts.",
                age, threshold,
            )
            os._exit(1)


async def main_loop() -> None:
    conn = connect_db()
    ensure_schema(conn)
    interval_seconds = INTERVAL_MINUTES * 60

    # Seed the heartbeat before starting the watchdog, so the very first
    # cycle gets a full stale_after_seconds() window rather than looking
    # instantly stale (no heartbeat file yet) the moment the watchdog wakes up.
    heartbeat.touch()
    threading.Thread(target=_watchdog_loop, daemon=True).start()

    while True:
        try:
            await run_once(conn)
        except LaCrosseError as e:
            log.error("La Crosse API error: %s", e)
        except psycopg.Error as e:
            log.error("Database error: %s", e)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect_db()
        except Exception:
            log.exception("Unexpected error during collection cycle")

        # Touched after every cycle attempt, success or caught failure alike
        # -- a cleanly logged error still means the loop is alive and will
        # retry next interval. Only a cycle that never returns at all (truly
        # stuck) leaves this stale.
        heartbeat.touch()
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(main_loop())
