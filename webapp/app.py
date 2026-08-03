"""
Read-only web dashboard for La Crosse weather station data.

Connects to Postgres using the least-privilege `lacrosse_readonly` role
(see db/init/00-create-readonly-role.sh) -- this service can only ever
SELECT, and can never modify or delete collected data.

Opens a fresh connection per request rather than pooling. Traffic here is
a single household dashboard, not a high-throughput API, so the simplicity
is worth the small per-request connect cost. Revisit with a connection pool
(e.g. psycopg_pool) if this is ever used by more than a handful of clients.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from limits import parse as parse_rate_limit
from limits.storage import MemoryStorage
from limits.strategies import FixedWindowRateLimiter
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


# Deliberately generic -- slowapi's default handler echoes back the exact
# limit ("30 per 1 minute"), which just tells anyone probing this exactly
# where the threshold is so they can stay a request under it. Used for both
# the per-route decorators below and the blanket middleware, so a caller
# can't even tell which layer they tripped.
RATE_LIMIT_MESSAGE = "Too many requests. Please slow down and try again shortly."


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    return JSONResponse({"error": RATE_LIMIT_MESSAGE}, status_code=429)


# --- Anonymous per-browser session cookie ---
# Not an account system -- there's no login yet, so this is just a random ID
# handed to each browser on first visit, used below to rate-limit per
# "session" in addition to per source IP (two browsers behind the same
# router's IP still get separate buckets; a rate-limit-evading IP change
# doesn't reset a browser's bucket).
#
# Deliberately built so a future login can slot in without redoing this:
# add a server-side store (e.g. a dict, or a table) keyed by this same
# session_id, and a /login endpoint that marks that session as
# authenticated. Nothing here needs to change for that to work.
SESSION_COOKIE = "session_id"
SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 365  # 1 year


class SessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        session_id = request.cookies.get(SESSION_COOKIE)
        is_new = session_id is None
        if is_new:
            session_id = uuid.uuid4().hex
        request.state.session_id = session_id

        response = await call_next(request)

        if is_new:
            # httponly: never needed by our own JS. samesite=lax: fine for a
            # same-origin dashboard. No `secure` flag -- this runs over plain
            # HTTP on the LAN by design (see docker-compose.yml), and
            # `secure` cookies are silently dropped by browsers over HTTP.
            response.set_cookie(
                SESSION_COOKIE,
                session_id,
                max_age=SESSION_MAX_AGE_SECONDS,
                httponly=True,
                samesite="lax",
            )
        return response


def get_session_key(request: Request) -> str:
    # Reads the cookie already present on the *incoming* request. A brand
    # new browser's first request has none yet (this middleware only sets
    # it on the outgoing response), so first-touch requests briefly share
    # one "anonymous" bucket until the cookie round-trips -- negligible in
    # practice.
    return request.cookies.get(SESSION_COOKIE, "anonymous")


limiter = Limiter(key_func=get_remote_address)

# --- Blanket safety net for the whole /api/ surface ---
# The @limiter.limit(...) decorators below are tuned per-endpoint for how
# the dashboard actually uses list_devices/get_readings today. This is a
# separate, coarser backstop covering every path under /api/, including any
# added later without remembering to decorate it.
#
# Deliberately a plain middleware rather than a third slowapi decorator:
# slowapi's own SlowAPIMiddleware skips any route that already has a
# @limiter.limit decorator (so it doesn't double-count that route), and
# skips routes with none at all unless default_limits/application_limits
# are configured on the Limiter -- neither covers "any current or future
# /api/ path, decorated or not" in one place. A tiny dedicated middleware
# does, using the same underlying `limits` library slowapi itself wraps.
#
# /api/health is deliberately excluded -- it's a cheap SELECT 1 used for
# Docker healthchecks and manual troubleshooting, and shouldn't compete
# with dashboard traffic for the same budget.
#
# Configurable via .env (see .env.example for format and guidance), but
# these are meant to stay a loose backstop -- to tune actual dashboard
# behavior, adjust the per-endpoint limits below instead of these.
_blanket_storage = MemoryStorage()
_blanket_limiter = FixedWindowRateLimiter(_blanket_storage)
_BLANKET_IP_LIMIT = parse_rate_limit(_env("WEBAPP_RATE_LIMIT_BLANKET_PER_IP", "300/minute"))
_BLANKET_SESSION_LIMIT = parse_rate_limit(_env("WEBAPP_RATE_LIMIT_BLANKET_PER_SESSION", "150/minute"))


class BlanketApiRateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path.startswith("/api/") and path != "/api/health":
            ip_key = get_remote_address(request)
            if not _blanket_limiter.hit(_BLANKET_IP_LIMIT, "blanket-ip", ip_key):
                return JSONResponse({"error": RATE_LIMIT_MESSAGE}, status_code=429)
            session_key = get_session_key(request)
            if not _blanket_limiter.hit(_BLANKET_SESSION_LIMIT, "blanket-session", session_key):
                return JSONResponse({"error": RATE_LIMIT_MESSAGE}, status_code=429)
        return await call_next(request)


POSTGRES_HOST = _env("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(_env("POSTGRES_PORT", "5432"))
POSTGRES_DB = _env("POSTGRES_DB", "lacrosse")
# Deliberately the read-only role, not the collector's read/write user --
# a bug here can never touch collected data.
POSTGRES_USER = _env("WEBAPP_DB_USER", required=True)
POSTGRES_PASSWORD = _env("WEBAPP_DB_PASSWORD", required=True)

RANGE_PRESETS = {
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "90d": timedelta(days=90),
}


def get_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        autocommit=True,
    )


app = FastAPI(title="La Crosse Weather Dashboard")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(BlanketApiRateLimitMiddleware)
app.add_middleware(SessionMiddleware)


@app.get("/api/devices")
@limiter.limit(_env("WEBAPP_RATE_LIMIT_DEVICES_PER_IP", "60/minute"))
@limiter.limit(_env("WEBAPP_RATE_LIMIT_DEVICES_PER_SESSION", "30/minute"), key_func=get_session_key)
def list_devices(request: Request):
    """Locations -> devices -> distinct fields/units, to populate the pickers."""
    query = """
        SELECT l.id, l.name, d.id, d.name, d.sensor_type, r.field, r.unit
        FROM locations l
        JOIN devices d ON d.location_id = l.id
        JOIN LATERAL (
            SELECT DISTINCT field, unit FROM readings WHERE device_id = d.id
        ) r ON true
        ORDER BY l.name, d.name, r.field;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    locations: dict[str, dict] = {}
    for location_id, location_name, device_id, device_name, sensor_type, field, unit in rows:
        loc = locations.setdefault(
            location_id, {"id": location_id, "name": location_name, "devices": {}}
        )
        dev = loc["devices"].setdefault(
            device_id,
            {"id": device_id, "name": device_name, "sensor_type": sensor_type, "fields": []},
        )
        dev["fields"].append({"field": field, "unit": unit})

    result = []
    for loc in locations.values():
        loc["devices"] = list(loc["devices"].values())
        result.append(loc)
    return result


@app.get("/api/readings")
@limiter.limit(_env("WEBAPP_RATE_LIMIT_READINGS_PER_IP", "120/minute"))
@limiter.limit(_env("WEBAPP_RATE_LIMIT_READINGS_PER_SESSION", "60/minute"), key_func=get_session_key)
def get_readings(
    request: Request,
    device_id: str,
    field: str,
    range: str = Query("24h", description="One of 24h, 7d, 30d, 90d -- ignored if start/end given"),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    if start and end:
        try:
            start_ts = datetime.fromisoformat(start)
            end_ts = datetime.fromisoformat(end)
        except ValueError as e:
            raise HTTPException(400, f"invalid start/end: {e}")
    else:
        if range not in RANGE_PRESETS:
            raise HTTPException(400, f"range must be one of {list(RANGE_PRESETS)}")
        end_ts = datetime.now(timezone.utc)
        start_ts = end_ts - RANGE_PRESETS[range]

    query = """
        SELECT ts, value, unit
        FROM readings
        WHERE device_id = %s AND field = %s AND ts BETWEEN %s AND %s AND value IS NOT NULL
        ORDER BY ts ASC;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query, (device_id, field, start_ts, end_ts))
        rows = cur.fetchall()

    unit = rows[0][2] if rows else None
    points = [{"ts": ts.isoformat(), "value": value} for ts, value, _ in rows]
    return {"device_id": device_id, "field": field, "unit": unit, "points": points}


@app.get("/api/health")
def health():
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(503, str(e))


# Registered last: a Mount only catches requests that don't match a more
# specific route already registered above (e.g. /api/devices), so this
# serves the dashboard's static files (index.html, etc.) for everything else.
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
