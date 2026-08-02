"""Shared heartbeat-file logic for the collector's liveness check.

Deliberately its own tiny module with no dependency on collector.py's full
import chain (psycopg, lacrosse_view) or its required-env-var validation --
healthcheck.py needs to import only this, so a health check never fails for
reasons unrelated to whether the collector is actually stuck.

The same mechanism works unchanged as a Kubernetes livenessProbe later: run
`python3 healthcheck.py` as the probe's exec command instead of wiring it
into docker-compose.yml's `healthcheck:` block. Unlike plain `docker compose`
(where a failing healthcheck only changes reported status, not restart
behavior), Kubernetes *does* restart a container whose liveness probe fails
-- collector.py's self-watchdog below exists so the same protection also
applies to plain Docker/Compose deployments.
"""

from __future__ import annotations

import os
import pathlib
import time
from typing import Optional

HEARTBEAT_FILE = pathlib.Path(os.environ.get("HEARTBEAT_FILE", "/tmp/collector_heartbeat"))


def stale_after_seconds() -> float:
    """How old the heartbeat can get before it's considered stuck, not just slow.

    A normal cycle takes well under a minute; this is deliberately a generous
    multiple of the collection interval (minimum 10 minutes) so a slow API
    response or a brief La Crosse outage never triggers a false positive --
    the goal is catching a genuinely wedged process, not fast detection.
    """
    interval_minutes = float(os.environ.get("COLLECTION_INTERVAL_MINUTES", "15"))
    return max(interval_minutes * 3, 10) * 60


def touch() -> None:
    HEARTBEAT_FILE.write_text(str(time.time()))


def age_seconds() -> Optional[float]:
    """Seconds since the last touch(), or None if the heartbeat file doesn't
    exist yet (e.g. before the first cycle has completed)."""
    try:
        last = float(HEARTBEAT_FILE.read_text())
    except (FileNotFoundError, ValueError):
        return None
    return time.time() - last
