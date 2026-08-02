"""Liveness check for the collector.

Run standalone: `python3 healthcheck.py`. Exit 0 if the heartbeat is fresh,
exit 1 (with an explanatory message on stderr) otherwise. Used by
docker-compose.yml's `healthcheck:` for this service today; if this image is
ever run under Kubernetes instead, the exact same command works unchanged as
a livenessProbe's exec command -- see heartbeat.py for why the logic lives
there rather than here.
"""

from __future__ import annotations

import sys

import heartbeat


def main() -> int:
    age = heartbeat.age_seconds()
    threshold = heartbeat.stale_after_seconds()

    if age is None:
        print("No heartbeat file yet (collector may still be starting).", file=sys.stderr)
        return 1

    if age > threshold:
        print(f"Heartbeat is {age:.0f}s old, exceeds {threshold:.0f}s threshold.", file=sys.stderr)
        return 1

    print(f"OK -- heartbeat is {age:.0f}s old.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
