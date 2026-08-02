# Build context is the repo root (see docker-compose.yml).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collector.py heartbeat.py healthcheck.py .
COPY db/ ./db/

# No credentials or .env files are copied into the image -- they're supplied
# at runtime via environment variables (see docker-compose.yml / .env.example).

# Run as a dedicated non-root user rather than the default (root). The
# collector only makes outbound HTTP/DB calls and reads its own files -- it
# never needs elevated privileges, so there's no reason to run as root.
RUN groupadd --system collector \
    && useradd --system --no-create-home --gid collector collector \
    && chown -R collector:collector /app
USER collector

CMD ["python", "collector.py"]
