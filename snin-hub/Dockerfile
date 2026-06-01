# syntax=docker/dockerfile:1.4
# SNIN V4 — Hub Docker Image
# FastAPI микросервисы: Supervisor, Hub API, Encryption Layer, Privacy Layer
#
# Build: docker build -t snin-v4/hub -f snin-hub/Dockerfile .
# Run:   docker run --rm -p 9900:9900 snin-v4/hub

FROM python:3.11-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Зависимости
COPY snin-hub/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY snin-hub/ ./
COPY relay-mesh/identities/ ./identities/

# Права
RUN useradd -m -u 1000 snin && chown -R snin:snin /app
USER snin

VOLUME ["/app/identities", "/app/logs"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -sf http://localhost:9900/health || exit 1

CMD ["python3", "supervisor.py"]
