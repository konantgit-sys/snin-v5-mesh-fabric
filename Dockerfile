# syntax=docker/dockerfile:1.4
# SNIN V4 — Relay Mesh Docker Image
# 64 Python-файла, 3 основных модуля: relay-mesh, snin-hub, relay
# Multi-stage: builder <-> runtime, ~150MB final image
#
# Build: docker build -t snin-v4/relay-mesh -f relay-mesh/Dockerfile .
# Run:   docker run --rm -p 9932:9932 snin-v4/relay-mesh

# === Stage 1: builder ===
FROM python:3.11-slim AS builder

WORKDIR /build

# Копируем зависимости
COPY relay-mesh/requirements.txt ./
RUN pip install --user --no-cache-dir -r requirements.txt 2>/dev/null || true

# === Stage 2: runtime ===
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Копируем pip пакеты
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Копируем весь relay-mesh
COPY relay-mesh/ ./
COPY snin-hub/ ./hub/

# Идентичности
RUN mkdir -p identities logs
VOLUME ["/app/identities", "/app/logs", "/app/data"]

# Non-root user
RUN useradd -m -u 1000 snin && chown -R snin:snin /app
USER snin

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -sf http://localhost:9933/ || exit 1

# Default: Smart Router
CMD ["python3", "smart_router.py", "--port", "9932"]
