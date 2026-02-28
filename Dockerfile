# ─────────────────────────────────────────────────────────────────────
# Green Bharat — Dockerfile
# Multi-stage, production-ready
# ─────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS base

LABEL maintainer="Green Bharat Team"
LABEL description="Real-Time Government Budget Monitoring & AI Auditor"

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  curl \
  git \
  poppler-utils \
  libmagic1 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Install uv (fast Python package manager — handles deep dep trees) ─
RUN pip install --no-cache-dir uv

# ── Install Python deps via uv (resolves pathway's complex tree) ──────
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# ── Copy application source ──────────────────────────────────────────
COPY ingestion.py       .
COPY transformations.py .
COPY rag_layer.py       .
COPY main.py            .
COPY .env.example       .env

# ── Runtime directories (Pathway creates output / data at runtime) ───
RUN mkdir -p data output data/policy_docs

# ── Expose FastAPI port ──────────────────────────────────────────────
EXPOSE 8000

# ── Health check ─────────────────────────────────────────────────────
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s \
  CMD curl -f http://localhost:8000/api/status || exit 1

# ── Entry point ──────────────────────────────────────────────────────
CMD ["python", "main.py"]
