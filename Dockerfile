# hadolint global ignore=SC1091,DL3008
# Minimal Dockerfile - single stage, pip only, no Conda/venv duplication

FROM python:3.12-slim

# Install system dependencies for native extensions (NumPy, SciPy, PyArrow, etc.)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
        libgomp1 \
        && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install all dependencies directly into the base Python
RUN pip install --no-cache-dir -e .

# Create system users (from original Dockerfile)
RUN groupadd -r appgroup && \
    useradd -r -g appgroup -d /home/appuser -m appuser && \
    groupadd -g 450 slurm && \
    useradd -u 450 -g 450 -d /cm/local/apps/slurm -m -s /bin/bash slurm

USER appuser
