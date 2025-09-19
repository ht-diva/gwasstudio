# hadolint global ignore=SC1091
# Dockerfile

# -----------------
# Base environment
# -----------------
FROM condaforge/mambaforge:24.9.2-0 AS base_environment

COPY base_environment_docker.yml /docker/environment.yml

RUN . /opt/conda/etc/profile.d/conda.sh && \
    mamba create --name lock && \
    conda activate lock && \
    mamba env list && \
    mamba install --yes pip conda-lock>=2.5.2 setuptools wheel && \
    conda lock \
        --file /docker/environment.yml \
        --kind lock \
        --platform linux-64 \
        --platform linux-aarch64 \
        --lockfile /docker/conda-lock.yml

RUN . /opt/conda/etc/profile.d/conda.sh && \
    conda activate lock && \
    conda-lock install \
        --mamba \
        --copy \
        --prefix /opt/env \
        /docker/conda-lock.yml && \
    conda clean -afy

# -----------------
# Builder container
# -----------------
FROM python:3.12-slim AS builder
# copy over the generated environment
COPY --from=base_environment /opt/env /opt/env

# Set environment variables
ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  PATH="/opt/env/bin:${PATH}" \
  LC_ALL="C"

# Copy to cache them in docker layer
COPY src /opt/src/
COPY README.md /opt/src
COPY pyproject.toml /opt/src

WORKDIR /opt/src

RUN poetry build

# -----------------
# Primary container
# -----------------
FROM python:3.12-slim

# Set environment variables
ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  LC_ALL="C" \
  HOME=/home/userapp

COPY --from=builder /opt/src/dist /opt/dist

RUN pip install --no-cache-dir /opt/dist/*.whl

# Define the appuser if not defined
RUN groupadd -r appgroup && \
    useradd -r -g appgroup -d $HOME -m appuser && \
    groupadd -g 450 slurm && \
    useradd -u 450 -g 450 -d /cm/local/apps/slurm -m -s /bin/bash slurm

USER appuser:appgroup
