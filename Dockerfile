# Dockerfile

# -----------------
# Builder container
# -----------------
FROM condaforge/mambaforge:24.9.2-0 AS builder

COPY environment_docker.yml /docker/environment.yml

RUN . /opt/conda/etc/profile.d/conda.sh && \
    mamba create --name lock && \
    conda activate lock && \
    mamba env list && \
    mamba install --yes pip conda-lock>=2.5.2 setuptools wheel && \
    conda lock \
        --file /docker/environment.yml \
        --kind lock \
        --lockfile /docker/conda-lock.yml

RUN . /opt/conda/etc/profile.d/conda.sh && \
    conda activate lock && \
    conda-lock install \
        --mamba \
        --copy \
        --prefix /opt/env \
        /docker/conda-lock.yml && conda clean -afy

# -----------------
# Primary container
# -----------------
FROM python:3.10-slim
# copy over the generated environment
#COPY --from=builder /opt/env /opt/env
COPY --from=builder /opt/env/bin /opt/env/bin/
COPY --from=builder /opt/env/lib /opt/env/lib/

# Set environment variables
ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  PATH="/opt/env/bin:${PATH}" \
  LC_ALL="C" \
  HOME=/home/userapp

# Define the appuser if not defined
RUN groupadd -r appgroup && \
     useradd -r -g appgroup -d $HOME -m appuser

USER appuser:appgroup

# Copy to cache them in docker layer
WORKDIR $HOME
COPY . $HOME/

RUN make install && \
    make clean

ENV PATH=$PATH:$HOME/.local/bin
