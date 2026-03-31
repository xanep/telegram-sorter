FROM python:3.12-slim

WORKDIR /app

# System deps for Telethon crypto acceleration
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer cache-friendly)
COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Copy application source
COPY src/ ./src/
COPY scripts/ ./scripts/

# config.yaml is mounted via K8s ConfigMap; include a local copy for dev
COPY config.yaml ./

# SERVICE env var selects: listener | classifier | router | all
ENV SERVICE=all

CMD ["python", "src/main.py"]

