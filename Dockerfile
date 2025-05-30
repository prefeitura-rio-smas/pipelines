# Dockerfile for ARCgis → BigQuery + dbt pipeline

FROM python:3.11-slim

# Install OS dependencies (if needed; e.g., git for dbt packages)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy Python pipeline and dbt project
COPY pipeline/ ./pipeline
COPY queries/ ./queries

# Copy requirements and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir dbt-core dbt-bigquery

# Environment variables for credentials
ENV GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp/credentials/leoneabreu-smas.json

# Default command: run the ELT pipeline (bronze + dbt gold)
CMD ["python", "-m", "pipeline.flows"]
