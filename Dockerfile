# =============================================================================
# Axonal Data Platform - Celery Worker Dockerfile
# Supports both FastAPI server and Celery worker modes
# =============================================================================

FROM python:3.11-slim

# Build arguments
ARG BUILD_VERSION=dev

# Labels for container metadata
LABEL maintainer="Axonal Team"
LABEL version="${BUILD_VERSION}"
LABEL description="Axonal Data Platform - ETL Pipeline with Celery"

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    BUILD_VERSION=${BUILD_VERSION}

WORKDIR /app

# Upgrade pip and setuptools FIRST to address jaraco.context vulnerability
# Latest setuptools includes jaraco.context 6.1.0+ in vendor directory which fixes GHSA-58pv-8j8x-9vj2
# Must upgrade BEFORE installing any dependencies to ensure fixed version is used
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install system dependencies
# - gcc, libpq-dev: Required for psycopg2 compilation
# - curl: Health checks
# - procps: Process management utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install Python dependencies (setuptools is already upgraded above)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user for security
RUN groupadd --gid 1000 celery \
    && useradd --uid 1000 --gid celery --shell /bin/bash --create-home celery \
    && chown -R celery:celery /app

# Create directories for local data processing (if needed)
RUN mkdir -p /app/data /app/logs \
    && chown -R celery:celery /app/data /app/logs

USER celery

# Expose port for FastAPI (when running API server)
EXPOSE 8000

# Health check for container orchestration
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command - Celery worker
# Override with docker run command for different modes:
#   API Server:    uvicorn main:app --host 0.0.0.0 --port 8000
#   Celery Worker: celery -A celery_app worker --loglevel=info
#   Celery Beat:   celery -A celery_app beat --loglevel=info
#   Flower:        celery -A celery_app flower --port=5555
CMD ["celery", "-A", "celery_app", "worker", "--loglevel=info"]
