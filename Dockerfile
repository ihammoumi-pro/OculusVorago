# =============================================================================
# Stage 1: Builder — install all Python dependencies into a venv
# =============================================================================
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build-time system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment so we can copy it cleanly to the final stage
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies (runtime only — no test/dev extras needed)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        pydantic>=2.0 \
        pyyaml>=6.0 \
        typer>=0.12 \
        confluent-kafka>=2.4 \
        requests>=2.31 \
        sqlalchemy>=2.0 \
        pymupdf>=1.24 \
        pytesseract>=0.3 \
        pdf2image>=1.17 \
        pillow>=10.0


# =============================================================================
# Stage 2: Runtime — minimal image with no build toolchain
# =============================================================================
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="OculusVorago" \
      org.opencontainers.image.description="Configuration-driven streaming ETL pipeline for the Oculus intelligence platform." \
      org.opencontainers.image.source="https://github.com/ihammoumi-pro/OculusVorago"

# Install runtime system libraries required by pytesseract, pdf2image, and PyMuPDF
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    poppler-utils \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create a non-root user and group for security
RUN groupadd --gid 1001 vorago \
    && useradd --uid 1001 --gid vorago --no-create-home --shell /usr/sbin/nologin vorago

WORKDIR /app

# Copy application source code
COPY main.py .
COPY vorago/ ./vorago/
COPY configs/ ./configs/

# Ensure the non-root user owns the application directory
RUN chown -R vorago:vorago /app

USER vorago

# All logs go to stdout/stderr (Twelve-Factor App compliance)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# ENTRYPOINT allows passing CLI arguments to trigger specific pipeline configs.
# Example usage:
#   docker run oculus-vorago run-pipeline --extractor csv --source /data/file.csv --config /configs/icij_mapping.yaml
ENTRYPOINT ["python", "main.py"]

# Default to showing help when no arguments are provided
CMD ["--help"]
