# Healthcare Claims Resubmission Pipeline - Production Docker Image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV LOG_LEVEL=INFO
ENV REFERENCE_DATE=2025-07-30

# Create app directory and user
WORKDIR /app
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install python-multipart

# Copy application code
COPY healthcare_pipeline.py .
COPY fastapi_extension.py .
COPY dagster_pipeline.py .
COPY test_suite.py .

# Create directories for data and logs
RUN mkdir -p /app/data /app/logs /app/output \
    && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/ || exit 1

# Expose ports
EXPOSE 8000 3000

# Default command runs FastAPI server
CMD ["uvicorn", "fastapi_extension:app", "--host", "0.0.0.0", "--port", "8000"]
