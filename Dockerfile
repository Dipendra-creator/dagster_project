# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml setup.py setup.cfg ./
COPY dragster_project/ ./dragster_project/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e ".[dev]"

# Expose Dagster webserver port
EXPOSE 3000

# Set environment variables
ENV DAGSTER_HOME=/app/dagster_home

# Create dagster home directory
RUN mkdir -p $DAGSTER_HOME

# Default command to run Dagster
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
