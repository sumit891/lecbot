# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim-bookworm

# Set environment variables for Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set work directory
WORKDIR /app

# Install system dependencies including FFmpeg and cleanup in one layer
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Create and set permissions for working directory
RUN mkdir -p /tmp/tgbot_converter_2025 && \
    chmod 755 /tmp/tgbot_converter_2025

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Expose port (Zeabur might need this)
EXPOSE 8080

# Run the application
CMD ["python", "main.py"]