FROM python:3.13-slim

WORKDIR /app

# Install curl for container healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app.py database.py migrations.py settings.py rate_limiter.py routing.py config.yaml ./
COPY wwwroot/ ./wwwroot/

# Create data directory with appropriate write permissions
RUN mkdir -p /app/data && chown -R nobody:nogroup /app

USER nobody

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/healthz || exit 1

CMD ["python", "app.py", "--host", "0.0.0.0", "--port", "8000", "--allow-non-local-bind", "true"]
