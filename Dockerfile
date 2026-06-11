FROM python:3.11-slim

WORKDIR /app

# System deps for curl_cffi (needs libcurl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4 curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies (main + scraper)
COPY QualityDB/requirements.txt ./requirements.txt
COPY QualityDB/scraper/requirements.txt ./scraper_requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir -r scraper_requirements.txt \
 && pip install --no-cache-dir curl_cffi>=0.7.0

# Copy application code (no DBs — they live on the Fly volume)
COPY QualityDB/ ./QualityDB/

# Persistent volume at /data — DB_PATH and SNAPSHOTS_DB_PATH point here
RUN mkdir -p /data /app/QualityDB/scraper/logs

EXPOSE 8080

# DB_PATH and SNAPSHOTS_DB_PATH are set via fly.toml [env] or fly secrets
CMD ["python3", "QualityDB/server.py"]
