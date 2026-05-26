FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY QualityDB/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt 2>/dev/null || true

# Copy application code
COPY QualityDB/ ./QualityDB/

# The database lives on a persistent Fly volume mounted at /data
# entrypoint.sh seeds the volume on first boot if needed
RUN mkdir -p /data

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

CMD ["/entrypoint.sh"]
