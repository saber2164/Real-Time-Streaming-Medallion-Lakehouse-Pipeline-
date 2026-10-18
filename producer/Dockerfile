# Dockerfile.producer
# Python producer for synthetic e-commerce order events.
# Build context is ./producer (see docker-compose.yml `producer.build.context`),
# so COPY paths are relative to that directory.
FROM python:3.9-slim

WORKDIR /app

# Install deps first (cached layer) then copy source.
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . /app/

CMD ["python", "event_producer.py"]
