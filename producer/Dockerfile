# Dockerfile.producer
# Python producer for synthetic e-commerce order events.
FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        "confluent-kafka==2.3.0"

COPY producer/ /app/

CMD ["python", "event_producer.py"]
