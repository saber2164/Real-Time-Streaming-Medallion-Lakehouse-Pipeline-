# dashboard/Dockerfile
# Read-only stakeholder dashboard. No third-party Python packages: the image is
# just Python 3.11 + two files. This keeps startup fast and the supply chain tiny.
FROM python:3.11-slim

WORKDIR /app
COPY server.py index.html ./

ENV PYTHONUNBUFFERED=1 \
    PORT=8501 \
    METRICS_PATH=/lakehouse/dashboard/metrics.json

EXPOSE 8501

# Pure-Python healthcheck (slim image has no curl).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8501/healthz', timeout=2).status == 200 else 1)"

CMD ["python", "-u", "server.py"]
