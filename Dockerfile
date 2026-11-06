FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY manager/ ./manager/
COPY workers/ ./workers/

RUN mkdir -p /data/raw /data/processed

CMD ["python", "-m", "manager.main"]
