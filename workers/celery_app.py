import os
from celery import Celery

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://redis:6379/0"
)

app = Celery(
    "ibge_workers",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["workers.tasks"]
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="America/Sao_Paulo",
    enable_utc=True,
    task_track_started=True,
)
