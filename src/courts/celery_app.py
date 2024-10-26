import os

from celery import Celery
from dotenv import load_dotenv

load_dotenv()

BROKER_URL = os.getenv('BROKER_URL')
RESULT_BACKEND_URL = os.getenv('RESULT_BACKEND_URL')


app = Celery('parser',
             broker=BROKER_URL,
             include=['courts.tasks'],
             backend=RESULT_BACKEND_URL)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Moscow',
    enable_utc=True,
)
