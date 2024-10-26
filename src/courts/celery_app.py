import os

from celery import Celery
from dotenv import load_dotenv

load_dotenv()

BROKER_URL = os.getenv('BROKER_URL')
RESULT_BACKEND_URL = os.getenv('RESULT_BACKEND_URL')


app = Celery('parser',
             broker=BROKER_URL,
             include=['courts.tasks'])

app.conf.task_ignore_result = True
