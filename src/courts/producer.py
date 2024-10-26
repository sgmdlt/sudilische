from celery import Celery

from courts.generator.url_generator import generate_urls
from courts.tasks import search_cases

app = Celery('tasks', broker='amqp://guest:guest@172.17.0.1:5672//')


def produce_tasks(search_params):
    for url in generate_urls(search_params):
        search_cases.delay(url)
    print("Все задачи отправлены на выполнение")
