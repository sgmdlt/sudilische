from celery import group

from courts.generator.url_generator import generate_urls
from courts.tasks import search_cases


def produce_tasks(search_params):
    urls = list(generate_urls(search_params))
    job = group(search_cases.s(url) for url in urls)
    result = job.apply_async()
    print("Все задачи отправлены на выполнение")
    return result.children
