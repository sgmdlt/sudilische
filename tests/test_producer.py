import json

import pytest
from celery.contrib.testing.worker import start_worker
from conftest import get_fixtures_path

from courts.celery_app import app as current_app
from courts.producer import produce_tasks


@pytest.fixture(scope='module')
def celery_config():
    current_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
        broker_url='memory://',
        backend='cache+memory://'
    )

@pytest.fixture(scope='module')
def celery_worker(celery_config):
    with start_worker(current_app, perform_ping_check=False, loglevel='info') as worker:
        yield worker


def get_links_fixture():
    links = []
    with open(get_fixtures_path() / "links.jsonl") as f:
        for line in f:
            links.append(json.loads(line.strip()))
    return links


@pytest.mark.vcr
def test_produce_tasks(celery_worker):
    search_params = {
        "court_url": ["http://giaginsky.adg.sudrf.ru/modules.php?"],
        "instance": [1],
        "type": ["УК"],
        "entry_date_from": "01.01.2023",
        "entry_date_to": "31.01.2023",
    }
    results = produce_tasks(search_params)
    assert isinstance(results, list)
    assert len(results) == 1
    first_result = results[0]
    assert first_result.get()[0]['link'] == get_links_fixture()[0]['link']
