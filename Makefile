test:
	uv run pytest -vv tests

lint:
	uv run ruff check

start:
	uv run celery -A courts.celery_app worker --loglevel=info

start-debug:
	uv run celery -A courts.celery_app flower --loglevel=info
