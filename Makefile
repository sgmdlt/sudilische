test:
	uv run pytest -s tests

lint:
	uv run ruff check src/courts

start:
	uv run celery -A courts.celery_app worker --loglevel=info

start-debug:
	uv run celery -A courts.celery_app flower --loglevel=info

run-server:
	uv run python -m courts.court_server
