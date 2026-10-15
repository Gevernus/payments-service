.PHONY: lint fmt typecheck test-unit test ci up down e2e loadtest

lint:
	uv run ruff check
	uv run ruff format --check

fmt:
	uv run ruff check --fix
	uv run ruff format

typecheck:
	uv run mypy

test-unit:
	uv run pytest tests/unit -q

test:
	uv run pytest -m "not e2e" -q

ci: lint typecheck
	REQUIRE_INFRA=1 uv run pytest -m "not e2e"

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

e2e:
	docker compose -f compose.yaml -f compose.e2e.yaml up -d --build --wait
	REQUIRE_INFRA=1 uv run pytest -m e2e -q; status=$$?; \
	docker compose -f compose.yaml -f compose.e2e.yaml down -v; exit $$status

loadtest:
	uv run python tools/loadtest.py
