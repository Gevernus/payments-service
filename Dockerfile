FROM ghcr.io/astral-sh/uv:0.9 AS uv

FROM python:3.13-slim AS build
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Dependencies first, so source changes do not invalidate this layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PATH="/app/.venv/bin:$PATH"
RUN useradd --system --uid 10001 --no-create-home app
WORKDIR /app
COPY --from=build /app/.venv ./.venv
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
USER app
EXPOSE 8000
CMD ["uvicorn", "payments.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
