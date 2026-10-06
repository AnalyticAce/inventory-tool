FROM python:3.14-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser

WORKDIR /app

ENV LOG_DIR=/app/logs

# Copy dependency files for layer caching (build context is the repo root)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache

# Copy application source
COPY backend/*.py ./

RUN mkdir -p /app/logs && chown -R appuser:appgroup /app

USER appuser

EXPOSE 8020

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8020"]