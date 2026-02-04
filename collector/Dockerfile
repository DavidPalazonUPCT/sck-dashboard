FROM python:3.12-slim

# Install uv (fast Python package manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency definition first (cache layer)
COPY pyproject.toml .

# Install dependencies (no dev deps, no editable install)
RUN uv pip install --system --no-cache .

# Copy application code
COPY config.py collector.py ./
COPY tests/ tests/

HEALTHCHECK --interval=30s --timeout=5s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["python", "-u", "collector.py"]
