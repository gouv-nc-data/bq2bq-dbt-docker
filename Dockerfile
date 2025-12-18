# =============================================================================
# bq2bq-dbt-runner Docker Image
# Optimized multi-stage build with uv
# =============================================================================

FROM python:3.12-slim-trixie AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.9.17 /uv /uvx /bin/

# Set environment variables for uv
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Set working directory
WORKDIR /app

# Copy dependency files first for caching
COPY pyproject.toml uv.lock README.md ./

# Sync dependencies using cache mount and skip project install for now
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# =============================================================================
# Runtime stage
# =============================================================================

FROM python:3.12-slim-trixie

# Install uv for runtime (needed for dbt)
COPY --from=ghcr.io/astral-sh/uv:0.9.17 /uv /uvx /bin/

# Set bytecode compilation for faster startup
ENV UV_COMPILE_BYTECODE=1

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy source files
COPY src/ ./

# Set environment
ENV PATH="/app/.venv/bin:$PATH"
ENV DBT_PROFILES_DIR=/app
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "main.py"]
