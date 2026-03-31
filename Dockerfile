FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# Install system dependencies including curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory for the code
WORKDIR /opt/dagster/app

# Set environ variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV UV_COMPILE_BYTECODE=1

# Copy project dependencies files
COPY pyproject.toml uv.lock ./

# Install dependencies (including dev groups to get dagster-webserver)
RUN uv sync --frozen --no-install-project

# Install playwright system dependencies and browsers (Chromium)
RUN .venv/bin/playwright install --with-deps chromium

# Prepare dagster_home and source_code directories
RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/dagster_home/storage /source_code

# Copy configuration
COPY workspace.yaml ./
COPY dagster.yaml /opt/dagster/dagster_home/

# Copy the rest of the project codebase
COPY . .

# Ensure the duckdb file is in the expected absolute location so that
# named volumes and the application code can access it correctly
RUN if [ -f src/orchestration/source_code/db.duckdb ]; then \
        cp src/orchestration/source_code/db.duckdb /source_code/db.duckdb; \
    fi

# Re-sync in case local packages are to be installed
RUN uv sync --frozen

# Ensure uv virtual environment binaries are in the PATH
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"

# Expose webserver port
EXPOSE 3000

# Default command starts the webserver
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
