# One image that runs the whole pipeline the same way everywhere (laptop, CI, Airflow's
# DockerOperator, Kubernetes, AWS ECS ...).
#
#   docker build -t aqi-pipeline .
#   docker run --rm aqi-pipeline sample --days 30                           # offline demo
#   docker run --rm --env-file .env -v "$PWD/keys:/keys:ro" aqi-pipeline run  # real run on Snowflake

FROM python:3.12-slim

# Don't write .pyc files, flush logs immediately, no pip cache in the image
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    AQI_REPO_ROOT=/app

WORKDIR /app

# Install dependencies first (this layer is cached until pyproject.toml changes)
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

# Then the project files dbt needs
COPY dbt ./dbt

# Run as a normal user, not root
RUN useradd --create-home aqi && mkdir -p /app/data /app/site && chown -R aqi /app
USER aqi

ENTRYPOINT ["aqi"]
CMD ["--help"]
