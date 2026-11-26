# Imagem base enxuta com Python 3.13
FROM python:3.13-slim

# uv na mesma versão usada no CI
ARG UV_VERSION=0.12.4
RUN pip install --no-cache-dir "uv==$UV_VERSION"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app/queries

WORKDIR /app

# Usuário non-root (o Prefect/dbt não precisam de root)
RUN useradd --create-home --uid 10001 appuser

# Dependências a partir do lockfile (reprodutível, igual ao CI)
COPY pyproject.toml uv.lock /app/
RUN uv export --frozen --no-dev --no-emit-project -o requirements.txt && \
    uv pip install --system --no-cache -r requirements.txt && \
    rm -f requirements.txt

# Código (o .dockerignore exclui .git/.env/.venv/target/logs)
COPY --chown=appuser:appuser . /app

# dbt deps antes de trocar para non-root; depois garante escrita em logs/target
RUN dbt deps --project-dir queries --profiles-dir queries && \
    mkdir -p queries/logs queries/target && \
    chown -R appuser:appuser /app

USER appuser

# Não define ENTRYPOINT, permitindo que o Prefect (ou docker run) controle o comando
