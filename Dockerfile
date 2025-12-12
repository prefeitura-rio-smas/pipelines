# Imagem base enxuta com Python 3.13
FROM python:3.13-slim

# Instala o uv (versão mais rápida e moderna que Poetry)
ENV UV_VERSION=0.2.25
RUN pip install --no-cache-dir "uv==$UV_VERSION"

WORKDIR /app

# Copia manifests e instala dependências com uv
COPY pyproject.toml /app/
RUN uv pip compile pyproject.toml -o requirements.txt && \
    uv pip install --system --no-cache -r requirements.txt

# Copia o resto do código
COPY . /app
COPY queries/profiles.yml /app/queries/profiles.yml

# Configura variável de ambiente do DBT
ENV DBT_PROFILES_DIR=/app/queries

# Não define ENTRYPOINT, permitindo que o Prefect (ou docker run) controle o comando
