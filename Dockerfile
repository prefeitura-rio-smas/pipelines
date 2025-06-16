# Imagem base enxuta
FROM python:3.10-slim

# Instala o Poetry (versão fixa p/ reprodutibilidade)
ENV POETRY_VERSION=1.8.2 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_HOME=/opt/poetry
RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

WORKDIR /app

# Copia manifests e instala dependências
COPY pyproject.toml poetry.lock /app/
RUN poetry install --no-interaction --no-ansi --no-dev

# Copia o resto do código + entrypoint
COPY . /app
COPY queries/profiles.yml /app/queries/profiles.yml
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
