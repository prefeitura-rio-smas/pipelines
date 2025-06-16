# Imagem base enxuta
FROM python:3.10-slim

# Instala libs de sistema que o Poetry e alguns pacotes nativos precisam
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential libffi-dev libssl-dev curl \
 && rm -rf /var/lib/apt/lists/*

# Instala o Poetry no /opt e cria link global
ENV POETRY_HOME=/opt/poetry
RUN curl -sSL https://install.python-poetry.org | python3 - \
 && ln -s $POETRY_HOME/bin/poetry /usr/local/bin/poetry

WORKDIR /app

# Copia só os manifests e já instala as deps via Poetry
COPY pyproject.toml poetry.lock /app/
RUN poetry config virtualenvs.create false \
 && poetry install --no-dev --no-interaction --no-ansi

# Copia o restante do código e seu arquivo de profiles
COPY . /app
COPY queries/profiles.yml /app/queries/profiles.yml

# Entrypoint
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
