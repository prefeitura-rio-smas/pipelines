# Imagem base enxuta com Python 3.13
FROM python:3.13-slim

# Instala o uv (versão mais rápida e moderna que Poetry)
ENV UV_VERSION=0.2.25
RUN pip install --no-cache-dir "uv==$UV_VERSION"

WORKDIR /app

# Copia manifests e instala dependências com uv
COPY pyproject.toml /app/
RUN uv pip install --system --no-cache --requirement <(uv pip compile pyproject.toml)

# Copia o resto do código + entrypoint
COPY . /app
COPY queries/profiles.yml /app/queries/profiles.yml
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
