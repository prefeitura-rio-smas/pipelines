#!/usr/bin/env bash
# Não use "set -e" para podermos tratar erros manualmente
set -uo pipefail

SUCCESS_DELAY="${SUCCESS_DELAY:-3600}"   # aguarda 1 h após sucesso
RETRY_DELAY="${RETRY_DELAY:-3600}"       # aguarda 1 h após falha (pode mudar via -e)
LOG_TAG="pipeline"

while true; do
  echo "⏱️  $(date) [$LOG_TAG] — disparando incremental_flow"

  # Executa o fluxo; $? guarda o exit code
  if python -m pipeline.flows incremental_flow; then
    echo "✅  $(date) [$LOG_TAG] — sucesso. Dormindo ${SUCCESS_DELAY}s…"
    sleep "$SUCCESS_DELAY"
  else
    status=$?
    echo "❌  $(date) [$LOG_TAG] — falhou (exit $status). Retentativa em ${RETRY_DELAY}s…"
    sleep "$RETRY_DELAY"
  fi
done
