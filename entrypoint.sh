#!/usr/bin/env bash
set -euo pipefail           # aborta se qualquer comando falhar

while true; do
  echo "⏱️  $(date) — disparando incremental_flow"
  python -m pipeline.flows incremental_flow
  status=$?

  if [ $status -ne 0 ]; then
    echo "❌  $(date) — fluxo falhou (exit $status). Encerrando contêiner."
    exit $status            # encerra o script ⇒ contêiner para
  fi

  echo "🏁  $(date) — ciclo concluído, dormindo 1 h"
  sleep 3600
done
