#!/usr/bin/env bash
set -e

while true; do
  echo "⏱️  $(date) — disparando incremental_flow"
  python -m pipeline.flows incremental_flow
  echo "🏁  $(date) — ciclo concluído, dormindo 1 h"
  sleep 3600
done
