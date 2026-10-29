#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
uv sync --frozen --all-extras
uv run dbt deps --project-dir queries --profiles-dir queries
uv tool install --force 'sqlfluff==4.3.0'
activation="source $(pwd)/.venv/bin/activate"
grep -Fqx "$activation" "$HOME/.bashrc" || printf '\n%s\n' "$activation" >> "$HOME/.bashrc"
path_line='export PATH="$HOME/.local/bin:$PATH"'
grep -Fqx "$path_line" "$HOME/.bashrc" || printf '\n%s\n' "$path_line" >> "$HOME/.bashrc"
alias_line="alias dbtf='uv run dbt'"
grep -Fqx "$alias_line" "$HOME/.bashrc" || printf '\n%s\n' "$alias_line" >> "$HOME/.bashrc"
