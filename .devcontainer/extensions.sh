#!/usr/bin/env bash
set -euo pipefail
if ! command -v code >/dev/null 2>&1; then
    echo 'VS Code CLI indisponível: reconecte o editor para instalar as extensões fixadas.' >&2
    exit 1
fi
extensions=(
    ms-python.python@2026.4.0
    ms-python.debugpy@2026.6.0
    ms-python.vscode-python-envs@1.40.0
    innoverio.vscode-dbt-power-user@0.66.3
    altimateai.vscode-altimate-mcp-server@0.14.0
    samuelcolvin.jinjahtml@0.20.0
    proxzima.sweetdracula@1.0.9
)
installed="$(code --list-extensions --show-versions)"
for extension in "${extensions[@]}"; do
    if ! grep -Fxiq "$extension" <<< "$installed"; then
        if ! code --install-extension "$extension" --force; then
            echo "Falha ao instalar versão exigida: $extension" >&2
            exit 1
        fi
    fi
done
installed="$(code --list-extensions --show-versions)"
for extension in "${extensions[@]}"; do
    if ! grep -Fxiq "$extension" <<< "$installed"; then
        echo "Versão exigida não confirmada: $extension" >&2
        exit 1
    fi
done
