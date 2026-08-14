#!/usr/bin/env bash
set -euo pipefail

REPO="prefeitura-rio-smas/pipelines"
RULESET_NAME="required-checks"

if ! gh auth status >/dev/null 2>&1; then
  echo "gh nao esta autenticado. Rode: gh auth login" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq nao encontrado. Instale jq para continuar." >&2
  exit 1
fi

PAYLOAD_FILE=$(mktemp)
trap 'rm -f "$PAYLOAD_FILE"' EXIT

cat > "$PAYLOAD_FILE" <<'JSON'
{
  "name": "required-checks",
  "enforcement": "active",
  "target": "branch",
  "conditions": {
    "ref_name": {
      "include": ["refs/heads/main", "refs/heads/staging/*"],
      "exclude": []
    }
  },
  "rules": [
    {
      "type": "required_status_checks",
      "parameters": {
        "strict_required_status_checks_policy": false,
        "required_status_checks": [
          { "context": "CI - Security / gitleaks" },
          { "context": "CI - Security / audit" },
          { "context": "CI - Security / hadolint" }
        ]
      }
    }
  ]
}
JSON

EXISTING=$(gh api "repos/$REPO/rulesets" --paginate | jq -r --arg n "$RULESET_NAME" '.[] | select(.name == $n) | .id' 2>/dev/null || true)

if [[ -n "$EXISTING" ]]; then
  echo "Ruleset '$RULESET_NAME' ja existe (id=$EXISTING). Atualizando..."
  gh api -X PUT "repos/$REPO/rulesets/$EXISTING" --input "$PAYLOAD_FILE"
else
  echo "Criando ruleset '$RULESET_NAME'..."
  gh api -X POST "repos/$REPO/rulesets" --input "$PAYLOAD_FILE"
fi
