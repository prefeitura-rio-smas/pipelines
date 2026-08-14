# 🚀 Desenvolvimento: rj-smas Pipelines (dbt + Prefect)

Bem-vindo(a)! Este guia te leva do zero a um ambiente de desenvolvimento pronto
para rodar as pipelines e os modelos dbt do projeto **rj-smas** — usando
**GitHub Codespaces** (devcontainer), sem instalar nada na sua máquina.

> Se você é da equipe de geógrafas: o ambiente já vem configurado.
> Você só precisa de 1 login manual (passo 2) e já pode rodar os modelos.

---

## 🏠 1. Abrir o Codespaces

1. Acesse o repositório no GitHub: `prefeitura-rio-smas/pipelines`.
2. Clique em **Code ▾ → Codespaces → Create codespace on <branch>**.
3. Aguarde o container ser criado. O `postCreateCommand` roda sozinho e:
   - executa `uv sync --all-extras --dev` (instala Python 3.13 + dbt + todas as
     dependências na pasta `.venv/`);
   - executa `dbt deps` (baixa os pacotes do dbt);
   - ativa o `.venv` automaticamente em todos os terminais novos.
4. Quando abrir o terminal, você deve ver o prefixo `(venv)` no prompt —
   significa que o ambiente já está ativo.

**Pronto, ambiente no ar. Nada mais para instalar.**

---

## 🔐 2. Autenticação no BigQuery (único passo manual)

Diferente da produção (robôs), aqui você usa sua **conta pessoal do Google** (OAuth).
Rode no terminal do codespace:

```bash
gcloud auth application-default login --project rj-smas-dev
```

Siga o link no navegador e autorize. *(O `--project` evita erros de cota e permissão.)*

O container já tenta abrir esse login sozinho ao iniciar (se não houver
credencial válida); se não abrir, rode o comando manualmente.

Esse login fica salvo no codespace; se o token expirar, repita o comando.
**Atenção:** um *rebuild* do codespace (ou prebuild expirado) limpa o login —
basta rodar o comando acima de novo.

---

## ✅ 3. Verificando que está tudo certo

```bash
dbt debug --project-dir queries --profiles-dir queries
```

Se aparecer **"All checks passed!"**, você está pronto! 🚀

---

## ⚡ 4. Dia a dia

### Rodando modelos dbt

```bash
# Exemplo: rodar todos os modelos da pasta pic
dbt run --select pic --project-dir queries --profiles-dir queries
```

### Rodando pipelines (Prefect)

As pipelines ficam em `pipelines/`. O `.venv` já está ativo, então use os
comandos direto no terminal.

### Ferramentas que já vêm no container

| Ferramenta | Uso |
|---|---|
| `uv` | Gerenciador de pacotes (Python 3.13) |
| `gcloud` | SDK do Google Cloud (autenticação) |
| `dbt` | Modelagem de dados (via `.venv`) |
| `python` | Interpretador do projeto (via `.venv`) |

### Extensões do VS Code já instaladas

- **dbt Power User** — autocomplete, preview, lineage e validação de SQL nos
  modelos dbt (rodar modelos/testes com 1 clique);
- **Python** — IntelliSense, testes (pytest) e debug;
- **Google Cloud Code** — explorar datasets/tabelas do BigQuery;
- **GitHub Pull Requests** — revisar e abrir PRs sem sair do editor;
- **Ruff** — linting do código Python.

---

## ⚠️ 5. Solução de Problemas

*   **Erro de autenticação**: seu token expirou. Rode `gcloud auth application-default login --project rj-smas-dev` novamente.
*   **Terminal sem `(venv)`**: abra um terminal novo ou rode `source .venv/bin/activate`.
*   **Erro 'dbt command not found'**: garanta que o `.venv` está ativo ou use `uv run dbt ...`.

---

*Equipe de Dados - RJ SMAS*
