# 🚀 Onboarding: Projetos de Dados (dbt + Prefect)

Bem-vindo! Este guia vai te ajudar a configurar seu ambiente de desenvolvimento para rodar as pipelines e modelos dbt do projeto **rj-smas**.

## 🛠️ 1. Configuração do Ambiente (Modo Simples com `uv`)

Para garantir que todos usem as mesmas versões de Python e dbt sem conflitos, usamos o **[uv](https://docs.astral.sh/uv/)**. 

### Passo A: Instalar o `uv`
Se você ainda não tem o `uv`, instale-o com um destes comandos no seu terminal:

*   **Windows (PowerShell):** `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`
*   **Linux/macOS:** `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Passo B: Sincronizar o Ambiente
Na pasta raiz do projeto, execute:
```bash
uv sync
```
Este comando criará uma pasta `.venv/` com o Python 3.13 e todas as bibliotecas (dbt, etc.) configuradas automaticamente.

### Passo C: Configurar o Editor (VS Code ou outro)
1. Abra a pasta do projeto no seu editor de preferência.
2. Certifique-se de que o editor está usando o interpretador Python localizado na pasta **`.venv/`** que o `uv` criou. No VS Code, isso geralmente acontece automaticamente, mas você pode forçar pressionando `Ctrl + Shift + P` -> `Python: Select Interpreter`.

---

## 🔐 2. Autenticação (Como Logar)

Diferente do ambiente de produção (que usa robôs), aqui no desenvolvimento você usará sua **conta pessoal do Google** (OAuth).

### Passo Único: Login no Terminal
Execute este comando e siga as instruções (abrir link no navegador e logar):

```bash
gcloud auth application-default login
```

### Testando a Conexão
Para ter certeza que o dbt está configurado corretamente, rode:
```bash
dbt debug --project-dir queries --profiles-dir queries
```
Se aparecer **"All checks passed!"**, você está pronto! 🚀

---

## ⚡ 3. Como Desenvolver (Dia a Dia)

### Rodando Modelos dbt
Você pode rodar os modelos dbt manualmente pelo terminal:
```bash
# Exemplo: rodar todos os modelos da pasta pic
dbt run --select pic --project-dir queries --profiles-dir queries
```

### Rodando Pipelines (Prefect)
Nossas pipelines são definidas na pasta `pipelines/`. Para rodar ou testar localmente, certifique-se de que seu ambiente virtual (`.venv`) está ativado.

---

## ⚠️ 4. Solução de Problemas Comuns

*   **Erro de Autenticação**: Seu token pode ter expirado. Rode o `gcloud auth application-default login` novamente.
*   **Erro 'dbt command not found'**: Certifique-se de que o ambiente virtual está ativado no seu terminal ou use `uv run dbt ...`.

---
*Equipe de Dados - RJ SMAS* 
 