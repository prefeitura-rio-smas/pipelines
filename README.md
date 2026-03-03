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

### Passo C: Configurar o VS Code
1. Abra a pasta do projeto no VS Code.
2. Pressione **`Ctrl + Shift + P`** e selecione **"Python: Select Interpreter"**.
3. Escolha a opção que aponte para o caminho **`.venv/Scripts/python.exe`** (ou similar).
4. Verifique se a extensão **Power User for dbt** está instalada. As configurações necessárias já estão automáticas no arquivo `.vscode/settings.json`.

---

## 🔐 2. Autenticação (Como Logar)

Diferente do ambiente de produção (que usa robôs), aqui no desenvolvimento você usará sua **conta pessoal do Google** (OAuth).

### Passo Único: Login no Terminal
Execute este comando e siga as instruções (copiar link, logar no navegador, colar código):

```bash
# Se estiver rodando localmente na sua máquina:
gcloud auth application-default login

# Se estiver rodando no VS Code Server (sem navegador):
gcloud auth application-default login --no-browser
```

### Testando a Conexão
Para ter certeza que funcionou, rode:
```bash
dbt debug --project-dir queries --profiles-dir queries
```
Se aparecer **"All checks passed!"**, você está pronto! 🚀

---

## ⚡ 3. Como Desenvolver (Dia a Dia)

### Rodando Modelos dbt
Graças à extensão **Power User for dbt**, você não precisa ficar digitando comandos o tempo todo.

1. Abra qualquer arquivo `.sql` na pasta `queries/models/`.
2. Pressione **`Ctrl + Enter`** (ou `Cmd + Enter`).
3. O resultado da query aparecerá na aba lateral **Query Results**.

### Rodando Pipelines (Prefect)
Nossas pipelines são definidas na pasta `pipelines/`. Para rodar ou testar, verifique a documentação específica de cada fluxo ou use o dashboard do Prefect se disponível.

---

## ⚠️ 4. Solução de Problemas Comuns

*   **Erro `No module named 'distutils'`**: Você esqueceu de rodar `pip install setuptools`.
*   **Erro `dbt.adapters.factory`**: Sua versão do dbt está errada. Rode o comando de `pip install --force-reinstall` listado acima.
*   **Erro de Autenticação**: Seu token pode ter expirado. Rode o `gcloud auth ...` novamente.
*   **Extensão não carrega**: Dê um **Reload Window** no VS Code.

---
*Equipe de Dados - RJ SMAS* 