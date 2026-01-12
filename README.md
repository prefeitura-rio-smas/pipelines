# 🚀 Onboarding: Projetos de Dados (dbt + Prefect)

Bem-vindo! Este guia vai te ajudar a configurar seu ambiente de desenvolvimento para rodar as pipelines e modelos dbt do projeto **rj-smas**.

## 🛠️ 1. Configuração do Ambiente (Crucial)

Estamos usando **Python 3.13**, que é muito recente e exige alguns ajustes manuais para funcionar. Siga os passos abaixo exatamente na ordem.

### Passo A: Instalar Dependências Específicas
No terminal do seu VS Code, execute:

```bash
# 1. Instala o setuptools (corrige erro de 'distutils' no Python 3.13)
pip install setuptools

# 2. Instala versões específicas do dbt para garantir compatibilidade
pip install --force-reinstall dbt-core==1.7.16 dbt-bigquery==1.7.8
```

### Passo B: Configurar o VS Code
Verifique se a extensão **Power User for dbt** está instalada. 
As configurações necessárias já estão no arquivo `.vscode/settings.json`, apontando para a pasta `/queries`.

> **Dica:** Se a extensão parecer "perdida", pressione `Ctrl + Shift + P` e escolha **"Developer: Reload Window"**.

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