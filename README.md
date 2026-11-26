# 🚀 Onboarding: Projetos de Dados (dbt + Prefect)

Bem-vindo! Este guia vai te ajudar a configurar seu ambiente de desenvolvimento para rodar as pipelines e modelos dbt do projeto **rj-smas**.

## ⚡ Opção rápida: GitHub Codespaces

Abra o repositório no GitHub em **Code → Codespaces**: o ambiente é configurado automaticamente (uv sync + dbt deps). O único passo manual é autenticar:

```bash
gcloud auth application-default login
```

Valide com `dbt debug --project-dir queries --profiles-dir queries` — deve retornar **"All checks passed!"**.

---

*O restante deste guia descreve o fluxo manual (instalação local de uv, gcloud e configuração do editor).*

## 🛠️ 1. Configuração do Ambiente (Modo Simples com `uv`)

Para garantir que todos usem as mesmas versões de Python e dbt sem conflitos, usamos o **[uv](https://docs.astral.sh/uv/)**. 

### Passo A: Instalar o `uv`
Se você ainda não tem o `uv`, execute este comando no seu terminal (**PowerShell**):

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
*(O parâmetro `-ExecutionPolicy Bypass` garante que a instalação funcione mesmo em máquinas Windows com políticas restritivas).*

### Passo B: Instalar o Google Cloud SDK (Sem Admin)
Se você está no Windows e não tem acesso administrador, execute estes comandos no seu terminal (**PowerShell**) para uma instalação rápida e isolada na sua pasta de usuário:

```powershell
# 1. Baixar o instalador (ZIP)
curl.exe -L "https://dl.google.com/dl/cloudsdk/channels/rapid/google-cloud-sdk-windows-x86_64-bundled-python.zip" -o "$env:USERPROFILE\gcloud.zip"

# 2. Extrair os arquivos na sua pasta de usuário
Expand-Archive -Path "$env:USERPROFILE\gcloud.zip" -DestinationPath "$env:USERPROFILE" -Force

# 3. Rodar o instalador (Aceite 'Y' para todas as perguntas, especialmente para o PATH)
& "$env:USERPROFILE\google-cloud-sdk\install.bat"
```
**Importante:** Após o término, **feche e abra o VS Code** para que os novos comandos sejam reconhecidos pelo Windows.

### Passo C: Sincronizar o Ambiente
Na pasta raiz do projeto, execute:
```bash
uv sync
```
Este comando criará uma pasta `.venv/` com o Python 3.13 e todas as bibliotecas (dbt, etc.) configuradas automaticamente.

### Passo D: Configurar o Editor (VS Code ou outro)
1. Abra a pasta do projeto no seu editor de preferência.
2. Certifique-se de que o editor está usando o interpretador Python localizado na pasta **`.venv/`** que o `uv` criou. No VS Code, isso geralmente acontece automaticamente, mas você pode forçar pressionando `Ctrl + Shift + P` -> `Python: Select Interpreter`.

---

## 🔐 2. Autenticação (Como Logar)

Diferente do ambiente de produção (que usa robôs), aqui no desenvolvimento você usará sua **conta pessoal do Google** (OAuth).

### Passo Único: Login no Terminal
Execute este comando e siga as instruções (abrir link no navegador e logar):

```bash
gcloud auth application-default login --project rj-smas-dev
```
*(O parâmetro `--project` evita erros de cota e permissão durante o uso do dbt).*

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

#### Transformação diária e Prontuário em staging

O deployment `dbt-transform-daily-prod` executa `dbt build --select +tag:daily` diariamente às 10h (São Paulo) e, após o sucesso do build, publica o Prontuário em `rj-smas.abordagem.prontuario_eletronico`. A primeira publicação cria a tabela após os controles da candidata. O deployment `dbt-transform-staging` é manual.

O modelo intermediário `int_atendimentos_selecionados` seleciona os atendimentos e profissionais a partir dos raws do AcolheRio e da planilha de permissões. O flow materializa a seleção no dataset isolado e a usa para montar e reconciliar o Prontuário.

Para homologar a troca de fonte, execute o deployment de staging com `atualizar_prontuario=true` e `publicar_prontuario=false`. O artefato `prontuario-staging` registra controles, diferenças de volume e IDs dos jobs. Compare a seleção dbt com a programada usando a mesma fotografia dos raws, a mesma cópia da planilha e a mesma data de referência. Confira esquema, identidade e multiplicidade por usuário, atendimento e profissional, além dos endereços, classificações, projetos, permissões e cartões publicados. Use a programada como referência da comparação e mantenha seu agendamento ativo no BigQuery. A seleção dbt replica os rankings publicados. Empates sem ordenação completa podem produzir vencedores diferentes entre execuções.

Para publicar em `rj-smas-dev.abordagem.prontuario_eletronico`, habilite também `publicar_prontuario=true`. O worker precisa de permissões de escrita e de criação de snapshots. A promoção é transacional e o snapshot de recuperação dura sete dias. O flow remove o dataset após verificar a publicação. As tabelas de candidatas que ficam no dataset expiram em 24 horas.

O flow valida e publica diretamente a mart dbt `prontuario_eletronico`, no grão de uma linha por cidadão e unidade. A promoção exige esquema compatível, chave `id_usuario_unidade` única e preservação dos atendimentos e das permissões.

Se a execução falhar antes de criar o job de publicação, use um novo run para reconstruir a candidata. Se o job já existir, repita o mesmo run para recuperar o resultado.

---

## ⚠️ 4. Solução de Problemas Comuns

*   **Erro de Autenticação**: Seu token pode ter expirado. Rode o `gcloud auth application-default login` novamente.
*   **Erro 'dbt command not found'**: Certifique-se de que o ambiente virtual está ativado no seu terminal ou use `uv run dbt ...`.

---
*Equipe de Dados - RJ SMAS* 
 