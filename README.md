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

#### Transformação diária do Prontuário

O deployment existente inclui o Prontuário via `dbt build --select +tag:daily`. A homologação da seleção da Medallion acontece em desenvolvimento, com a busca agendada ativa.

Antes da publicação em desenvolvimento, valide a autorização no ambiente isolado com uma conta de uma única unidade, uma conta sem permissão e uma identidade vazia. Registre o commit e os resultados da homologação. Preserve uma cópia recuperável da publicação atual antes da primeira promoção incremental.

Para a homologação, execute o build como job dbt e use um identificador de execução novo a cada candidata completa. Por exemplo, `schema_id: prontuario_homologacao_001` gera `rj-smas-dev.ci_prontuario_homologacao_001`.

```bash
dbt build --project-dir queries --profiles-dir queries --target ci \
    --vars '{schema_id: prontuario_homologacao_001}' --select +prontuario_eletronico
```

A busca agendada e a Medallion executam o mesmo cálculo em jobs independentes, cada uma lendo os raws e a planilha. A programada publica as colunas legadas em `rj-smas-dev.dashboard_acolherio.dev_atendimentos`. Gere novamente o SQL versionado quando o cálculo ou seus raws mudarem, usando o perfil `ci`:

```bash
dbt compile --project-dir queries --profiles-dir queries --target ci \
    --target-path "$PWD/queries/target" --no-introspect --select +int_acolherio_atendimentos
python queries/scheduled/render_atendimentos.py queries/target/manifest.json \
    > queries/scheduled/atendimentos.sql
```

Para homologar, grave uma cópia autorizada da planilha real em um dataset isolado. Fixe o mesmo timestamp de time travel para os raws e a mesma data de referência nas cópias de teste do SQL. Rode duas execuções de cada pipeline, redirecionando os destinos da programada para tabelas isoladas. Compare chaves, valores de todas as colunas comuns e contagem por linha completa no total e em Centro POP. Exija zero diferenças, inclusive entre as duas execuções de cada pipeline. Valide também esquema, grão pessoa–unidade e ordem dos arrays nas duas OBTs.

Execute a versão anterior da programada sobre a mesma fotografia e registre a variação de população e multiplicidade por profissional, hora, data, nome original, usuário e unidade. Conte trocas de ID, família, CBO e classificação no total e em Centro POP. Para grupos eliminados pelo `QUALIFY`, registre os vencedores dos dois rankings. Mudanças de população nessa transição exigem revisão explícita. A planilha real é requisito para a homologação.

Após o aceite, preserve cópias recuperáveis das duas saídas e do SQL anterior. Atualize o SQL da configuração existente da programada em desenvolvimento e publique a OBT pelo job incremental abaixo. Compare as saídas após a primeira execução independente de cada pipeline. Se divergirem, restaure o SQL anterior e as saídas preservadas.

```bash
dbt run-operation publicar_prontuario_dev --project-dir queries --profiles-dir queries \
    --target dev --args '{schema_candidata: ci_prontuario_homologacao_001}'
```

A operação publica em `rj-smas-dev.abordagem.prontuario_eletronico` e registra a versão da candidata na mesma transação que insere, atualiza e exclui linhas. Para ensaiar a publicação, passe `schema_destino` com o nome de outro dataset isolado `ci_prontuario_*`. Em conflito de atualização concorrente, repita a operação com a mesma candidata após a conclusão da carga mais recente.

#### Planilha Unificada do Centro POP

A fonte da planilha é `prontuario_eletronico`. Use `usuario` para o cadastro atual e `centro_pop.historico_mensal` para o perfil e os eventos da competência. A consulta abaixo abre os atendimentos e mantém seus profissionais aninhados. A integração deve preencher `@email_visualizador` com a identidade autenticada.

```sql
select
    p.id_usuario_unidade,
    p.id_usuario,
    p.id_unidade,
    p.nome_unidade,
    p.usuario,
    mes.* except (id_unidade, nome_unidade, atendimentos, evolucoes, ocorrencias),
    atendimento
from `rj-smas-dev.abordagem.prontuario_eletronico` as p
cross join unnest(p.centro_pop.historico_mensal) as mes
cross join unnest(mes.atendimentos) as atendimento
where
    nullif(trim(@email_visualizador), '') is not null
    and lower(trim(@email_visualizador)) in unnest(split(p.email_filtro, ', '))
```

Conte eventos pela chave `(id_usuario_unidade, id_atendimento_modulo)`. Para oficinas, leia `quantidade_participacoes` uma vez por pessoa, unidade e mês, antes de abrir os atendimentos. Na comparação com a relação anterior, confira também a classificação técnica por CBO, a idade no fechamento, a origem de RG, certidão e NIS e a evidência histórica de saúde.

---

## ⚠️ 4. Solução de Problemas Comuns

*   **Erro de Autenticação**: Seu token pode ter expirado. Rode o `gcloud auth application-default login` novamente.
*   **Erro 'dbt command not found'**: Certifique-se de que o ambiente virtual está ativado no seu terminal ou use `uv run dbt ...`.

---
*Equipe de Dados - RJ SMAS* 
 