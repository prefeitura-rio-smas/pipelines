# Documento Técnico: Arquitetura de CI/CD para Pipelines ArcGIS

## 1. Visão Geral e Objetivos

Este documento descreve a arquitetura de Continuous Integration/Continuous Deployment (CI/CD) para os pipelines de integração com ArcGIS. O objetivo principal é permitir a distinção clara entre ambientes de **Staging** e **Produção** utilizando uma única infraestrutura de execução Prefect self-hosted, minimizando a duplicação de código e garantindo a segurança e estabilidade dos deploys.

A abordagem visa:
*   **Isolamento de Ambientes:** Garantir que pipelines de Staging e Produção interajam com recursos (BigQuery, GCS, ArcGIS) específicos de cada ambiente.
*   **Reproducibilidade:** Utilizar containers Docker para encapsular as dependências e o código dos pipelines.
*   **Automação:** Utilizar GitHub Actions para automatizar o build, push da imagem Docker e o deploy dos flows no Prefect Server.
*   **Reuso:** Maximizar o reuso do código-fonte dos pipelines entre os ambientes.
*   **Estabilidade:** Manter o Prefect Worker estável e resiliente a falhas temporárias.

## 2. Infraestrutura Prefect Self-Hosted

Nosso ambiente Prefect é auto-hospedado utilizando Docker Compose, conforme o arquivo `prefect/compose.yml`.

### 2.1. Componentes Principais:
*   **Prefect Server (prefect-api, prefect-background):** Responsáveis por orquestrar os flows, gerenciar metadados, agendamentos e o estado das execuções. Acessível via `https://prefect-rj-smas.org/api`.
*   **Base de Dados (postgres, redis):** Armazenam os metadados do Prefect Server e gerenciam o cache/mensageria.
*   **Proxy Reverso (traefik):** Direciona o tráfego externo para os serviços internos do Prefect Server.

### 2.2. Prefect Worker (`prefect-worker`):
O worker é o componente responsável por executar os flows. Sua configuração é crucial para a estratégia de ambientes:
*   **Tipo:** `docker`. Isso permite que o worker inicie novos containers para cada flow run, utilizando imagens Docker específicas para o código do pipeline.
*   **Pool:** `docker-pool`. Esta work pool é configurada no Prefect Server e o worker fica "escutando" por jobs atribuídos a ela.
*   **Acesso ao Docker Host:** O worker tem o volume `/var/run/docker.sock` montado, permitindo que ele se comunique com o daemon Docker do host para criar e gerenciar os containers dos flow runs.
*   **Instalação de Dependências:** O comando de inicialização do worker (`bash -c "pip install prefect-docker && prefect worker start..."`) garante que as dependências necessárias para o worker Docker sejam instaladas no momento da inicialização do container, sem a necessidade de uma imagem customizada para o worker em si.
*   **Desativação de Eventos (Temporário):** `PREFECT_EVENTS_ENABLED: "false"` e `PREFECT_API_ENABLE_HTTP2: "false"` foram definidos para mitigar problemas com WebSockets do Traefik, priorizando a estabilidade do worker. Isso significa que os logs em tempo real na UI podem não funcionar, mas os logs serão persistidos e visíveis após a conclusão do flow run.

## 3. Fluxo de CI/CD (GitHub Actions)

O pipeline de CI/CD será acionado por pushs nas branches `main` (Produção) e `staging/**` (Staging).

1.  **Testes (Job `test`):** Executa testes de unidade, linting e checagens de qualidade do código.
2.  **Smoke Test DBT (Job `smoke-dbt`):** Compila os modelos DBT para garantir a sintaxe e a integridade.
3.  **Build e Push da Imagem Docker (Job `build-push`):**
    *   Constrói uma imagem Docker do repositório contendo todo o código dos pipelines.
    *   A imagem é taggeada dinamicamente com base na branch e no SHA do commit (ex: `ghcr.io/seu-org/pipelines-arcgis:staging-abcdef123` ou `ghcr.io/seu-org/pipelines-arcgis:main-abcdef123`).
    *   A imagem é enviada para um Container Registry (GHCR).
4.  **Deploy dos Flows (Job `deploy-flows` - a ser implementado):**
    *   **Acesso ao Prefect Server:** O GitHub Actions se autentica com o Prefect Server usando `PREFECT_API_URL` e `PREFECT_API_KEY` (secrets do GitHub).
    *   **Definição do Ambiente (`MODE`):** Uma variável de ambiente `MODE` é setada como `staging` ou `prod` dependendo da branch que acionou o workflow.
    *   **Criação/Atualização de Deployments:** O comando `prefect deploy` utiliza o arquivo `prefect.yaml` (a ser criado na raiz do repositório) para registrar ou atualizar os deployments dos flows no Prefect Server. Este comando injeta a tag da imagem Docker recém-construída e a variável `MODE` na configuração do deployment.

## 4. Estratégia de Ambientes (RIO_ENV)

A chave para o isolamento entre Staging e Produção é a variável de ambiente **`MODE`**.

### 4.1. No Código do Pipeline (`pipelines/arcgis/pipelines/arcgis/constants.py`):
*   O arquivo `constants.py` será refatorado para ler a variável `MODE` (que pode ser `dev`, `staging` ou `prod`).
*   Com base em `MODE`, o código determinará qual projeto GCP, bucket GCS, dataset BigQuery ou endpoint de ArcGIS deve ser utilizado.
*   Para desenvolvimento local, `MODE` será definido como `dev` via um arquivo `.env`.

### 4.2. No Prefect Deployment (`prefect.yaml`):
*   O `prefect.yaml` definirá dois deployments para cada flow (ex: `abordagem-staging` e `abordagem-prod`).
*   Cada deployment especificará a imagem Docker a ser utilizada (com a tag correta) e injetará a variável de ambiente `MODE` no container que será rodado pelo worker.

**Exemplo Conceitual:**

```yaml
# prefect.yaml
deployments:
  - name: abordagem-staging
    entrypoint: pipelines/arcgis/abordagem/flows.py:abordagem_flow
    work_pool:
      name: docker-pool
      job_variables:
        image: ghcr.io/seu-org/pipelines-arcgis:staging-{{ github.sha }}
        env:
          RIO_ENV: staging # Injeta a variável para o container
  - name: abordagem-prod
    entrypoint: pipelines/arcgis/abordagem/flows.py:abordagem_flow
    work_pool:
      name: docker-pool
      job_variables:
        image: ghcr.io/seu-org/pipelines-arcgis:main-{{ github.sha }}
        env:
          RIO_ENV: prod # Injeta a variável para o container
```

Dessa forma, o mesmo código binário (dentro da imagem Docker) se comporta de maneira diferente dependendo do deployment e do ambiente para o qual foi registrado.

## 5. Próximos Passos (Roadmap)

Para solidificar esta arquitetura, as seguintes tarefas precisam ser implementadas:

1.  **Refatorar `pipelines/arcgis/pipelines/arcgis/constants.py`:**
    *   Implementar a lógica de leitura de `MODE` e mapeamento de configurações (GCP Project, GCS Bucket, BigQuery Dataset) para `dev`, `staging` e `prod`.
2.  **Criar `prefect.yaml`:**
    *   Definir a estrutura do `prefect.yaml` na raiz do repositório `pipelines/arcgis`, incluindo as seções `build`, `push`, `pull` (se necessário) e `deployments` para os flows existentes (abordagem, cartao_primeira_infancia_carioca, cras_cas_poligonos, equipamentos, gestao_vagas, limite_bairros_25).
    *   Garantir que os `job_variables` de cada deployment injetem a `image` correta e a `MODE` apropriada.
3.  **Atualizar GitHub Actions (`.github/workflows/ci_cd.yml`):**
    *   Adicionar o job de `deploy-flows` que executa o comando `prefect deploy` para os ambientes de staging e produção, passando os parâmetros corretos para o `prefect.yaml` (especialmente a tag da imagem).
    *   Configurar os secrets necessários (`PREFECT_API_KEY`).
4.  **Testar o Pipeline Completo:**
    *   Realizar um deploy de teste para a branch `staging/**` e verificar se o flow é registrado corretamente e se interage com os recursos de Staging.
    *   Realizar um deploy de teste para a branch `main` e verificar se o flow é registrado corretamente e se interage com os recursos de Produção.
    *   Executar um flow run e verificar se o worker consegue pegar o job e executá-lo sem problemas.

Este roadmap nos guiará na implementação completa da arquitetura proposta.