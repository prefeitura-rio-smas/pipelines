# Arquitetura de Dados - SMAS (Medallion)

Este documento descreve a organização e os princípios da arquitetura de dados utilizada no projeto dbt da Secretaria Municipal de Assistência Social (SMAS).

## 1. Princípios Gerais

- **Orientação a Domínio:** A partir da camada `intermediate`, os dados são organizados por domínios de negócio (ex: Social, Território) e não por ferramentas ou dashboards.
- **Blindagem da Fonte:** A camada `raw` é o único ponto de contato com os dados externos, protegendo o resto do pipeline de mudanças de schema na fonte.
- **Unicidade da Verdade:** Lógicas complexas (como critérios de vulnerabilidade ou identificação de atendimento domiciliar) são calculadas uma única vez na camada `intermediate` e reusadas por múltiplos modelos.

## 2. Estrutura de Pastas e Camadas

### `raw/` (Bronze)
- **Objetivo:** Captura e limpeza inicial.
- **Responsabilidades:**
    - Renomeação de colunas (`seqatend` -> `id_atendimento`).
    - Tipagem de dados (`SAFE_CAST`, `PARSE_DATE`).
    - Limpezas básicas (`TRIM`, `UPPER`).
    - Abstração de nomes de sistemas (ex: pasta `prontuario_carioca_assistencia_social`).
- **Materialização:** `view`.

### `intermediate/` (Silver - Preparação)
- **Objetivo:** Lógica modular e auxiliar.
- **Responsabilidades:**
    - Cálculos intermediários e tabelas auxiliares.
    - Modelos que preparam dados para o `core`.
- **Materialização:** `ephemeral`.

### `intermediate/core/` (Silver - Conformed)
- **Objetivo:** Entidades integradas e definitivas (Fatos e Dimensões).
- **Responsabilidades:**
    - Geração de Surrogate Keys.
    - Joins entre tabelas do mesmo domínio.
    - Deduplicação de registros.
    - Definição de Dimensões Conformadas (ex: `dim_usuarios`, `dim_unidades`).
- **Materialização:** `table`.

### `marts/` (Gold)
- **Objetivo:** Consumo final e Dashboards.
- **Responsabilidades:**
    - Agregações de negócio (Somas, Médias, Contagens).
    - Filtros específicos para visões de BI (ex: Relatório RMA).
    - Métricas finais.
- **Materialização:** `table`.

## 3. Estratégia de Histórico e Auditoria

### Snapshots
- Utilizados para entidades onde o sistema de origem não mantém histórico ou onde erros de código podem sobrescrever o passado (ex: Unidades, Evoluções).
- Os modelos de auditoria devem consumir de `snapshots/`, enquanto o fluxo principal consome de `raw/` (a menos que a análise exija o histórico temporal).

## 4. Convenção de Nomenclatura

- **Modelos Raw:** `raw_<sistema>__<tabela>.sql`
- **Modelos Intermediate:** `int_<descricao>.sql`
- **Modelos Core:** `dim_<entidade>.sql` ou `fct_<evento>.sql`
- **Modelos Marts:** `mart_<produto>__<descricao>.sql`

---
*Este documento serve como guia para desenvolvedores e agentes de IA manterem a consistência do repositório.*
