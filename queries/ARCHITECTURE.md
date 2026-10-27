> Contrato público atual: `por_assunto_v9_campos_diretos_formularios_texto`; veja a seção de campos diretos e formulários em texto ao final.

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

## 5. Base compartilhada por usuário

O projeto dbt fica em `/opt/pipelines/pipelines/queries`. A tabela
`marts/planilha_unificada/planilha_unificada.sql` mantém uma linha por
`usuario.id_usuario`, com a mesma população de `dim_usuarios`.

A raiz tem seis blocos: `usuario`, `familias`, `projetos_sociais`,
`atendimentos`, `evolucoes` e `escopos`. Dados pessoais, documentos e perfil são agrupados
em usuario. Atendimentos e evoluções são STRUCTs com quantidade INT64 e uma
lista registros. Os demais escalares são STRING; ausências ficam vazias,
contagens sem eventos são 0 e listas sem registros são [].

Os módulos `int_pu_*` são ephemeral. Os joins de nomes de unidades são feitos
por ID antes de agregar listas; a dimensão precisa ser única por ID. IDs de
unidade não são expostos. Permanecem apenas usuário, família e IDs qualificados
por módulo de atendimento/evolução; participações do escopo Centro POP também
preservam id_atividade para distinguir oficinas homônimas.

Os atendimentos preservam as PKs da referência programada; evoluções com data de
cancelamento preenchida são excluídas. Atendimentos mantêm hora HH:MM.
Todas as evoluções não canceladas aparecem uma vez por usuário, inclusive sem
formulário. A descrição legível remove HTML antes de decodificar entidades;
os formulários ficam associados à evolução, preservando conteúdo e ordem.

A agregação por usuário precede os LEFT JOINs da saída. Cadastro, documentos e
nomes são atuais; o histórico e a cobertura dependem de cada origem. Projetos
sociais continuam na regra compartilhada de último cadastro por usuário/projeto.
Serviços familiares se repetem entre membros e não comprovam atendimento pessoal.

A tag planilha_unificada é explícita e não integra +tag:daily. A base permanece
em validação paralela, no schema relatorio em dev. A mart Centro POP existente e
os modelos compartilhados mantêm seus contratos. Produtos Centro POP, CRAS,
CREAS e RMAs definirão competências, populações e regras de indicadores próprias.
Não fazer dim_usuarios nem seus insumos dependerem da base final, evitando ciclos.

O contrato, as consultas atualizadas e as limitações estão em
[Planilha unificada e escopo Centro POP](docs/dimensoes_planilha_unificada.md).

---
*Este documento serve como guia para desenvolvedores e agentes de IA manterem a consistência do repositório.*

### Perfil e histórico Centro POP

`dim_centro_pop` tem uma linha por `id_usuario`, sem STRUCTs, com o último
formulário inteiro por assunto em todas as unidades. Datas internas permanecem
tipadas; listas de encaminhamento são textos distintos e ordenados. Não há
preenchimento de vazios com formulários anteriores nem inferência de acompanhamento
ativo pela presença de PAI. O histórico materializado `int_pu_registros_centro_pop`
conserva as ocorrências datadas e suas regras de extração.

A unificada mantém a população de `dim_usuarios`, cruzando o perfil por LEFT JOIN.
Os 35 campos do perfil ficam diretamente em `escopos.centro_pop`, sem agrupador
`respostas` nem contexto `unidades`; histórico e
participações permanecem integrais. Dados comuns, contagens, CRAS, CREAS e a mart
existente não são reformulados. O contrato `centro_pop_usuario_v3` exige construção
isolada, comparação integral na mesma base congelada e publicação manual com backup
e restauração compensatória. A consulta programada permanece pausada.
Consulte [o contrato e a operação](docs/dimensoes_planilha_unificada.md).

### Complementos do BI na supertabela

O contrato `por_assunto_v7_complementos_bi` adiciona endereço em `usuario.endereco`,
contexto da referência em `atendimentos.registros` e identificadores da linha
profissional em `profissionais`. Mantém os seis blocos e os campos anteriores.
A referência congelada prevalece para os atendidos; o cadastro congelado de
endereços complementa apenas os usuários fora dessa referência, sem preencher
lacunas entre origens. O lote derivado mantém os insumos anteriores imutáveis.
A migração publica apenas supertabela/controle, com backup e restauração.
Consulte [o contrato e a operação](docs/complementos_bi_planilha_unificada.md).

### Centro POP direto no contrato v8

`por_assunto_v8_centro_pop_direto` remove somente o agrupador `respostas` e move
seus 35 campos para `escopos.centro_pop.<campo>`. Os seis blocos, histórico,
participações, 13 complementos do BI e demais dados são preservados. Mantém
31 escalares STRING e quatro ARRAY<STRING>, com ausências vazias. O perfil
Centro POP conserva o contrato v3. A migração compara o conteúdo integral
reconstruindo o formato antigo apenas durante a validação, publica somente
a supertabela/controle com backup e mantém a consulta programada pausada.

A apresentação atual usa `por_assunto_v9_campos_diretos_formularios_texto`: cadastro, documentos e
endereço diretamente em `usuario`; contagens `quantidade_<serviço>` diretamente
nos blocos de eventos; respostas em `formularios.campos` como `ARRAY<STRING>`
no formato `rótulo: valor`. A auditoria mantém os pares originais no intermediário.
Os seis blocos principais e as demais listas permanecem inalterados.
