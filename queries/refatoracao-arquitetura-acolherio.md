# Refatoração Arquitetura Medallion - Acolherio

> Resumo da conversa com Gemini CLI | 20/04/2026 - 29/04/2026
> Branch: `refactor/medallion-architecture-migration`

---

## Contexto

Os modelos dbt estavam organizados por projeto (`dashboard_acolherio`, `rma`, `bolsa_familia`), cada um com seu próprio `staging/intermediate/mart`. Isso gerava retrabalho e lógicas divergentes — quando `acolherio` e `rma` precisavam das mesmas dimensões (unidades, profissionais), cada um reconstruía do zero. A decisão foi migrar para uma **arquitetura Medallion** organizada por camada e não por projeto.

---

## Decisões Arquiteturais

### Estrutura de Pastas (sem prefixos numéricos)

```
models/
├── raw/
│   └── prontuario_carioca_assistencia_social/
├── intermediate/
│   ├── core/          # dim/fact finais
│   ├── social/        # int_ por domínio
│   └── atividades/
├── marts/
└── old_architecture/  # legado preservado, desabilitado no dbt_project.yml
```

### Princípios por Camada

| Camada | Responsabilidade | Materialização |
|--------|-----------------|----------------|
| **raw** | Único ponto de contato com `source()`. Já faz renomeação de colunas, limpeza e tipagem. Não filtra dados. | `view` |
| **intermediate/social**, **atividades** | Modelos de apoio que unem fontes diferentes (ex: consolidar vulnerabilidades). | `ephemeral` |
| **intermediate/core** | Dimensões e fatos finais com surrogate keys. | `table` |
| **marts** | Tabelas de consumo para dashboards e relatórios. | A definir |

### Decisões de Nomenclatura

- Sistema renomeado de "acolherio" para **prontuario_carioca_assistencia_social** (nome oficial do sistema)
- `_pacientes` vira **`_usuarios`** (faz mais sentido no contexto da Assistência Social)
- Prefixos: `raw_`, `dim_`, `fct_`, `int_`
- Sem prefixos numéricos (`01_`, `02_`) nas pastas

### Decisão sobre Staging

Camada `staging` foi eliminada. A `raw` já absorve essa responsabilidade (renomeação, tipagem, limpeza leve). Se a `raw` faz o trabalho de "tradução", a `staging` vira camada fantasma.

### Decisão sobre Intermediate vs Core

`core` fica **dentro** de `intermediate`. A intermediate é a "cozinha" (modelos auxiliares, ephemeral) e o `core` é o "prato pronto" (dim/fact materializados como table). Isso evita uma pasta bagunçada na intermediate.

### Snapshots

- Usados para histórico (SCD Type 2) no BigQuery
- Materializados em dataset separado (`snapshots`)
- Equipes vizinhas usam como "seguro", não como source primária
- Decisão: adotar snapshots, mas não como feed principal dos modelos

### Fonte de Dados

Dataset oficial: `brutos_acolherio_staging` (Airbyte → BigQuery). Tabelas "filho bastardo" (fontes não-oficiais) identificadas e tratadas separadamente.

---

## Camada Raw (20 modelos)

Todos os modelos em `raw/prontuario_carioca_assistencia_social/`:

### Unidades e Pessoas
| Modelo | Fonte (BigQuery) | Descrição |
|--------|------------------|-----------|
| `raw_unidades` | `gh_unidades` | Cadastro de unidades CRAS/CREAS |
| `raw_usuarios` | `gh_pac_dados` | Cadastro de usuários (ex-pacientes) |
| `raw_usuarios_detalhes` | `gh_cidadao_pac` | Detalhes adicionais dos usuários |
| `raw_usuarios_saude_mental` | `gh_pac_sm` | Dados de saúde mental dos usuários |
| `raw_usuarios_acolhimentos` | `gh_pac_ciclos` | Acolhimentos de usuários |
| `raw_profissionais` | `gh_profissionais` | Cadastro de profissionais |
| `raw_profissionais_ocupacoes` | `gh_prof_ocupacoes` | Ocupações/CBO dos profissionais |
| `raw_cbo` | `gh_cbo` | Tabela de CBO |
| `raw_operadores` | `gh_contas` | Operadores do sistema (contas) |

### Famílias
| Modelo | Fonte (BigQuery) | Descrição |
|--------|------------------|-----------|
| `raw_familias` | `gh_familias` | Cadastro de famílias |
| `raw_membros_familia` | `gh_membros_familia` | Membros de cada família |
| `raw_familias_vulnerabilidades` | `gh_familias_vuln` | Vulnerabilidades das famílias |
| `raw_familias_projetos_sociais` | `gh_famil_projsociais` | Projetos sociais das famílias |
| `raw_familias_servicos_assistenciais` | `gh_famil_servassist` | Serviços assistenciais das famílias |

### Atendimentos e Evoluções
| Modelo | Fonte (BigQuery) | Descrição |
|--------|------------------|-----------|
| `raw_atendimentos_usuarios` | `gh_atend_usuario` | Atendimentos de usuários |
| `raw_atendimentos_familias` | `gh_atend_familia` | Atendimentos de famílias |
| `raw_evolucoes_administrativas` | `gh_evoluadm` | Evoluções administrativas |
| `raw_evolucoes_familias` | `gh_evolufamil` | Evoluções de famílias |
| `raw_evolucoes_usuarios` | `gh_evolupac` | Evoluções de usuários |

### Evoluções: nota importante
Existem 3 tipos de evolução (`gh_evoluadm`, `gh_evolufamil`, `gh_evolupac`). O nome `raw_evolucoes` era ambíguo — foi renomeado para `raw_evolucoes_administrativas`, `raw_evolucoes_familias` e `raw_evolucoes_usuarios`.

---

## Camada Intermediate

### Modelos de Apoio (`social/`)
| Modelo | Descrição |
|--------|-----------|
| `int_vulnerabilidades_agregadas` | Agrega vulnerabilidades das famílias em struct REPEATED |
| `int_servicos_agregados` | Agrega serviços assistenciais e projetos sociais das famílias |
| `int_usuarios_violacoes` | Processa violações de direitos dos usuários a partir dos arquivos .ini |

### Modelos Core (`intermediate/core/`)

#### Dimensões
| Modelo | Descrição | Registros |
|--------|-----------|-----------|
| `dim_usuarios` | Usuários com detalhes, saúde mental, situação de rua e violações de direitos (struct) | ~347k |
| `dim_familias` | Famílias com vulnerabilidades e serviços aggregados em structs REPEATED | - |
| `dim_profissionais` | Profissionais com CBO/ocupações | - |
| `dim_unidades` | Unidades CRAS/CREAS | - |

#### Fatos
| Modelo | Descrição | Registros |
|--------|-----------|-----------|
| `fct_atendimentos` | Atendimentos (UNION ALL de famílias + usuários) | ~527k |
| `fct_evolucoes` | Evoluções (UNION ALL de adm + famílias + usuários) | ~1.3M |

---

## Pontos Chave de Discussão

### Situação de Rua na dim_usuarios
Flag de situação de rua mapeada a partir de `raw_usuarios_saude_mental`. Permite responder perguntas como "total de usuários atendidos em situação de rua" (~4.663 registros).

### Vulnerabilidades e Serviços: struct, não dims separadas
Decisão de não criar `dim_vulnerabilidades` e `dim_servicos_assistenciais` separadas. Em vez disso, usar **STRUCTs REPEATED** na `dim_familias` para:
- Vulnerabilidades (`int_vulnerabilidades_agregadas`)
- Serviços assistenciais (`int_servicos_agregados`)

Motivo: são "etiquetas" da família, não entidades independentes com grão próprio. Isso evita `dim_familia` aparecendo no meio de ambas as propostas.

### Violações de Direitos: struct na dim_usuarios
Processadas via `int_usuarios_violacoes` que faz split dos códigos e cruzamento com arquivos `.ini` do sistema (armazenados em `models/aux/lists/`). Resultado: struct REPEATED na `dim_usuarios` com descrições legíveis (ex: "Negligência e abandono", "Violência financeira").

### Pastas na Intermediate
Organização por **domínio** (`social/`, `atividades/`), não por sistema. Isso permite que modelos intermediários sirvam múltiplos sistemas sem acoplamento.

---

## dbt_project.yml

Configuração das camadas no `dbt_project.yml`:

```yaml
models:
  pipelines:
    +materialized: view
    raw:
      +materialized: view
    intermediate:
      +materialized: ephemeral
      core:
        +materialized: table
    marts:
      +materialized: table
    old_architecture:
      +enabled: false
```

A `old_architecture` está desabilitada (`+enabled: false`) para não conflitar com os novos modelos durante a migração.

---

## Arquivos Auxiliares

- **`_sources.yml`**: Centraliza todas as sources do `brutos_acolherio_staging`
- **`.yml` por modelo**: Documentação 1:1 (um `.yml` para cada `.sql`)
- **`models/aux/lists/`**: Arquivos `.ini` do sistema com mapeamentos de códigos (violações, moradia, etc.). Pasta no `.gitignore`.

---

## Pontos Pendentes

- [ ] Criar modelos **marts** para dashboards e relatórios
- [ ] Migrar sistema **bolsa_familia** para a nova arquitetura
- [ ] Migrar sistema **arcgis** para a nova arquitetura
- [ ] Migrar **rma/cras** para a nova arquitetura
- [ ] Configurar **snapshots** para histórico (SCD Type 2)
- [ ] Revisar e completar `ARCHITECTURE.md`
- [ ] Remover `old_architecture/` quando migração estiver completa
- [ ] Padronizar flags (transformar 'N' em 'Não' na raw para facilitar consumo)