# Macros do PR 115 — RMA CRAS

Documentação das macros criadas/alteradas no PR 115
(`feat(rma_cras): converte indicadores RMA CRAS para a nova arquitetura`).

Localização:

- `queries/macros/calc_idade.sql`
- `queries/macros/acolherio/extrair_encaminhamentos.sql`
- `queries/macros/acolherio/familia_servico_unidade.sql`
- `queries/macros/acolherio/mes_referencia.sql`
- `queries/macros/acolherio/nao_cancelado.sql`
- `queries/macros/acolherio/no_mes.sql`
- `queries/macros/acolherio/pool_evolucoes_ficha.sql`

---

## calc_idade

**Arquivo:** `queries/macros/calc_idade.sql`

Idade aniversário-correta na data de referência (expressão SQL retornada como texto).

**Assinatura:**

```
calc_idade(data_nascimento, data_referencia=none)
```

**Parâmetros:**

| parâmetro | tipo | default | descrição |
|---|---|---|---|
| `data_nascimento` | str | — | Coluna/expressão de nascimento. |
| `data_referencia` | str/none | `none` | Data de referência. `none` → `current_date()` (comportamento original). Sentinel `'fim_do_mes'` → `last_day(mes_referencia())`, ou seja, último dia do mês de referência dos RMAs. |

**Racional:** para RMA mensal a idade deve ser calculada no fim do mês de competência, não no dia corrente — o legado usava `current_date()`.

**Exemplo de uso (RMA):**

```sql
select calc_idade('data_nascimento', 'fim_do_mes')
```

SQL emitido para `data_referencia='fim_do_mes'`:

```sql
date_diff(last_day(date_trunc(current_date(), month)), data_nascimento, year)
- case
    when extract(month from last_day(...)) < extract(month from data_nascimento) then 1
    when extract(month from last_day(...)) = extract(month from data_nascimento)
      and extract(day from last_day(...)) < extract(day from data_nascimento) then 1
    else 0
  end
```

---

## extrair_encaminhamentos

**Arquivo:** `queries/macros/acolherio/extrair_encaminhamentos.sql`

Extrai os campos de encaminhamento (SMAS, Benefícios, Órgãos) do HTML das
evoluções. Generalista: serve ao RMA CRAS, ao RMA CREAS e ao pipeline core.

**Assinatura:**

```
extrair_encaminhamentos(source_relation, pass_through_cols, col_html='descricao_evolucao')
```

**Parâmetros:**

| parâmetro | tipo | default | descrição |
|---|---|---|---|
| `source_relation` | str | — | Aceita `ref()` ou nome de CTE. |
| `pass_through_cols` | list | — | Colunas repassadas além das extraídas (obrigatório — `select *` não funciona por causa do subquery wrapper). |
| `col_html` | str | `'descricao_evolucao'` | Coluna com o HTML da evolução. |

**Comportamento:**

1. Remove as tags HTML da coluna (`<[^>]+>` → `;`) e normaliza sequências de `;`.
2. Extrai com regex os campos:
   - `encaminhamento_smas` — `Encaminhamentos - SMAS:` (aceita "Atividades SMAS")
   - `encaminhamento_beneficios` — `Encaminhamentos - Benefícios:`
   - `encaminhamento_orgaos` — `Encaminhamentos Órgãos:`
3. O regex captura apenas o **primeiro item** da seção (`[^;]+?` até `;Encaminhamentos|;Outros|$`). Se houver múltiplos checkboxes marcados, o padrão não casa → retorna `NULL` (subcontagem deliberada, paridade com o legado `int_evolucao.sql`).

**Exemplo de uso (PR 115):**

```sql
select * from {{
    extrair_encaminhamentos(
        'base',
        [
            'id_evolucao_sk',
            'id_usuario_sk',
            'id_unidade_sk',
            'id_unidade',
            'data_evolucao',
            'nome_usuario'
        ]
    )
}}
```

---

## familia_servico_unidade

**Arquivo:** `queries/macros/acolherio/familia_servico_unidade.sql`

Famílias vinculadas a um serviço assistencial × membros ativos, com atribuição
de unidade em **3 níveis** (serviço → operador de cadastro → atendimento mais
recente no tipo de unidade informado).

**Assinatura:**

```
familia_servico_unidade(id_servico, tipo_unidade)
```

**Parâmetros:**

| parâmetro | tipo | descrição |
|---|---|---|
| `id_servico` | int | ID do serviço assistencial (ex.: 1 = PAIF/CRAS, 6 = PAEFI/CREAS). |
| `tipo_unidade` | str | Tipo da unidade a priorizar na atribuição (ex.: `'CRAS'`, `'CREAS'`). |

**Colunas retornadas:** `id_familia`, `data_cadastro_servico`, `id_usuario`,
`data_nascimento`, `beneficio`, `violacoes`, `vulnerabilidades`, `id_unidade`.

**Lógica de atribuição de unidade** (`coalesce(p.id_unidade, ul.id_unidade, af.id_unidade)`):

1. **Serviço** — `raw_familias_servicos_assistenciais.id_unidade` (do vínculo da família ao serviço).
2. **Operador de cadastro** — `raw_operadores_unidades` (do `id_login_cadastro` do vínculo; `min(id_unidade)` por login).
3. **Atendimento mais recente** — de `raw_atendimentos_familias` em unidade do tipo informado (não cancelado; último por `data_atendimento`).

**Notas:** subqueries aninhadas, sem `WITH` (BigQuery não permite `WITH` dentro de `FROM`). Fonte do serviço é filtrada por `data_cancelamento is null`; membros por `data_saida is null`.

**Exemplo de uso:**

```sql
select * from familia_servico_unidade(1, 'CRAS')   -- PAIF
select * from familia_servico_unidade(6, 'CREAS')  -- PAEFI
```

---

## mes_referencia

**Arquivo:** `queries/macros/acolherio/mes_referencia.sql`

Primeiro dia do mês de referência dos RMAs.

**Assinatura:**

```
mes_referencia()
```

**Comportamento:** lê a variável `competencia` (`var('competencia', '')`, formato `AAAA-MM`). Vazia → `date_trunc(current_date(), month)` (mês corrente); preenchida → `date('AAAA-MM-01')`.

**Exemplo:**

```sql
-- competencia=''  -> date_trunc(current_date(), month)
-- competencia='2026-09' -> date('2026-09-01')
select mes_referencia()
```

---

## nao_cancelado

**Arquivo:** `queries/macros/acolherio/nao_cancelado.sql`

Predicado canônico de registro vigente (flag nulo ou diferente de `'S'`).

**Assinatura:**

```
nao_cancelado(col_flag='flag_cancelado')
```

**Exemplo de uso:**

```sql
where nao_cancelado()
where nao_cancelado('a.flag_cancelado')
```

SQL emitido:

```sql
(col_flag is null or col_flag != 'S')
```

---

## no_mes

**Arquivo:** `queries/macros/acolherio/no_mes.sql`

Predicado "coluna cai no mês de referência".

**Assinatura:**

```
no_mes(col_data)
```

**Exemplo de uso:**

```sql
where no_mes('data_atendimento')
```

SQL emitido:

```sql
date_trunc(data_atendimento, month) = mes_referencia()
```

---

## pool_evolucoes_ficha

**Arquivo:** `queries/macros/acolherio/pool_evolucoes_ficha.sql`

Pool de evoluções elegível a RMA: módulo administrativo tipo `'F'` registrado
na aba informada + (opcional) módulo família explodido nos membros ativos
(com surrogate keys resolvidas).

**Assinatura:**

```
pool_evolucoes_ficha(aba_ficha, incluir_familia_membros=true)
```

**Parâmetros:**

| parâmetro | tipo | default | descrição |
|---|---|---|---|
| `aba_ficha` | str | — | Título `<h3>` da aba (ex.: `'CRAS - Ficha de Atendimento Individualizado'`). |
| `incluir_familia_membros` | bool | `true` | Inclui o ramo família (evoluções de módulo família explodidas nos membros ativos). |

**Colunas retornadas:** `id_evolucao_sk`, `id_usuario_sk`, `id_unidade_sk`,
`id_unidade`, `data_evolucao`, `descricao_evolucao`, `id_familia`.

**Ramos:**

1. **Administrativo:** `fct_evolucoes` com `origem_modulo='administrativa'` e `tipo_evolucao='F'`, casando o `<h3>` exato da aba (`regexp_extract(descricao_evolucao, r'<h3>(.*?)</h3>') = aba`).
2. **Família** (se habilitado): `fct_evolucoes` com `origem_modulo='familia'`, unido a `raw_membros_familia` (membros ativos, `data_saida is null`) e `dim_usuarios` para resolução da surrogate key de usuário.

`id_familia` vem da evolução; é `NULL` no ramo administrativo quando a ficha não referencia família — usado como grão família nos indicadores C2/C3/C5 do RMA.

**Exemplo de uso (PR 115):**

```sql
select * from {{
    pool_evolucoes_ficha('CRAS - Ficha de Atendimento Individualizado')
}}
```

---

## Dependências (upstream)

| fonte | papel |
|---|---|
| `var('competencia')` | mês de referência (mes_referencia) |
| `date_trunc`/`current_date` | cálculo do mês |
| `fct_evolucoes` | evoluções (ramo adm + família) |
| `raw_familias_servicos_assistenciais` | vínculo família × serviço |
| `raw_membros_familia` | membros ativos da família |
| `raw_familias_vulnerabilidades` | vulnerabilidades |
| `raw_operadores_unidades` | unidade do operador de cadastro |
| `raw_atendimentos_familias` | atribuição de unidade (nível 3) |
| `dim_usuarios` | usuário, nascimento, benefícios, violações |
| `dim_unidades` | tipo de unidade |

## Considerações de paridade / riscos registrados

- `extrair_encaminhamentos`: regex captura só o primeiro item da seção → subcontagem em fichas com múltiplos encaminhamentos marcados (paridade com o legado).
- `pool_evolucoes_ficha`: match do `<h3>` é exato — sensível a variação de whitespace/case no HTML.
- `familia_servico_unidade`: atribuição de unidade em 3 níveis é mais ampla que o legado (só operador) — pode deslocar séries A/B.
- `calc_idade` com `'fim_do_mes'`: difere do legado (`current_date()`) — correto para RMA mensal, mas quebra paridade de B5/D por alguns dias.