# Linhagem dos encaminhamentos do Centro POP

Este documento descreve o código do checkout atual. Não foi feita consulta aos dados do BigQuery para confirmar preenchimento ou cobertura por módulo.

## Caminho resumido

| Fonte no dataset `rj-smas.brutos_acolherio_staging` | Raw | Campo HTML → alias |
|---|---|---|
| `gh_evolupac` | `raw_evolucoes_usuarios` | `dscevopac` → `descricao_evolucao` |
| `gh_evolufamil` | `raw_evolucoes_familias` | `dscevopac` → `descricao_evolucao` |
| `gh_evoluadm` | `raw_evolucoes_administrativas` | `dscevopac` → `descricao_evolucao` |
| `gh_evolugrupo` | `raw_evolucoes_grupo` | `dscevogrp` → `descricao_evolucao` |

```text
Raw: os quatro modelos acima
  → int_evolucoes
  → int_evolucoes_por_usuario
  → int_formularios_por_usuario
  → int_registros_centro_pop
  → int_perfil_centro_pop
  → centro_pop (alias: dim_centro_pop)
  → prontuario_eletronico (alias: planilha_unificada)
```

Campos finais na tabela principal:

- `centro_pop.encaminhamentos_pai`: STRING.
- `centro_pop.encaminhamentos_atendimento_social`: STRING.

## Código por camada

Os blocos abaixo são trechos literais dos arquivos vinculados. Trechos parciais não são consultas autônomas.

### Raw — usuarios

Arquivo: [raw_evolucoes_usuarios.sql](models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql).

```sql
-- Camada Raw: Evoluções do módulo Usuário
with source as (
    select
        seqpac as id_usuario,
        seqevopac as id_evolucao,
        seqatend as id_atendimento,
        sequs as id_unidade,
        seqprof as id_profissional,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        dtcancpac as data_cancelamento,
        codabapac as codigo_abrangencia
    from {{ source('brutos_acolherio_staging', 'gh_evolupac') }}
)

select * from source
```

Mapeia o HTML da evolução e os identificadores da fonte, sem interpretar os encaminhamentos.

### Raw — familias

Arquivo: [raw_evolucoes_familias.sql](models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql).

```sql
-- Camada Raw: Evoluções do módulo Família
with source as (
    select
        seqfamil as id_familia,
        seqevopac as id_evolucao,
        seqatend as id_atendimento,
        sequs as id_unidade,
        seqprof as id_profissional,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        dtcancpac as data_cancelamento,
        codabapac as modulo_prontuario,
        seqpac as id_paciente
    from {{ source('brutos_acolherio_staging', 'gh_evolufamil') }}
)

select * from source
```

Mapeia o HTML da evolução e os identificadores da fonte, sem interpretar os encaminhamentos.

### Raw — administrativas

Arquivo: [raw_evolucoes_administrativas.sql](models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql).

```sql
-- Camada Raw: Evoluções administrativas dos pacientes
with source as (
    select
        seqpac as id_paciente,
        seqevopac as id_evolucao,
        sequs as id_unidade,
        seqlogin as id_login,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        dtcancpac as data_cancelamento
    from {{ source('brutos_acolherio_staging', 'gh_evoluadm') }}
)

select * from source
```

Mapeia o HTML da evolução e os identificadores da fonte, sem interpretar os encaminhamentos.

### Raw — grupo

Arquivo: [raw_evolucoes_grupo.sql](models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql).

```sql
-- Camada Raw: Evoluções de atividades de grupo
with source as (
    select
        seqevogrp as id_evolucao_grupo,
        seqevogrp as id_evolucao,
        seqatigrp as id_atividade,
        seqprof as id_profissional,
        dtevogrp as data_evolucao,
        dscevogrp as descricao_evolucao,
        codabagrp as codigo_abrangencia,
        indtpevogrp as tipo_evolucao,
        dtcancgrp as data_cancelamento,
        dsclistpac as lista_pacientes,
        codcboprof as cbo_profissional,
        dscrefpres as referencia_presenca
    from {{ source('brutos_acolherio_staging', 'gh_evolugrupo') }}
)
select * from source
```

Mapeia o HTML da evolução e os identificadores da fonte, sem interpretar os encaminhamentos.

### Intermediate — união das evoluções

Arquivo: [int_evolucoes.sql](models/intermediate/core/int_evolucoes.sql).

```sql
uniao as (
    select
        id_paciente as id_usuario,
        id_unidade,
        id_login as id_profissional,
        data_evolucao,
        descricao_evolucao,
        tipo_evolucao,
        origem_modulo,
        null as id_familia,
        null as modulo_prontuario,
        null as codigo_abrangencia,
        null as id_paciente_familia,
        null as id_evolucao_grupo,
        id_evolucao,
        null as id_atividade,
        cast(data_cancelamento as datetime) as data_cancelamento
    from adm
    union all
    select
        null as id_usuario,
        id_unidade,
        id_profissional,
        data_evolucao,
        descricao_evolucao,
        tipo_evolucao,
        origem_modulo,
        id_familia,
        modulo_prontuario,
        modulo_prontuario as codigo_abrangencia,
        id_paciente as id_paciente_familia,
        null as id_evolucao_grupo,
        id_evolucao,
        null as id_atividade,
        cast(data_cancelamento as datetime) as data_cancelamento
    from fam
    union all
    select
        id_usuario,
        id_unidade,
        id_profissional,
        data_evolucao,
        descricao_evolucao,
        tipo_evolucao,
        origem_modulo,
        null as id_familia,
        null as modulo_prontuario,
        codigo_abrangencia,
        null as id_paciente_familia,
        null as id_evolucao_grupo,
        id_evolucao,
        null as id_atividade,
        cast(data_cancelamento as datetime) as data_cancelamento
    from usu
    union all
    select
        id_usuario,
        id_unidade,
        id_profissional,
        data_evolucao,
        descricao_evolucao,
        tipo_evolucao,
        origem_modulo,
        null as id_familia,
        null as modulo_prontuario,
        codigo_abrangencia,
        null as id_paciente_familia,
        id_evolucao_grupo,
        id_evolucao,
        id_atividade,
        cast(data_cancelamento as datetime) as data_cancelamento
    from grupo
)

select * from uniao
```

Une os quatro módulos. Na família, mantém o paciente explicitamente indicado. O ramo grupo, preparado antes desta união, expande a lista de pacientes e obtém a unidade pela atividade.

### Intermediate — evoluções por cidadão

Arquivo: [int_evolucoes_por_usuario.sql](models/intermediate/core/int_evolucoes_por_usuario.sql).

```sql
{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Evoluções familiares só pertencem ao paciente explicitamente indicado.
-- Não converte id_familia em id_usuario nem distribui registros aos membros.
select distinct
    e.id_evolucao,
    e.origem_modulo,
    e.id_familia,
    e.id_unidade,
    un.nome_unidade,
    {{ pu_tipo_servico('un.tipo_unidade') }} as tipo_servico,
    e.id_profissional,
    e.codigo_abrangencia,
    e.id_atividade,
    e.data_evolucao,
    e.data_cancelamento,
    e.tipo_evolucao,
    e.descricao_evolucao,
    a.nome_atividade,
    case
        when e.origem_modulo = 'administrativa' then o.nome_operador
        else p.nome
    end as nome_profissional,
    coalesce(e.id_usuario, e.id_paciente_familia) as id_usuario,
    concat(e.origem_modulo, ':', cast(e.id_evolucao as string)) as id_evolucao_modulo
from {{ ref('int_evolucoes') }} as e
left join {{ ref('dim_profissionais') }} as p
    on e.origem_modulo != 'administrativa' and e.id_profissional = p.id_profissional
left join {{ ref('raw_operadores') }} as o
    on e.origem_modulo = 'administrativa' and e.id_profissional = o.id_login
left join {{ ref('raw_atividades_grupo') }} as a on e.id_atividade = a.id_atividade
left join {{ ref('dim_unidades') }} as un on e.id_unidade = un.id_unidade
where
    coalesce(e.id_usuario, e.id_paciente_familia) is not null
    and e.data_cancelamento is null
```

Define o cidadão por coalesce(id_usuario, id_paciente_familia), cria a chave módulo:evolução e exclui cancelamentos. Não distribui evoluções familiares para todos os membros.

### Intermediate — entrada dos formulários

Arquivo: [int_formularios_por_usuario.sql](models/intermediate/formularios/int_formularios_por_usuario.sql).

```sql
{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
{{ pu_formularios_sql(ref('int_evolucoes_por_usuario')) }}
```

Aplica a macro de formulários às evoluções por cidadão.

### Macro — extração do HTML

Arquivo: [extrair_campos_html_evolucao.sql](macros/dominios/acolherio/extrair_campos_html_evolucao.sql).

```sql
{% macro extrair_campos_html_evolucao(
    source_relation,
    id_cols,
    col_html = 'descricao_evolucao',
    extra_where = '',
    table_alias = 'src'
) %}
(
    with evolucoes as (
        select
            {{ id_cols | join(', ') }},
            split(regexp_replace(
                {{ table_alias }}.{{ col_html }}, r'(?is)<h3\b[^>]*>', '\u001e'
            ), '\u001e') as secoes_html
        from {{ source_relation }} as {{ table_alias }}
        where regexp_contains({{ table_alias }}.{{ col_html }}, r'(?i)<b\b[^>]*>')
        {% if extra_where %}
            and {{ extra_where }}
        {% endif %}
    ),

    secoes as (
        select
            {{ id_cols | join(', ') }},
            secao,
            secoes_html[safe_offset(posicao + 1)] as proxima_secao
        from evolucoes, unnest(secoes_html) as secao with offset as posicao
        where posicao > 0
    ),

    formularios as (
        select
            {{ id_cols | join(', ') }},
            trim(regexp_replace(
                regexp_extract(secao, r'(?is)^(.*?)</h3\s*>'), r'<[^>]*>', ''
            )) as titulo_formulario,
            regexp_replace(secao, r'(?is)^.*?</h3\s*>', '') as bloco_campos,
            -- A seção seguinte pode conter as observações deste formulário.
            case
                when upper(trim(regexp_replace(
                    regexp_extract(proxima_secao, r'(?is)^(.*?)</h3\s*>'),
                    r'<[^>]*>', ''
                ))) in ('OBSERVAÇÕES', 'CONTEÚDO E DESCRIÇÃO')
                    then regexp_replace(proxima_secao, r'(?is)^.*?</h3\s*>', '')
            end as observacoes
        from secoes
    ),

    campos as (
        select
            {{ id_cols | join(', ') }},
            titulo_formulario,
            observacoes,
            field
        from formularios,
            unnest(regexp_extract_all(
                regexp_replace(
                    bloco_campos,
                    r'(?is)</?(?:span|em|i|u|font|small|s|strike|mark|a)\b[^>]*>', ''
                ),
                r'(?is)([^<>]+?:?\s*<b\b[^>]*>.*?</b\s*>)'
            )) as field
        where upper(titulo_formulario) not in ('OBSERVAÇÕES', 'CONTEÚDO E DESCRIÇÃO')
    )

    select
        {{ id_cols | join(', ') }},
        titulo_formulario,
        trim(regexp_replace(
            replace(regexp_extract(field, r'(?is)^(.*?)<b\b[^>]*>'), '&nbsp;', ' '),
            r':\s*$', ''
        )) as label,
        -- Tags de bloco separam palavras; tags de formatação só são removidas.
        trim(replace(replace(regexp_replace(
            regexp_replace(
                regexp_extract(field, r'(?is)<b\b[^>]*>(.*?)</b\s*>'),
                r'(?is)</?(?:br|p|div|li)\b[^>]*>', ' '
            ), r'<[^>]*>', ''
        ), '&nbsp;', ' '), '&amp;', '&')) as valor,
        observacoes
    from campos
)
{% endmacro %}
```

Separa seções por <h3>, obtém o título, extrai label antes de <b> e valor dentro de <b>, removendo tags. Só processa HTML com <b>.

### Macro — normalização e agrupamento

Arquivo: [formularios.sql](macros/dominios/acolherio/formularios.sql).

```sql
campos as (
    select distinct
        c.* replace (
            {{ decodificar_entidades_html('c.titulo_formulario') }} as titulo_formulario,
            {{ decodificar_entidades_html('c.label') }} as label,
            {{ decodificar_entidades_html('c.valor') }} as valor
        )
    from campos_extraidos as c
),

formularios as (
    select
        id_usuario,
        id_evolucao_modulo,
        id_evolucao,
        origem_modulo,
        id_familia,
        id_unidade,
        id_profissional,
        codigo_abrangencia,
        id_atividade,
        data_evolucao,
        data_cancelamento,
        tipo_evolucao,
        nome_profissional,
        nome_atividade,
        titulo_formulario,
        observacoes,
        array_agg(struct(label, valor) order by label, valor) as campos
    from campos
    group by
        id_usuario, id_evolucao_modulo, id_evolucao, origem_modulo,
        id_familia, id_unidade, id_profissional, codigo_abrangencia,
        id_atividade, data_evolucao, data_cancelamento, tipo_evolucao,
        titulo_formulario, observacoes, nome_profissional, nome_atividade
),
```

Decodifica entidades HTML após interpretar a estrutura e agrupa as respostas como campos, um array de STRUCT<label, valor>. As etapas seguintes da macro calculam a data de ocorrência e agrupam formulários por cidadão; não criam encaminhamentos.

### Intermediate — registros Centro POP

Arquivo: [int_registros_centro_pop.sql](models/intermediate/centro_pop/int_registros_centro_pop.sql).

```sql
{{ config(enabled=target.name != 'prod', materialized='table', schema=var('pu_candidato_schema', 'pu_candidatos'), tags=['planilha_unificada', 'pu_escopos']) }}
-- Uma ocorrência de resposta/presença; não agrega o histórico por usuário.
{{ registros_centro_pop() }}
```

Materializa ocorrências por resposta/presença usando registros_centro_pop.

### Macro — limpeza de respostas

Arquivo: [escopo.sql](macros/dominios/centro_pop/escopo.sql).

```sql
{% macro normalizar_texto_centro_pop(expr) %}
    case when lower(trim({{ expr }})) in ('', '-', 'undefined', 'null', 'não informado', 'nao informado', 'não informada', 'nao informada') then null else trim({{ expr }}) end
{% endmacro %}
```

Converte marcadores de ausência em NULL e remove espaços das extremidades.

### Macro — seleção dos encaminhamentos

Arquivo: [escopo.sql](macros/dominios/centro_pop/escopo.sql).

```sql
        array(select distinct as struct regexp_replace(trim(c.label), r':\s*$', '') label, {{ normalizar_texto_centro_pop('c.valor') }} valor
            from unnest(f.campos) c where starts_with(trim(c.label), 'Encaminhamentos') and {{ normalizar_texto_centro_pop('c.valor') }} is not null order by label, valor) encaminhamentos_extraidos
    from formularios f join publico p on f.id_usuario = p.id_usuario
    join {{ ref('dim_usuarios') }} u on f.id_usuario = u.id_usuario
    where f.data_cancelamento is null and f.titulo_formulario in (
        'Centro POP - Plano de Atendimento Individual (PAI)', 'Centro POP - Atendimento Social', 'Centro POP - Desligamento PAI', 'Situação de Saúde'
    )
),
```

Seleciona labels iniciados exatamente por Encaminhamentos, remove o dois-pontos final e descarta valores ausentes. Exige cidadão em dim_usuarios e formulário não cancelado. O CTE publico inclui cidadãos com atendimento Centro POP, formulário Centro POP ou presença em unidade Centro POP.

### Macro — identificação do assunto

Arquivo: [escopo.sql](macros/dominios/centro_pop/escopo.sql).

```sql
    select v.id_usuario, v.id_unidade, 'formulario_html' origem,
        case v.titulo_formulario when 'Situação de Saúde' then 'saude' when 'Centro POP - Plano de Atendimento Individual (PAI)' then 'pai' when 'Centro POP - Desligamento PAI' then 'desligamento' else 'atendimento_social' end tipo_registro,
        if(v.titulo_formulario = 'Situação de Saúde', 'pessoa', 'unidade') abrangencia,
        v.id_evolucao_modulo, v.titulo_formulario,
        v.data_ocorrencia, v.origem_data_ocorrencia, v.situacao_data_ocorrencia,
        cast(v.data_evolucao as datetime) data_registro, cast(v.data_evolucao as datetime) data_resposta,
        to_json_string(struct(v.id_evolucao_modulo, v.id_unidade, v.titulo_formulario, v.observacoes_html as observacoes, v.campos)) chave_origem,
```

O título Centro POP - Plano de Atendimento Individual (PAI) vira pai; Centro POP - Atendimento Social vira atendimento_social. O array encaminhamentos_extraidos é projetado como encaminhamentos pela configuração valores_html da mesma macro. Questionários e presenças não fornecem respostas para esses dois campos.

### Intermediate — perfil por cidadão

Arquivo: [int_perfil_centro_pop.sql](models/intermediate/centro_pop/int_perfil_centro_pop.sql).

```sql
{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Commas precede dynamically generated Jinja projections.
-- noqa: disable=LT04
{{ perfil_centro_pop(ref('int_registros_centro_pop'), ref('int_atendimentos_por_usuario'), ref('dim_usuarios')) }}
```

Consolida os registros de todas as unidades no perfil do cidadão.

### Macro — agregação histórica

Arquivo: [perfil_centro_pop.sql](macros/dominios/centro_pop/perfil_centro_pop.sql).

```sql
with encaminhamentos_historicos as (
    select
        r.id_usuario,
        r.tipo_registro,
        array_agg(distinct concat(trim(e.label), ': ', trim(e.valor)) order by concat(trim(e.label), ': ', trim(e.valor))) as valores
    from {{ registros }} as r, unnest(r.encaminhamentos) as e
    where
        r.tipo_registro in ('pai', 'atendimento_social')
        and nullif(trim(e.label), '') is not null and nullif(trim(e.valor), '') is not null
    group by r.id_usuario, r.tipo_registro
),
```

Agrupa por cidadão e tipo de registro. Produz valores distintos e ordenados no formato label: valor. Usa todo o histórico recebido, sem limitar ao último formulário. A projeção posterior usa enc_pai.valores e enc_atendimento_social.valores, preenchendo ausência com array vazio.

### Mart — campos na dimensão

Arquivo: [centro_pop.sql](models/marts/prontuario_eletronico/centro_pop/centro_pop.sql).

```sql
select
    id_usuario,
    unidades_atendimento,
    possui_registro_pai,
    data_ocorrencia_pai,
    origem_data_ocorrencia_pai,
    situacao_data_ocorrencia_pai,
    data_registro_pai,
    data_resposta_pai,
    data_inclusao_pai,
    data_inclusao_pai_informada,
    demandas_pai,
    encaminhamentos_pai,
    observacoes_pai,
    data_ocorrencia_atendimento_social,
    origem_data_ocorrencia_atendimento_social,
    situacao_data_ocorrencia_atendimento_social,
    data_registro_atendimento_social,
    data_resposta_atendimento_social,
    motivo_secundario_permanencia_rua,
    possui_referencias_familiares,
    territorio_referencia_familia,
    possibilidade_reinsercao_familiar,
    demandas_atendimento_social,
    encaminhamentos_atendimento_social,
    observacoes_atendimento_social,
```

Publica encaminhamentos_pai e encaminhamentos_atendimento_social na dimensão dim_centro_pop. Aqui os campos ainda são arrays de STRING.

### Macro — transformação para texto

Arquivo: [escopo.sql](macros/dominios/centro_pop/escopo.sql).

```sql
{% for campo in campos %}
{% set nome = nome_campo_perfil_centro_pop(assunto, campo) %}
            , {% if campo in ['demandas', 'encaminhamentos'] %}{% if listas_em_texto %}array_to_string(coalesce({{ alias }}.{{ nome }}, []), ', '){% else %}coalesce({{ alias }}.{{ nome }}, []){% endif %}
              {% elif indicadores_medicos is not none and nome in indicadores_medicos %}{{ indicadores_medicos[nome] }}
              {% else %}coalesce(cast({{ alias }}.{{ nome }} as string), ''){% endif %} as {{ nome }}
{% endfor %}
```

Na chamada com listas_em_texto=true, converte cada array em STRING separado por vírgula e espaço. Os nomes vêm de nome_campo_perfil_centro_pop: encaminhamentos + sufixo do assunto.

### Mart — publicação na tabela principal

Arquivo: [prontuario_eletronico.sql](models/marts/prontuario_eletronico/prontuario_eletronico.sql).

```sql
    struct(
        {{ perfil_publico_centro_pop('perfil_cp', listas_em_texto=true, indicadores_medicos={
            'uso_substancias_psicoativas': "case upper(trim(cast(medico.flag_uso_substancias_psicoativas as string))) when 'S' then 'Sim' when 'N' then 'Não' else '' end",
            'possui_problema_saude': "case upper(trim(cast(medico.flag_possui_comorbidade as string))) when 'S' then 'Sim' when 'N' then 'Não' else '' end"
        }) }},
        {{ consolidar_emails('coalesce(cp_reg.registros, [])', campo='email_filtro') }} as email_filtro,
        coalesce(cp_reg.registros, []) as registros,
        coalesce(hist.historico_mensal, []) as historico_mensal,
```

Inclui os campos no STRUCT centro_pop. perfil_cp vem de centro_pop, ligado por id_usuario. O alias físico da mart principal é planilha_unificada.

## Ramificações

### Perfil por unidade

`int_registros_centro_pop` → [int_perfil_centro_pop_por_unidade](models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) → CTE `cp_registros` da mart principal → `centro_pop.registros[]`.

Antes de chamar a mesma macro de perfil, troca temporariamente a chave por `id_usuario:id_unidade` e exclui registros sem unidade. Assim, o histórico é agregado separadamente por cidadão e unidade. A mart acrescenta nome da unidade e `email_filtro`; os encaminhamentos já são texto.

Campos: `centro_pop.registros[].encaminhamentos_pai` e `centro_pop.registros[].encaminhamentos_atendimento_social`.

### Histórico mensal

`int_registros_centro_pop` → [int_centro_pop_mensal](models/intermediate/centro_pop/int_centro_pop_mensal.sql) → [int_centro_pop_historico_mensal](models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) → CTE `historico_centro_pop` → `centro_pop.historico_mensal[]`.

A base mensal restringe unidades ao tipo Centro POP, usa chave cidadão:unidade e aplica corte até o menor valor entre fim da competência e data atual, além do corte de cadastro. Reutiliza a macro de perfil e publica listas em texto. Portanto, os encaminhamentos são históricos acumulados até o corte, não apenas respostas dadas naquele mês.

O histórico escolhe a maior versão dos snapshots por competência e acrescenta a prévia do mês corrente quando ele não tem fechamento. Os snapshots existentes são preservados; a operação de novos fechamentos foi removida nesta limpeza.

Campos: `centro_pop.historico_mensal[].encaminhamentos_pai` e `centro_pop.historico_mensal[].encaminhamentos_atendimento_social`.

## Regras que afetam o resultado

- Os títulos dos formulários e o prefixo Encaminhamentos precisam corresponder ao código; não há busca genérica por sinônimos.
- A deduplicação final considera o texto completo label: valor. Labels diferentes com o mesmo valor permanecem distintos.
- A ordem final é textual, não cronológica.
- Os encaminhamentos acumulam histórico, mesmo quando outros campos do perfil representam o último formulário.
- O fluxo geral não exige tipo de unidade Centro POP em cada formulário; a seleção dos formulários usa seus títulos. A ramificação mensal aplica esse filtro de unidade explicitamente.
- A presença de email_filtro é metadado de destinatários; não comprova filtragem automática por login.

### centro_pop.encaminhamentos

STRING somente do perfil geral: respostas de todos os campos do tópico Encaminhamentos dos formulários Centro POP - Atendimento Social e Centro POP - Plano de Atendimento Individual (PAI). Social primeiro, depois PAI, separados por vírgula e espaço. Não publica os rótulos. Respostas com vírgulas ou dois-pontos são preservadas integralmente.

**Raw → Int → Mart:** evoluções de usuários/famílias/administrativas/grupo → int_evolucoes → int_evolucoes_por_usuario → int_formularios_por_usuario → int_registros_centro_pop → CTE encaminhamentos_centro_pop → prontuario_eletronico (planilha_unificada).

Usa o histórico não cancelado dos pares estruturados label/valor. Deduplica o mesmo par por cidadão e assunto; respostas iguais em campos diferentes ou em assuntos diferentes permanecem repetidas. Sem respostas válidas retorna texto vazio. Não altera as colunas individuais, os registros por unidade ou o histórico mensal.

Código original em [prontuario_eletronico.sql](models/marts/prontuario_eletronico/prontuario_eletronico.sql):

```sql
with encaminhamentos_centro_pop as (
    select
        id_usuario,
        string_agg(
            valor, ', '
            order by if(tipo_registro = 'atendimento_social', 0, 1), concat(label, ': ', valor)
        ) as encaminhamentos
    from (
        select distinct
            r.id_usuario,
            r.tipo_registro,
            trim(e.label) as label,
            trim(e.valor) as valor
        from {{ ref('int_registros_centro_pop') }} as r, unnest(r.encaminhamentos) as e
        where
            r.tipo_registro in ('atendimento_social', 'pai')
            and nullif(trim(e.label), '') is not null
            and nullif(trim(e.valor), '') is not null
    )
    group by id_usuario
)
```

Projeção no STRUCT centro_pop:

```sql
coalesce(cp_enc.encaminhamentos, '') as encaminhamentos
```

O resultado é associado ao cidadão por `p.id_usuario = cp_enc.id_usuario`. A consolidação não recorta valores a partir do texto formatado das duas colunas antigas.

### centro_pop.procedimentos

O nome procedimentos é preservado por compatibilidade, mas a origem correta são as demandas obrigatórias: Demanda inicial (Atendimento Social) e Identificação das demandas apresentadas pelo usuário (PAI).

**Raw → Int → Mart:** HTML dos quatro módulos de evoluções → int_evolucoes → int_evolucoes_por_usuario → int_formularios_por_usuario → int_registros_centro_pop.demandas → procedimentos_centro_pop → prontuario_eletronico.

Consolida o histórico não cancelado, com valores distintos por cidadão e assunto. Social antes de PAI; valores ordenados alfabeticamente em cada assunto, separados por vírgula e espaço. Preserva repetições entre assuntos e pontuação interna. Ausência retorna texto vazio. Não altera encaminhamentos, os campos individuais, os perfis por unidade ou o histórico mensal.

Código em [prontuario_eletronico.sql](models/marts/prontuario_eletronico/prontuario_eletronico.sql).
