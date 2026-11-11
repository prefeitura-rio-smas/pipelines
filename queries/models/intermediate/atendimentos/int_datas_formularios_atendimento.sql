{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with evolucoes as (
    select
        'u' as modulo,
        id_atendimento,
        id_usuario,
        id_unidade,
        id_evolucao,
        data_evolucao as data_registro,
        descricao_evolucao
    from {{ ref('raw_evolucoes_usuarios') }}
    where data_cancelamento is null and id_atendimento is not null and id_atendimento != 0
    union all
    select
        'f' as modulo,
        id_atendimento,
        id_paciente as id_usuario,
        id_unidade,
        id_evolucao,
        data_evolucao as data_registro,
        descricao_evolucao
    from {{ ref('raw_evolucoes_familias') }}
    where data_cancelamento is null and id_atendimento is not null and id_atendimento != 0
),

atendimentos as (
    select
        modulo,
        id_atendimento,
        id_paciente as id_usuario,
        id_unidade
    from {{ ref('raw_atendimentos_usuarios') }}
    union all
    select
        modulo,
        id_atendimento,
        id_paciente,
        id_unidade
    from {{ ref('raw_atendimentos_familias') }}
),

chaves_atendimento as (
    select distinct
        modulo,
        id_atendimento,
        id_usuario,
        id_unidade
    from atendimentos
),

vinculados as (
    select e.* from evolucoes as e
    inner join chaves_atendimento
        as a on e.modulo = a.modulo and e.id_atendimento = a.id_atendimento
    and e.id_usuario = a.id_usuario and e.id_unidade = a.id_unidade
),

extraidos as (
    select * from {{ extrair_campos_html_evolucao('vinculados', ['modulo','id_atendimento','id_usuario','id_unidade','id_evolucao','data_registro']) }}
),

campos as (
    select * replace (
        {{ decodificar_entidades_html('titulo_formulario') }} as titulo_formulario,
        lower(regexp_replace(trim({{ decodificar_entidades_html('label') }}), r'\s+', ' ')) as label,
        {{ decodificar_entidades_html('valor') }} as valor
    ) from extraidos
),

titulos as (
    select distinct titulo_formulario from campos
    where regexp_contains(label, r'^data (do|de) atendimento\b')
),

formularios as (
    select
        modulo,
        id_atendimento,
        id_usuario,
        id_unidade,
        id_evolucao,
        data_registro,
        titulo_formulario,
        array_agg(struct(label, valor) order by label, valor) as campos
    from campos
    where titulo_formulario in (select t.titulo_formulario from titulos as t)
    group by modulo, id_atendimento, id_usuario, id_unidade, id_evolucao, data_registro, titulo_formulario
),

ultimos as (
    select * from formularios
    qualify row_number() over (
        partition by modulo, id_atendimento, id_usuario, id_unidade, titulo_formulario
        order by data_registro desc nulls last, id_evolucao desc, to_json_string(campos) desc
    ) = 1
),

respostas as (
    select
        f.modulo,
        f.id_atendimento,
        f.id_usuario,
        f.id_unidade,
        c.valor,
        {{ data_ocorrencia_parse('c.valor') }} as data_informada
    from ultimos as f left join unnest(f.campos) as c on regexp_contains(c.label, r'^data (do|de) atendimento\b')
)

select
    modulo,
    id_atendimento,
    id_usuario,
    id_unidade,
    if(
        count(distinct data_informada) = 1 and countif(nullif(trim(valor), '') is not null and data_informada is null) = 0,
        min(data_informada), cast(null as date)
    ) as data_atendimento,
    case
        when countif(nullif(trim(valor), '') is not null) = 0 then 'ausente'
        when countif(nullif(trim(valor), '') is not null and data_informada is null) > 0 then 'invalida'
        when count(distinct data_informada) > 1 then 'conflitante' else 'formulario'
    end as origem_data_atendimento
from respostas
group by modulo, id_atendimento, id_usuario, id_unidade
