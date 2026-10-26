-- noqa: disable=ST06
{{ config(tags=['planilha_unificada']) }}
{% if var('pu_referencia', false) %}
    with referencia as (
        select distinct
            concat(cast(seq_atendimento as string), modulo) as chave,
            cast(id_usuario as string) as id_usuario,
            coalesce(cast(data_de_atendimento as string), '') as data_atendimento,
            {{ normalizar_hora_atendimento('hora_de_atendimento') }} as hora_atendimento,
            coalesce(unidade_atendimento, '') as nome_unidade,
            coalesce(nome_atendimento_original, '') as tipo_atendimento_descricao
        from {{ pu_programada() }}
    ),

    obtido as (
        select
            a.id_atendimento_modulo as chave,
            p.usuario.id_usuario,
            a.data_atendimento,
            a.hora_atendimento,
            a.nome_unidade,
            a.tipo_atendimento_descricao
        from {{ ref('planilha_unificada') }} as p, unnest(p.atendimentos.registros) as a
    )

    select faltantes.chave from (
        select * from referencia
        except distinct
        select * from obtido
    ) as faltantes
    union all
    select extras.chave from (
        select * from obtido
        except distinct
        select * from referencia
    ) as extras
    union all
    select chave from obtido
    group by chave
    having count(*) != 1
    union all
    select 'total' as chave from unnest([1])
    where (select sum(atendimentos.quantidade) from {{ ref('planilha_unificada') }}) != (select count(*) from referencia)
{% else %}
    select cast(null as string) as chave from unnest([1])
    where false
{% endif %}
