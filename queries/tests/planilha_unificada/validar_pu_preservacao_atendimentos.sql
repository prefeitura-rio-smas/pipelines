-- noqa: disable=ST06
{{ config(tags = ['planilha_unificada']) }}

-- Confere eventos com datas, unidade e cancelamento. Profissionais
-- compartilhados não multiplicam o evento.
with esperado as (
    select distinct
        coalesce(cast(p.id_usuario as string), '') as id_usuario,
        coalesce(a.id_atendimento_modulo, '') as id_atendimento_modulo,
        coalesce(a.origem_modulo, '') as origem_modulo,
        coalesce(a.nome_unidade, '') as nome_unidade,
        coalesce(a.tipo_atendimento_descricao, '')
            as tipo_atendimento_descricao,
        coalesce(cast(a.data_atendimento as string), '') as data_atendimento,
        {{ normalizar_hora_atendimento('a.hora_atendimento') }} as hora_atendimento,
        coalesce(a.flag_cancelado, '') as flag_cancelado
    from {{ pu_ref('int_pu_atendimentos') }} as p, unnest(p.atendimentos) as a
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    where upper(trim(coalesce(a.flag_cancelado, ''))) != 'S'
),

obtido as (
    select
        p.usuario.id_usuario,
        a.* except (profissionais)
    from {{ ref('planilha_unificada') }} as p,
        unnest(p.atendimentos.registros) as a
),

faltantes as (
    select * from esperado
    except distinct
    select * from obtido
),

extras as (
    select * from obtido
    except distinct
    select * from esperado
)

select * from faltantes
union all
select * from extras
