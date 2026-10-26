-- noqa: disable=ST06
{{ config(tags = ['planilha_unificada']) }}

-- Datas de saída e transferências não podem sumir da base por usuário.
with esperado as (
    select distinct
        coalesce(cast(m.id_paciente as string), '') as id_usuario,
        coalesce(cast(m.id_familia as string), '') as id_familia,
        coalesce(cast(m.data_entrada as string), '') as data_entrada,
        coalesce(cast(m.data_saida as string), '') as data_saida,
        coalesce(m.parentesco_responsavel, '') as parentesco_responsavel,
        coalesce(ms.descricao_motivo_saida, '') as motivo_saida
    from {{ pu_ref('raw_membros_familia') }} as m
    left join
        {{ pu_ref('raw_familias_motivos_saida') }} as ms
        on m.id_motivo_saida = ms.id_motivo_saida
    inner join {{ pu_ref('dim_usuarios') }} as u on m.id_paciente = u.id_usuario
    where m.id_familia is not null
),

obtido as (
    select
        p.usuario.id_usuario,
        f.id_familia,
        v.*
    from {{ ref('planilha_unificada') }} as p,
        unnest(p.familias) as f,
        unnest(f.vinculos) as v
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
