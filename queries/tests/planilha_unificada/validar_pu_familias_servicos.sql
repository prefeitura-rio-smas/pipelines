-- noqa: disable=ST06
{{ config(tags = ['planilha_unificada']) }}

-- A responsabilidade cadastral também associa famílias sem linha de membro.
-- Cancelamentos não podem remover serviços do histórico disponível.
with relacoes as (
    select
        id_paciente as id_usuario,
        id_familia
    from {{ pu_ref('raw_membros_familia') }}
    union distinct
    select
        id_usuario_responsavel as id_usuario,
        id_familia
    from {{ pu_ref('raw_familias') }}
),

familias_esperadas as (
    select distinct
        coalesce(cast(r.id_usuario as string), '') as id_usuario,
        coalesce(cast(r.id_familia as string), '') as id_familia
    from relacoes as r
    inner join {{ pu_ref('dim_usuarios') }} as u on r.id_usuario = u.id_usuario
    where r.id_familia is not null
),

familias_obtidas as (
    select
        p.usuario.id_usuario,
        f.id_familia
    from {{ ref('planilha_unificada') }} as p,
        unnest(p.familias) as f
),

servicos_esperados as (
    select distinct
        coalesce(cast(f.id_usuario as string), '') as id_usuario,
        coalesce(cast(s.id_familia as string), '') as id_familia,
        coalesce(                        {{ map_nome_servico_assistencial('s.id_servico_assistencial') }}, ''
        )
            as nome_servico,
        coalesce(un.nome_unidade, '') as nome_unidade,
        coalesce(cast(s.data_cadastro as string), '') as data_cadastro,
        coalesce(cast(s.data_cancelamento as string), '') as data_cancelamento
    from familias_esperadas as f
    inner join
        {{ pu_ref('raw_familias_servicos_assistenciais') }} as s
        on f.id_familia = cast(s.id_familia as string)
    left join {{ pu_ref('dim_unidades') }} as un on s.id_unidade = un.id_unidade
),

servicos_obtidos as (
    select
        p.usuario.id_usuario,
        f.id_familia,
        s.nome_servico,
        s.nome_unidade,
        s.data_cadastro,
        s.data_cancelamento
    from {{ ref('planilha_unificada') }} as p,
        unnest(p.familias) as f,
        unnest(f.servicos) as s
),

familias_faltantes as (
    select * from familias_esperadas
    except distinct
    select * from familias_obtidas
),

familias_extras as (
    select * from familias_obtidas
    except distinct
    select * from familias_esperadas
),

servicos_faltantes as (
    select * from servicos_esperados
    except distinct
    select * from servicos_obtidos
),

servicos_extras as (
    select * from servicos_obtidos
    except distinct
    select * from servicos_esperados
),

familias_duplicadas as (
    select
        id_usuario,
        id_familia
    from familias_obtidas
    group by id_usuario, id_familia
    having count(*) > 1
)

select id_usuario from familias_faltantes
union all
select id_usuario from familias_extras
union all
select id_usuario from servicos_faltantes
union all
select id_usuario from servicos_extras
union all
select id_usuario from familias_duplicadas
