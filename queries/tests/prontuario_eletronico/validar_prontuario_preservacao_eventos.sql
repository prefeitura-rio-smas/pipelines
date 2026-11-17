{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Comparar identidade, unidade, data e multiplicidade: apenas somar totais esconderia perdas compensadas por duplicatas.
with esperado as (
    select
        cast(p.id_usuario as string) as id_usuario,
        coalesce(cast(a.id_unidade as string), '') as id_unidade,
        'atendimento' as origem,
        a.id_atendimento_modulo as id_evento,
        coalesce(cast(a.data_atendimento as string), '') as data_evento,
        to_json_string(array(
            select coalesce(cast(prof.id_profissional as string), '')
            from unnest(a.profissionais) as prof with offset as posicao
            order by posicao
        )) as profissionais
    from {{ ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    inner join {{ ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    union all
    select
        cast(e.id_usuario as string) as id_usuario,
        coalesce(cast(e.id_unidade as string), '') as id_unidade,
        'evolucao' as origem,
        e.id_evolucao_modulo as id_evento,
        coalesce(cast(e.data_evolucao as string), '') as data_evento,
        '' as profissionais
    from {{ ref('int_evolucoes_por_usuario') }} as e
    inner join {{ ref('dim_usuarios') }} as u on e.id_usuario = u.id_usuario
),

obtido as (
    select
        p.usuario.id_usuario,
        p.id_unidade,
        'atendimento' as origem,
        a.id_atendimento_modulo as id_evento,
        a.data_atendimento_informada as data_evento,
        to_json_string(array(
            select prof.id_profissional from unnest(a.profissionais) as prof with offset as posicao
            order by posicao
        )) as profissionais
    from {{ ref('prontuario_eletronico') }} as p, unnest(p.atendimentos.registros) as a
    union all
    select
        p.usuario.id_usuario,
        p.id_unidade,
        'evolucao' as origem,
        e.id_evolucao_modulo as id_evento,
        e.data_evolucao as data_evento,
        '' as profissionais
    from {{ ref('prontuario_eletronico') }} as p, unnest(p.evolucoes.registros) as e
),

diferencas as (
    select
        to_json_string(e) as registro,
        1 as sinal
    from esperado as e
    union all
    select
        to_json_string(o) as registro,
        -1 as sinal
    from obtido as o
)

select
    registro,
    sum(sinal) as diferenca
from diferencas
group by registro
having sum(sinal) != 0
