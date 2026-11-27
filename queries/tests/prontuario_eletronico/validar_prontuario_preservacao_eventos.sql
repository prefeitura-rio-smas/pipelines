{{ config(tags=['planilha_unificada']) }}
-- Comparar identidade, unidade, data e multiplicidade de participações e evoluções.
with esperado as (
    select
        cast(p.id_usuario as string) as id_usuario,
        coalesce(cast(a.id_unidade as string), '') as id_unidade,
        'atendimento' as origem,
        coalesce(a.id_atendimento_modulo, '') as id_evento,
        coalesce(cast(a.data_atendimento as string), '') as data_evento,
        coalesce(a.origem_modulo, '') as modulo,
        coalesce(cast(prof.id_profissional as string), '') as profissional,
        coalesce(prof.id_atendimento_legado, '') as id_atendimento_legado,
        coalesce(prof.classificacao_atendimento, '') as classificacao,
        a.atendimento_compartilhado
    from (
    {{ agrupar_atendimentos() }}
) as p
    inner join {{ ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    cross join unnest(p.atendimentos) as a
    cross join unnest(a.profissionais) as prof
    union all
    select
        cast(e.id_usuario as string) as id_usuario,
        coalesce(cast(e.id_unidade as string), '') as id_unidade,
        'evolucao' as origem,
        e.id_evolucao_modulo as id_evento,
        coalesce(cast(e.data_evolucao as string), '') as data_evento,
        '' as modulo,
        '' as profissional,
        '' as id_atendimento_legado,
        '' as classificacao,
        '' as atendimento_compartilhado
    from (
    {{ preparar_evolucoes() }}
) as e
    inner join {{ ref('dim_usuarios') }} as u on e.id_usuario = u.id_usuario
),

obtido as (
    select
        p.usuario.id_usuario,
        p.id_unidade,
        'atendimento' as origem,
        a.id_atendimento_modulo as id_evento,
        a.data_atendimento_informada as data_evento,
        a.origem_modulo as modulo,
        a.profissionais[safe_offset(0)].id_profissional as profissional,
        a.profissionais[safe_offset(0)].id_atendimento_legado as id_atendimento_legado,
        a.classificacao_atendimento as classificacao,
        a.atendimento_compartilhado
    from {{ ref('prontuario_eletronico') }} as p, unnest(p.atendimentos.registros) as a
    union all
    select
        p.usuario.id_usuario,
        p.id_unidade,
        'evolucao' as origem,
        e.id_evolucao_modulo as id_evento,
        e.data_evolucao as data_evento,
        '' as modulo,
        '' as profissional,
        '' as id_atendimento_legado,
        '' as classificacao,
        '' as atendimento_compartilhado
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
