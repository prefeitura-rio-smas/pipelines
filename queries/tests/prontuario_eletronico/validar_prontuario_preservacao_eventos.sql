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
        nullif(trim(upper(a.cadastrante)), '') as cadastrante,
        coalesce(prof.nome_profissional, '') as nome_profissional,
        coalesce(cast(prof.id_familia_referencia as string), '') as id_familia_referencia,
        coalesce(prof.id_atendimento_legado, '') as id_atendimento_legado,
        coalesce(prof.classificacao_atendimento, '') as classificacao,
        a.atendimento_compartilhado
    from (
        {{ agrupar_atendimentos() }}
    ) as p
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
        cast(null as string) as cadastrante,
        '' as nome_profissional,
        '' as id_familia_referencia,
        '' as id_atendimento_legado,
        '' as classificacao,
        '' as atendimento_compartilhado
    from (
        {{ preparar_evolucoes() }}
    ) as e
    inner join {{ ref('dim_usuarios') }} as usuario_evolucao on e.id_usuario = usuario_evolucao.id_usuario
),

obtido as (
    select
        p.usuario.id_usuario,
        p.id_unidade,
        'atendimento' as origem,
        a.id_atendimento_modulo as id_evento,
        a.data_atendimento_informada as data_evento,
        a.origem_modulo as modulo,
        prof.id_profissional as cadastrante,
        prof.nome_profissional,
        prof.id_familia_referencia,
        prof.id_atendimento_legado,
        a.classificacao_atendimento as classificacao,
        a.atendimento_compartilhado
    from {{ ref('prontuario_eletronico') }} as p
    cross join unnest(p.atendimentos.registros) as a
    left join unnest(a.profissionais) as prof on true
    union all
    select
        p.usuario.id_usuario,
        p.id_unidade,
        'evolucao' as origem,
        e.id_evolucao_modulo as id_evento,
        e.data_evolucao as data_evento,
        '' as modulo,
        cast(null as string) as cadastrante,
        '' as nome_profissional,
        '' as id_familia_referencia,
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
),

selecao as (
    select
        to_json_string(struct(
            cast(id_usuario as string) as id_usuario,
            coalesce(cast(_id_unidade as string), '') as id_unidade,
            coalesce(concat(seq_atendimento, modulo), '') as id_evento,
            nullif(trim(upper(cadastrante)), '') as cadastrante
        )) as registro,
        1 as sinal
    from {{ ref('int_acolherio_atendimentos') }}
    union all
    select
        to_json_string(struct(id_usuario, id_unidade, id_evento, cadastrante)) as registro,
        -1 as sinal
    from obtido
    where origem = 'atendimento'
)

select
    registro,
    sum(sinal) as diferenca
from diferencas
group by registro
having sum(sinal) != 0
union all
select
    registro,
    sum(sinal) as diferenca
from selecao
group by registro
having sum(sinal) != 0
