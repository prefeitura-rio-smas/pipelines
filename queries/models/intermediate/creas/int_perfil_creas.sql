{{ config(enabled=target.name != 'prod', tags=['planilha_unificada', 'pu_dimensoes']) }}
with formularios as (
    select
        p.id_usuario,
        array_agg(f order by f.data_evolucao, f.id_evolucao_modulo) as historico
    from {{ pu_ref('int_formularios_por_usuario') }} as p, unnest(p.formularios) as f
    where regexp_contains(f.titulo_formulario, r'(?i)CREAS.*(PETI|Adolescente em trabalho|MSE)')
    group by p.id_usuario
),

participacoes as (
    select
        p.id_usuario,
        array_agg(
            struct(
                p.id_unidade, p.id_atividade, a.nome_atividade,
                p.data_presenca, p.hora_presenca
            ) order by p.data_presenca, p.hora_presenca, p.id_atividade
        ) as participacoes
    from {{ pu_ref('fct_presencas_usuarios') }} as p
    inner join {{ pu_ref('raw_atividades_grupo') }} as a on p.id_atividade = a.id_atividade
    where regexp_contains(a.nome_atividade, r'(?i)passo[ -]+a[ -]+passo')
    group by p.id_usuario
)

select
    coalesce(f.id_usuario, p.id_usuario) as id_usuario,
    coalesce(f.historico, []) as historico,
    coalesce(p.participacoes, []) as participacoes
from formularios as f full outer join participacoes as p on f.id_usuario = p.id_usuario
