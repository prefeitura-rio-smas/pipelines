{{ config(materialized='table', enabled=target.name != 'prod', tags=['prontuario_bi_unidade']) }}
select
    cast(e.id_usuario as string) as id_usuario,
    e.id_evolucao,
    e.id_evolucao_modulo,
    e.origem_modulo,
    e.id_familia,
    e.id_unidade,
    e.nome_unidade,
    e.tipo_servico,
    e.id_profissional,
    e.id_atividade,
    e.data_evolucao,
    e.tipo_evolucao,
    e.descricao_evolucao,
    e.nome_atividade,
    e.nome_profissional,
    array_to_string(array(
        select
            concat(coalesce(f.titulo_formulario, ''), '\n', array_to_string(array(
                select concat(coalesce(c.label, ''), ': ', coalesce(c.valor, ''))
                from unnest(f.campos) as c
            ), '\n'))
        from unnest(p.formularios) as f
        where f.id_evolucao_modulo = e.id_evolucao_modulo and f.id_unidade = e.id_unidade
    ), '\n\n') as formularios_texto
from {{ pu_ref('int_evolucoes_por_usuario') }} as e
left join {{ ref('int_formularios_por_usuario') }} as p on e.id_usuario = p.id_usuario
where e.id_unidade is not null
