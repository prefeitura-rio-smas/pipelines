{{ config(tags=['prontuario_bi_unidade']) }}
select id_usuario from {{ ref('prontuario_bi_perfil_unidade') }}
where
    id_unidade is null
    or (alta_complexidade.id_unidade is not null and alta_complexidade.id_unidade != id_unidade)
union all
select id_usuario from {{ ref('prontuario_bi_perfil_unidade') }}
group by id_usuario, id_unidade
having count(*) != 1
union all
select cast(id_usuario as string) from {{ ref('prontuario_bi_atendimentos') }}
where id_unidade is null
union all
select cast(id_usuario as string) from {{ ref('prontuario_bi_evolucoes') }}
where id_unidade is null
