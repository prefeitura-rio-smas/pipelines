{{ config(tags=['centro_pop_mensal']) }}
select id_usuario from {{ ref('int_centro_pop_mensal') }}
group by id_usuario, id_unidade, mes_referencia
having count(*) != 1
union all
select id_usuario from {{ ref('int_centro_pop_mensal') }}
where
    id_unidade is null or mes_referencia != date_trunc(mes_referencia, month)
    or quantidade_atendimentos < 0 or quantidade_evolucoes < 0 or quantidade_participacoes < 0
union all
select id_usuario from {{ ref('int_centro_pop_snapshots') }}
group by id_usuario, id_unidade, mes_referencia, versao
having count(*) != 1
