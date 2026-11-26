{{ config(enabled=target.name != 'prod' or var('prontuario_publicacao', false), tags=['planilha_unificada']) }}

select
    id_usuario,
    id_unidade,
    mes_referencia
from {{ ref('int_centro_pop_mensal') }} as mensal
where
    quantidade_atendimentos != array_length(atendimentos)
    or quantidade_evolucoes != array_length(evolucoes)
    or quantidade_participacoes != (select countif(r.tipo_registro = 'participacao') from unnest(ocorrencias) as r)
    or array_length(atendimentos) + array_length(evolucoes) + array_length(ocorrencias) = 0
    {% for lista, data in [('atendimentos', 'data_atendimento'), ('evolucoes', 'data_evento'), ('ocorrencias', 'data_evento')] %}
        or exists (
            select 1 from unnest(mensal.{{ lista }}) as r
            where
                date_trunc(r.{{ data }}, month) is distinct from mes_referencia
                or r.id_unidade is distinct from mensal.id_unidade
                or r.{{ data }} > current_date('America/Sao_Paulo')
        )
    {% endfor %}
union all
select
    id_usuario,
    id_unidade,
    mes_referencia
from {{ ref('int_centro_pop_mensal') }}
group by id_usuario, id_unidade, mes_referencia
having count(*) > 1
