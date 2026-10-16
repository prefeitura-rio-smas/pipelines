-- Um registro por sujeito/serviço/unidade/mês, sem exigir atendimento no mês.
select min(id_registro) as id_registro
from {{ ref('mart_socioassistencial_unificada') }}
where tipo_registro = 'ACOMPANHAMENTO_MENSAL'
group by id_familia, acompanhamento.id_servico, id_unidade, competencia
having count(*) != 1
union all
select id_registro
from {{ ref('mart_socioassistencial_unificada') }}
where
    tipo_registro = 'ACOMPANHAMENTO_MENSAL'
    and (
        acompanhamento.flag_ativo_fechamento is distinct from exists (
            select 1 from unnest(acompanhamento.vinculos) as v
            where {{ vigencia_no_fechamento('v.data_cadastro', 'v.data_cancelamento', 'competencia') }}
        )
        or acompanhamento.flag_novo_no_mes is distinct from exists (
            select 1 from unnest(acompanhamento.vinculos) as v
            where date_trunc(date(v.data_cadastro), month) = competencia
        )
    )
