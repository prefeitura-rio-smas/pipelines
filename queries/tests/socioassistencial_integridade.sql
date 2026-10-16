-- Nenhum join de contexto pode mudar o grão ou fabricar uma competência.
select id_registro
from {{ ref('mart_socioassistencial_unificada') }}
where
    (id_unidade is null and tipo_unidade is not null)
    or competencia is distinct from date_trunc(data_referencia, month)
    or (
        tipo_registro != 'ACOMPANHAMENTO_MENSAL'
        and competencia is distinct from date_trunc(data_evento, month)
    )
    or (
        tipo_registro = 'ACOMPANHAMENTO_MENSAL'
        and (competencia is null or data_referencia != last_day(competencia) or data_evento is not null)
    )
    or (tipo_registro = 'ATENDIMENTO' and atendimento is null)
    or (
        tipo_registro = 'ATENDIMENTO'
        and (select countif(p.flag_principal) from unnest(atendimento.profissionais) as p) != 1
    )
    or (
        tipo_registro = 'ACOMPANHAMENTO_MENSAL'
        and (id_familia is null or acompanhamento.id_servico is null)
    )
    or (tipo_registro = 'ATIVIDADE_COLETIVA' and atividade is null)
    or (tipo_registro = 'ABORDAGEM' and abordagem is null)
    or (tipo_registro = 'FORMULARIO' and formulario is null)
    or (tipo_registro = 'ENCAMINHAMENTO' and encaminhamento is null)
    or (tipo_registro = 'VINCULO_SERVICO' and vinculo_servico is null)
union all
select min(id_registro)
from {{ ref('mart_socioassistencial_unificada') }}
group by tipo_registro, origem_registro, id_registro_origem
having count(*) != 1
