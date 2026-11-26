-- Consulta personalizada BigQuery para os cartões do Looker Studio.
-- Habilitar o parâmetro padrão DS_USER_EMAIL.
-- Dimensão de período: data_de_atendimento.
-- Profissionais: COUNT_DISTINCT(cadastrante).
-- Famílias: COUNT_DISTINCT(id_familia).
select
    a.tipo_unidade,
    a.cas,
    p.nome_unidade as unidade_atendimento,
    a.cadastrante,
    safe_cast(a.data_atendimento_informada as date) as data_de_atendimento,
    safe_cast(nullif(trim(prof.id_familia_referencia), '') as int64) as id_familia
from `rj-smas-dev.abordagem.prontuario_eletronico` as p
cross join unnest(p.atendimentos.registros) as a
left join unnest(a.profissionais) as prof
where
    p.nome_unidade != 'Negado'
    and a.tipo_unidade in ('CRAS', 'CREAS', 'CENTRO POP')
    and exists (
        select 1
        from unnest(split(regexp_replace(coalesce(p.email_filtro, ''), r'[;\r\n]', ','), ',')) as destinatario
        where lower(trim(destinatario)) = nullif(lower(trim(@DS_USER_EMAIL)), '')
    )
