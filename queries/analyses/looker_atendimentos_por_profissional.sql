-- Habilitar DS_USER_EMAIL no conector BigQuery e usar a contagem de registros.
with identidade as (
    select nullif(lower(trim(@DS_USER_EMAIL)), '') as email
),

unidades_autorizadas as (
    select
        p.id_usuario_unidade,
        p.id_usuario,
        p.id_unidade,
        p.nome_unidade,
        p.atendimentos.registros
    from `rj-smas-dev.abordagem.prontuario_eletronico` as p
    cross join identidade as i
    where
        i.email is not null
        and nullif(trim(p.id_unidade), '') is not null
        and {{ prontuario_email_autorizado('p.email_filtro', 'i.email') }}
)

select  -- noqa: ST06
    u.id_usuario_unidade,
    u.id_usuario,
    u.id_unidade,
    u.nome_unidade,
    a.id_atendimento_modulo,
    a.origem_modulo,
    safe_cast(a.data_atendimento_informada as date) as data_atendimento,
    safe_cast(a.data_atendimento_sistema as date) as data_cadastro_atendimento,
    a.tipo_unidade,
    a.tipo_servico,
    a.cas,
    a.elegivel_indicador,
    a.cadastrante,
    nullif(trim(p.id_profissional), '') as id_profissional,
    p.nome_profissional,
    p.profissional_cbo,
    p.classificacao_atendimento,
    p.nome_atendimento
from unidades_autorizadas as u
cross join unnest(u.registros) as a
cross join unnest(a.profissionais) as p
where a.id_unidade = u.id_unidade
