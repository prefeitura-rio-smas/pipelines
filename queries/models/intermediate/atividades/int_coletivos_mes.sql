-- Presenças em atividades de grupo no mês de referência, enriquecidas para o
-- Bloco D do RMA CRAS (D.1-D.7). Grão: 1 linha por presença.
-- Mês de referência: var competencia ('AAAA-MM'); vazio = mês corrente.
-- SCFV = tipos 1-4; tipos 5-8 (Tô de Boa) = proxy de não continuado (D.6).

{% set competencia = var('competencia', '') %}
{% if competencia == '' %}
    {% set mes_ref = "date_trunc(current_date(), month)" %}
{% else %}
    {% set mes_ref = "date('" + competencia + "-01')" %}
{% endif %}

with presencas as (
    select
        p.id_atividade,
        p.id_usuario,
        p.data_presenca,
        p.id_unidade
    from {{ ref('fct_presencas_usuarios') }} as p
    where date_trunc(p.data_presenca, month) = {{ mes_ref }}
),

enriquecida as (
    select
        p.id_unidade,
        p.id_usuario,
        p.data_presenca,
        a.id_tipo_atividade,
        a.nome_tipo_atividade,
        date_diff(
            last_day({{ mes_ref }}),
            u.data_nascimento,
            year
        ) as idade_anos,
        u.flag_deficiencia
    from presencas as p
    left join {{ ref('dim_atividades_grupo') }} as a
        on p.id_atividade = a.id_atividade
    left join {{ ref('dim_usuarios') }} as u
        on p.id_usuario = u.id_usuario
)

select * from enriquecida
