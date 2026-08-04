-- Camada Raw: Capacidade operacional das unidades (SMAS)
with source as (
    select
        sequs as id_unidade,
        indeixo as tipo_publico,
        indacessib as flag_acessibilidade,
        vagas_homens,
        vagas_mulheres,
        vagas_neutras,
        indgraudepend as grau_dependencia,
        indabrangatend as abrangencia
    from {{ source('prontuario_carioca_assistencia_social', 'gh_us_smas') }}
)
select * from source
