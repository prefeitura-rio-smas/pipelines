-- Camada Raw: Configuração de leitos e bloqueios das unidades
with source as (
    select
        sequs as id_unidade,
        numleitos as total_leitos,
        numleitosbloq as leitos_bloqueados,
        numbloqinfra as leitos_bloqueados_infra,
        numbloqjud as leitos_bloqueados_judiciais,
        indadminleitos as flag_administra_leitos
    from {{ source('brutos_acolherio_staging', 'gh_us_config') }}
)
select * from source
