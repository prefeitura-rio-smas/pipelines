-- Camada Raw: Vínculo contas de operadores × perfis de acesso customizados
-- Fonte: gh_contas_modulos (banco Prontuário Carioca)
-- Grão: 1 linha por conta (seqlogin); seqgrupo = id do perfil customizado
with source as (
    select
        seqlogin as id_login,
        seqgrupo as id_perfil_acesso,
        seqprof as id_profissional,
        indadmin as flag_admin
    from {{ source('brutos_acolherio_staging', 'gh_contas_modulos') }}
    where seqgrupo is not null
)

select * from source
