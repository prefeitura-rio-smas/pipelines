-- Camada Raw: Perfis de acesso customizados (catálogo de grupos de perfil)
-- Fonte: gh_perfil_grupos (banco Prontuário Carioca)
-- Grão: 1 linha por perfil (seqgrupo)
with source as (
    select
        seqgrupo as id_perfil_acesso,
        dsctitmod as nome_perfil,
        indnivel as nivel_perfil,
        indus as flag_perfil_em_uso,
        indassocprof as flag_associado_profissional,
        dsccnfaccess as config_acesso_modulos,
        dsccnfaccessrel as config_acesso_relatorios
    from {{ source('brutos_acolherio_staging', 'gh_perfil_grupos') }}
)
select * from source
