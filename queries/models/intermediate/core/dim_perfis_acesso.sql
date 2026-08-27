-- Camada Intermediate: Dimensão Perfis de Acesso Customizados
-- Grão: 1 linha por perfil (catálogo gh_perfil_grupos)
with perfis as (
    select * from {{ ref('raw_perfis_grupos') }}
)
select
    id_perfil_acesso,
    nome_perfil,
    nivel_perfil,
    flag_perfil_em_uso,
    flag_associado_profissional,
    config_acesso_modulos,
    config_acesso_relatorios
from perfis
