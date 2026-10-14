-- Snapshot cadastral; não representa diagnósticos na data do atendimento.
select
    seqpac as id_usuario,
    indcomorb as flag_possui_comorbidade,
    dsccomorb as descricao_comorbidades,
    datcomorbinfo as data_informacao_comorbidades,
    dsclocaltratcomorb as local_tratamento_comorbidades,
    datcadastro as data_cadastro,
    sequscadastro as id_unidade_cadastro
from {{ source('brutos_acolherio_staging', 'gh_pac_info_med') }}
