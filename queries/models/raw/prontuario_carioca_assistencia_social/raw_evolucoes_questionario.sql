-- Camada Raw: Evoluções de questionários estruturados (regeatv_evol_quest)
-- Vincula o prontuário (seqprontuario) às evoluções de questionário e às
-- respostas registradas em gh_regeatv_evol_quest_list / _quest_text.
with source as (
    select
        seqprontuario as id_prontuario,
        seqevol as id_evolucao,
        seqmodulo as id_modulo,
        seqtemplate as id_template,
        datevol as data_evolucao,
        dsclstevol as descricao_evolucao,
        dscconteudo as conteudo_evolucao
    from {{ source('brutos_acolherio_staging', 'gh_regeatv_evol_quest') }}
)

select * from source
