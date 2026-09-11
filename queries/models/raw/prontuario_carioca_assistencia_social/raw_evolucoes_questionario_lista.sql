-- Camada Raw: Respostas de lista dos questionários estruturados (regeatv_evol_quest_list)
-- Vinculadas a gh_regeatv_evol_quest via id_evolucao + id_template.
with source as (
    select
        seqevol as id_evolucao,
        seqtemplate as id_template,
        seqmodulo as id_modulo,
        seqquest as id_questao,
        datresp as data_resposta,
        dscresp as resposta,
        indresp as indicador_resposta,
        numresp as numero_resposta,
        dscresp2 as resposta_complementar
    from {{ source('brutos_acolherio_staging', 'gh_regeatv_evol_quest_list') }}
)

select * from source
