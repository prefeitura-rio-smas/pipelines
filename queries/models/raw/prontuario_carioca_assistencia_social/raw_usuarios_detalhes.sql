-- Camada Raw: Detalhes complementares dos pacientes
with source as (
    select
        seqpac as id_paciente,
        {{ map_flag_cadunico('indcadunico') }} as flag_cadunico,
        datcadunico as data_cadunico,
        indgestante as flag_gestante,
        dsctomdecproces as numero_processo_decisao_apoiada,
        dsctomdecnome as nome_apoiador,
        indsmentcompr as flag_saude_mental_comprometida,
        indmotivacol as id_motivo_acolhimento,
        indvioldir as violacao_direito,
        valpontos as pontuacao,
        indgraudepend as grau_dependencia,
        indorientsex as orientacao_sexual,
        valrendaativ as renda_ativa,
        valrendabenef as renda_beneficio,
        indtipovinc  as vinculo_trabalhista
    from  {{ source('brutos_acolherio_staging', 'gh_pac_dados') }}
)
select * from source
