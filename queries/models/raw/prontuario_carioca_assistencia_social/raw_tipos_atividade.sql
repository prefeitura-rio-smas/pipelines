-- Camada Raw: Tipos de atividade

with source as (
    select
        seqtpativ as id_tipo_atividade,
        dsctpativ as nome_tipo_atividade,
        indinativo as indicador_inativo,
        listcbo as lista_cbo,
        codproced as codigo_procedimento,
        dsclstraps as descricao_lista_raps,
        indformafat as indicador_forma_faturamento,
        indusopacsemcad as indicador_uso_pac_sem_cadastro
    from {{ source('brutos_acolherio_staging', 'gh_tpatividades') }}
)

select * from source
