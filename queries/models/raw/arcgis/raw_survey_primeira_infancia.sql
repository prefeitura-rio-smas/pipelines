-- Camada Raw: Survey Primeira Infância Carioca (entregas de cartão)
-- Fonte: ArcGIS (primeira_infancia_carioca_raw)
-- Limpeza técnica: arquivar_registro NULL, cpf sem máscara, datas epoch -> DATE.
-- O dedup de negócio (primeira entrega registrada no ArcGIS) é feito na int_status.
with source as (
    select
        * except (data_entrega, created_date, last_edited_date, data_nascimento),

        timestamp_millis(cast(data_entrega as int64)) as data_entrega,
        timestamp_millis(cast(created_date as int64)) as created_date,
        timestamp_millis(cast(last_edited_date as int64)) as last_edited_date,

        parse_date('%d/%m/%Y', data_nascimento) as data_nascimento,

        regexp_replace(cpf, r'[.-]', '') as cpf_sem_formatacao
    from {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }}
    where arquivar_registro is null
)

select * from source
