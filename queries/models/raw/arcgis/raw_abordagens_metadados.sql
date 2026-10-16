-- Metadados ausentes no tratamento legado; não replica suas normalizações.
select
    globalid,
    parentrowid,
    cast(data_abordagem as string) as data_abordagem_original,
    created_date as data_criacao_original,
    last_edited_date as data_edicao_original,
    timestamp_captura,
    date(safe.timestamp_millis(safe_cast(safe_cast(data_abordagem as numeric) as int64))) as data_abordagem_epoch
from {{ source('arcgis_raw', 'abordagem_repeat_raw') }}
