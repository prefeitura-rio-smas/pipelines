-- Camada Raw: Controle CAS (folhas de beneficiários do Cartão Primeira Infância)
-- Fonte: ArcGIS (controle_cas_raw)
-- Limpeza: cpf sem máscara, datas robustas (ISO ou BR)
with source as (
    select
        * except (data_particao, data_entrega_text),

        regexp_replace(cpf, r'[.-]', '') as cpf_sem_formatacao,

        -- Tratamento robusto da data da folha (ArcGIS)
        coalesce(
            safe.parse_date('%Y-%m-%d', data_particao),
            safe.parse_date('%d/%m/%Y', data_particao)
        ) as data_particao,

        nullif(data_entrega_text, 'None') as data_entrega_text
    from {{ source('arcgis_raw', 'controle_cas_raw') }}
)

select * from source