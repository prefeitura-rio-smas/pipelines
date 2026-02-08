-- queries/models/pic/primeira_infancia_carioca.sql
-- Modelo limpo do Survey Primeira Infância
-- Sugestão: Aplicar filtro de registros não arquivados aqui
SELECT 
    *
FROM {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }}
-- WHERE arquivar_registro IS NOT TRUE
