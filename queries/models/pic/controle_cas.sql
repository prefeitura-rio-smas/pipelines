-- queries/models/pic/controle_cas.sql
-- Modelo limpo do Controle CAS
SELECT 
    *
FROM {{ source('arcgis_raw', 'controle_cas_raw') }}
