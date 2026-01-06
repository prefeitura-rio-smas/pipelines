SELECT
    cod_unid AS codigo_unidade,
    'SMAS' AS secretaria_responsavel,
    nome_unid AS nome_oficial,
    categoria AS tipo_equipamento,
    ST_UNION_AGG(shape) AS geometry
FROM {{ source('arcgis_raw', 'cras_cas_poligonos_smas_raw') }}
GROUP BY 1, 3,4, 5