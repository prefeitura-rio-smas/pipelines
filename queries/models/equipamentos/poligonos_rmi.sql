SELECT
    fid,
    SHAPE__Area AS shape_area,
    cod_unid AS codigo_unidade,
    'SMAS' AS secretaria_responsavel,
    nome_unid AS nome_oficial,
    categoria AS tipo_equipamento,
    shape AS geometry
FROM {{ source('arcgis_raw', 'cras_cas_poligonos_smas_raw') }} 