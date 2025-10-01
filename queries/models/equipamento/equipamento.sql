{{
    config(
        alias="cras_cas_polígonos_smas_raw",
        schema="cas_cras_poligonos"
        materialized="table" , 

    )
}}
with base as (
    SELECT
        fid AS fid,
        SHAPE_Area AS shape_area,
        cod_unid AS codigo_unidade,
        nome_unid AS nome_unidade,
        categoria AS categoria,
        SHAPE_Leng AS shape_leng,
        SHAPE_Length AS shape_length,
        shape AS shape
    FROM {{ source("cras_cas_poligonos_smas_raw") }} 

)
