{{ config(tags=['planilha_unificada']) }}
with esperado as (
    select
        column_name,
        if(data_type = 'ARRAY<STRING>', data_type, 'STRING') as data_type
    from `{{ ref('dim_centro_pop').database }}.{{ ref('dim_centro_pop').schema }}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = '{{ ref('dim_centro_pop').identifier }}' and column_name != 'id_usuario'
),

obtido as (
    select
        data_type,
        regexp_extract(field_path, r'[^.]+$') as column_name
    from `{{ ref('planilha_unificada').database }}.{{ ref('planilha_unificada').schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    where
        table_name = '{{ ref('planilha_unificada').identifier }}'
        and regexp_contains(field_path, r'^escopos\.centro_pop\.[^.]+$')
        and field_path not in ('escopos.centro_pop.historico', 'escopos.centro_pop.participacoes')
)

select coalesce(e.column_name, o.column_name) as campo
from esperado as e full outer join obtido as o on e.column_name = o.column_name
where e.data_type is distinct from o.data_type
