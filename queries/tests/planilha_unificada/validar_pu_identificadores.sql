{{ config(tags = ['planilha_unificada']) }}

-- depends_on: {{ ref('planilha_unificada') }}
select field_path
from `{{ ref('planilha_unificada').database }}.{{ ref('planilha_unificada').schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
where
    table_name = '{{ ref('planilha_unificada').identifier }}'
    and ((
        regexp_contains(field_path, r'(^|\.)(id_|codigo|codigos_)|_codigo$')
        and regexp_extract(field_path, r'[^.]+$') not in (
            'id_usuario', 'id_familia', 'id_atendimento_modulo', 'id_evolucao_modulo'
        )
    )
    or (regexp_contains(field_path, r'^(atendimentos|evolucoes)\.quantidade(_por_servico\.(cras|creas|centro_pop|outros))?$') and data_type != 'INT64')
    or (
        not regexp_contains(field_path, r'^(atendimentos|evolucoes)\.quantidade(_por_servico\.(cras|creas|centro_pop|outros))?$')
        and data_type not in ('STRING', 'ARRAY<STRING>')
        and not starts_with(data_type, 'STRUCT<')
        and not starts_with(data_type, 'ARRAY<STRUCT<')
    ))
union all
select column_name as field_path
from `{{ ref('planilha_unificada').database }}.{{ ref('planilha_unificada').schema }}.INFORMATION_SCHEMA.COLUMNS`
where
    table_name = '{{ ref('planilha_unificada').identifier }}'
    and (
        column_name not in ('usuario', 'familias', 'projetos_sociais', 'atendimentos', 'evolucoes')
        or (column_name in ('usuario', 'atendimentos', 'evolucoes') and not starts_with(data_type, 'STRUCT<'))
        or (column_name in ('familias', 'projetos_sociais') and not starts_with(data_type, 'ARRAY<STRUCT<'))
    )
union all
select 'quantidade_colunas_raiz' as field_path
from (
    select count(*) as quantidade from `{{ ref('planilha_unificada').database }}.{{ ref('planilha_unificada').schema }}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = '{{ ref('planilha_unificada').identifier }}'
)
where quantidade != 5
