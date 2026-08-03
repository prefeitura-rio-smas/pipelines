{{ config(
    materialized = 'table',
    schema = 'alta_complexidade'
) }}

{{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }}
