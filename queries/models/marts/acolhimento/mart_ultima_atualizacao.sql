{{ config(
    materialized = 'table',
    schema = 'alta_complexidade'
) }}

{{ get_ultima_atualizacao('raw_configuracoes_sistema') }}
