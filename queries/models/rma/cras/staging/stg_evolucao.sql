-- Tabela responsável por tratar os dados do model base evolução.

with filtro_aba as (
    select
        dscevopac,
        regexp_extract(dscevopac, r"<h3>(.*?)</h3>") as aba,
        data_evolucao,
        sequs,
        seqpac,
        seqlogin,
    from {{ ref('base_evolucao') }}
),

texto_evolucao_sem_tags_html as (
    select
        aba,
        regexp_replace(dscevopac, '<[^>]+>', ';') as dscevopac_sem_html,
        data_evolucao,
        sequs,
        seqpac,
        seqlogin  
    from filtro_aba
    where aba = 'CRAS - Ficha de Atendimento Individualizado'
),

remover_caracteres_repetidos as (
    select
        aba,
        regexp_replace(dscevopac_sem_html, ';+', ';') as dscevopac_limpo,
        data_evolucao,
        sequs,
        seqpac,
        seqlogin
    from texto_evolucao_sem_tags_html
)

select * from remover_caracteres_repetidos