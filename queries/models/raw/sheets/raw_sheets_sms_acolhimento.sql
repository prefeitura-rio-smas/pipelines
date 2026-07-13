-- Camada Raw: Acolhimentos SMS (consolidado de planilhas externas da SUBPSE para meta).
-- Fonte bruta: rj-smas-dev.subpse_acolhimento.consolidado_sms_acolhimento_2026
-- Normaliza nomes de colunas (snake_case) e converte datas em string para DATE.

with source as (
    select
        prontuario,
        nome,
        safe.parse_date('%d/%m/%Y', nascimento) as data_nascimento,
        cpf,
        safe.parse_date('%d/%m/%Y', data_acolhimento) as data_acolhimento,
        unidade_acolhimento as unidade,
        mae as nome_mae,
        mes_acolhimento,
        origem,
        safe.parse_date('%d/%m/%Y', data_arquivo) as data_arquivo
    from {{ source('subpse_acolhimento', 'consolidado_sms_acolhimento_2026') }}
)

select * from source