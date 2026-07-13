-- Tabela responsável por conter os dados de tipo de atendimento

with tipo_atendimentos as (
    select
        descatend as descricao_atendimento,
        seqtpatend,
        codabapront as aba_prontuario
    from {{ source('brutos_acolherio_staging', 'gh_tpatendimentos') }}
)

select 
    *
from tipo_atendimentos