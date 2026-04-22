-- Tabela para tratar a coluna dados_institucional, capturando apenas o inst_list

with tratar_coluna_json as (
    select 
        seqpac,
        json_value(
            dados_institucional,
            '$[0].inst_list'
        ) as inst_list
    from {{ ref('raw_pacientes_sm') }}
),

inst_list_tratado as (
select
    seqpac,
    cast(inst_usuario as int64) as inst_usuario
from tratar_coluna_json,
unnest(split(inst_list, ',')) as inst_usuario
where inst_usuario <> ''
)

select
    seqpac,
    inst_usuario,
    'Sim' as flag_familia_menor_idade_serv_acolhimento
from inst_list_tratado
where inst_usuario = 1 -- Id do Abrigo para criança/adolescente 