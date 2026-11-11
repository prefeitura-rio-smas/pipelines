{{ config(materialized='table', enabled=target.name != 'prod', tags=['prontuario_bi_unidade']) }}
select
    cast(p.id_usuario as string) as id_usuario,
    a.id_atendimento,
    a.id_unidade,
    a.nome_unidade,
    a.id_tipo_atendimento,
    a.tipo_atendimento_descricao,
    a.data_atendimento as data_atendimento_informada,
    a.hora_atendimento,
    a.flag_cancelado,
    a.cas,
    a.tipo_unidade,
    a.elegivel_indicador,
    a.cadastrante,
    a.data_cadastro_atendimento as data_atendimento_sistema,
    a.id_atendimento_modulo,
    a.origem_modulo,
    a.tipo_servico,
    a.id_familia,
    a.classificacao_atendimento,
    array_length(a.profissionais) as quantidade_linhas_bi
from {{ ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
where a.id_unidade is not null
