{{ config(enabled=target.name != 'prod', materialized='ephemeral', tags=['planilha_unificada']) }}
select
    p.id_projeto_social,
    p.id_unidade,
    p.id_login_cadastro,
    p.data_cadastro,
    p.data_cancelamento,
    p.indicador_ativo,
    m.id_paciente as id_usuario
from {{ ref('raw_membros_familia') }} as m
inner join {{ ref('raw_familias_projetos_sociais') }} as p on m.id_familia = p.id_familia
where m.data_saida is null
qualify row_number() over (
    partition by m.id_paciente, p.id_projeto_social, p.id_unidade
    order by p.data_cadastro desc nulls last, to_json_string(p) desc
) = 1
