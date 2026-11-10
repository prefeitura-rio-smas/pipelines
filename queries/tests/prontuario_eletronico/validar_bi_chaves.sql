-- UNION branches inherit the column names from the first projection.
-- noqa: disable=AL03
{{ config(tags=['prontuario_bi_unidade']) }}
select
    'atendimento' as origem,
    cast(id_usuario as string) as id_usuario
from {{ ref('prontuario_bi_atendimentos') }}
group by id_usuario, id_unidade, id_atendimento_modulo
having count(*) != 1
union all
select
    'evolucao',
    cast(id_usuario as string)
from {{ ref('prontuario_bi_evolucoes') }}
group by id_usuario, id_unidade, id_evolucao_modulo
having count(*) != 1
union all
select
    'permissao',
    email
from {{ ref('prontuario_bi_permissoes') }}
group by email, id_unidade
having count(*) != 1
