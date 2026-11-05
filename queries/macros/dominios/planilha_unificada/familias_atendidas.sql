{% macro pu_familias_atendidas_sql(source_relation) %}
-- Compatibilidade com o indicador da programada: não inferir família pelo cadastro
-- nem condicionar a associação à existência do profissional no fato novo.
with associacoes as (
    select distinct
        id_usuario,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        cast(id_familia as string) as id_familia,
        coalesce(cast(data_de_atendimento as string), '') as data_atendimento,
        coalesce(unidade_atendimento, '') as nome_unidade,
        coalesce(tipo_unidade, '') as tipo_unidade,
        coalesce(email, '') as email,
        if(
            tipo_unidade in ('CRAS', 'CREAS', 'CENTRO POP')
            and unidade_atendimento != 'Negado', 'S', 'N'
        ) as elegivel_indicador
    from {{ source_relation }}
    where id_familia is not null
)

select
    id_usuario,
    array_agg(struct(
        id_atendimento_modulo,
        id_familia,
        data_atendimento,
        nome_unidade,
        tipo_unidade,
        email,
        elegivel_indicador
    ) order by id_atendimento_modulo, id_familia, data_atendimento, nome_unidade, tipo_unidade, email) as familias_atendidas
from associacoes
group by id_usuario
{% endmacro %}

{% test pu_familias_atendidas_unicas(model) %}
select p.usuario.id_usuario, f.id_atendimento_modulo, f.id_familia
from {{ model }} as p, unnest(p.atendimentos.familias_atendidas) as f
group by 1, 2, 3
having count(*) != 1 or nullif(f.id_familia, '') is null or nullif(f.id_atendimento_modulo, '') is null
{% endtest %}

{% test pu_familias_atendidas_paridade(model) %}
-- Compara conteúdo e multiplicidade independentemente do agrupamento da macro.
with esperado as (
    select distinct
        cast(id_usuario as string) as id_usuario,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        cast(id_familia as string) as id_familia,
        coalesce(cast(data_de_atendimento as string), '') as data_atendimento,
        coalesce(unidade_atendimento, '') as nome_unidade,
        coalesce(tipo_unidade, '') as tipo_unidade,
        coalesce(email, '') as email,
        case
            when tipo_unidade in ('CRAS', 'CREAS', 'CENTRO POP') and unidade_atendimento != 'Negado' then 'S'
            else 'N'
        end as elegivel_indicador
    from {{ ref('int_atendimentos_bi') }}
    where id_familia is not null
), obtido as (
    select p.usuario.id_usuario, f.*
    from {{ model }} as p, unnest(p.atendimentos.familias_atendidas) as f
), comparacao as (
    select to_json_string(e) as conteudo, 1 as delta from esperado as e
    union all
    select to_json_string(o), -1 from obtido as o
)
select conteudo
from comparacao
group by conteudo
having sum(delta) != 0
{% endtest %}
