-- BigQuery (GoogleSQL).
-- Retorna um usuário com atendimentos em exatamente duas unidades Centro POP.
-- Consulta de conferência para quem já tem acesso à tabela no BigQuery.
with unidades_por_usuario as (
    select
        p.usuario.id_usuario as id_usuario,
        a.id_unidade,
        max(nullif(trim(a.nome_unidade), '')) as nome_unidade,
        max(safe_cast(a.data_atendimento_informada as date)) as ultimo_atendimento
    from `rj-smas-dev.abordagem.planilha_unificada` as p
    cross join unnest(p.atendimentos.registros) as a
    where
        a.tipo_servico = 'centro_pop'
        and nullif(trim(p.usuario.id_usuario), '') is not null
        and nullif(trim(a.id_unidade), '') is not null
    group by p.usuario.id_usuario, a.id_unidade
)

select
    id_usuario,
    array_agg(
        struct(id_unidade, nome_unidade, ultimo_atendimento)
        order by id_unidade
    ) as unidades
from unidades_por_usuario
group by id_usuario
having count(*) = 2
order by max(ultimo_atendimento) desc, id_usuario
limit 1;
