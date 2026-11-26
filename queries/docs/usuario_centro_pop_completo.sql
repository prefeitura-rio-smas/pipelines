-- BigQuery (GoogleSQL). Consulta de conferência para quem já tem acesso à tabela.
-- Preencha usuario_informado com o ID retornado por usuario_duas_unidades.sql
-- para fixar o mesmo usuário. Com NULL, a consulta repete o critério de seleção.
declare usuario_informado string default null;

with unidades_por_usuario as (
    select
        p.usuario.id_usuario as id_usuario,
        a.id_unidade,
        max(safe_cast(a.data_atendimento_informada as date)) as ultimo_atendimento
    from `rj-smas-dev.abordagem.planilha_unificada` as p
    cross join unnest(p.atendimentos.registros) as a
    where
        a.tipo_servico = 'centro_pop'
        and nullif(trim(p.usuario.id_usuario), '') is not null
        and nullif(trim(a.id_unidade), '') is not null
        and (usuario_informado is null or p.usuario.id_usuario = usuario_informado)
    group by p.usuario.id_usuario, a.id_unidade
),

usuario_alvo as (
    select id_usuario
    from unidades_por_usuario
    group by id_usuario
    having usuario_informado is not null or count(*) = 2
    order by max(ultimo_atendimento) desc, id_usuario
    limit 1
)

-- Mantém as structs e os arrays no formato original da mart.
select p.*
from `rj-smas-dev.abordagem.planilha_unificada` as p
inner join usuario_alvo as alvo on p.usuario.id_usuario = alvo.id_usuario
inner join unidades_por_usuario as u
    on p.usuario.id_usuario = u.id_usuario and p.id_unidade = u.id_unidade
order by p.id_unidade;
