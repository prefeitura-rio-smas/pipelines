{{ config(tags = ['planilha_unificada'], severity = 'warn') }}

-- Sinaliza nomes pendentes sem excluir registros da tabela final.
-- Serviços sem tradução permanecem com nome NULL; projetos têm proteção na projeção.
select distinct
    'servico_assistencial' as catalogo,
    cast(s.id_servico_assistencial as string) as codigo_pendente
from {{ pu_ref('int_pu_familias') }} as p,
    unnest(p.familias) as f,
    unnest(f.servicos) as s
where s.nome_servico is null
union all
select distinct
    'projeto_social' as catalogo,
    cast(ps.id_projeto_social as string) as codigo_pendente
from {{ pu_ref('dim_usuarios') }} as u,
    unnest(u.projetos_sociais) as ps
where {{ map_nome_projeto_social('ps.id_projeto_social') }} is null
