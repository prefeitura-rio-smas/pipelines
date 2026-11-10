-- Mart: Prontuário Carioca para PIC
-- Grão: 1 linha por família
--
-- Universo (filtro duplo):
--   PIC: famílias com projeto social PIC ativo ou evolução aba 20, desde que o
--        responsável familiar tenha CPF (comportamento histórico). Família PIC
--        sem criança 0-6 permanece com membros = [].
--   Novas origens: famílias fora do PIC com ao menos uma criança 0-6 que tenha
--        violação de direito e/ou situação de rua (qualquer CPF).
--
-- Origens de identificação (origem_identificacao):
--   1. Projeto social PIC (id_projeto_social = 2) via dim_familias.projetos_sociais
--   2. Evolução aba Pequenos Cariocas (codigo_abrangencia = 20) via fct_evolucoes
--   3. Violação de direito (criança 0-6 com flag_possui_violacao_direito = 'Sim')
--   4. Situação de rua (criança 0-6 com flag_situacao_rua = 'Sim')
--
-- Membros:
--   PIC: crianças 0-6 com CPF (comportamento histórico).
--   Novas origens: crianças 0-6, com ou sem CPF.
--   Cada struct traz indicador_violacao_direito e indicador_situacao_rua da criança.
--   Via dim_usuarios.id_familia (vínculo ativo mais recente).
-- Indicadores de violação/rua são no nível da criança 0-6 (LOGICAL_OR por família);
-- os demais indicadores seguem a família.
-- Busca Ativa: coluna busca_ativa = ARRAY<STRUCT> com TODAS as buscas ativas
--   feitas pela família (todas as evoluções do formulário
--   '1. Pequenos Cariocas - Busca Ativa', codigo_abrangencia=20), em ordem
--   cronológica (data_evolucao, id_evolucao). Cada struct = 1 busca, com
--   campos: data (DATE - NULL se a data digitada for inválida, ex. 06/08/25026;
--   a evolução é PRESERVADA no array com data NULL, não descartada),
--   tipo (ARRAY), familia_localizada (BOOL), motivo_nao_localizada (ARRAY),
--   protocolo_violado (ARRAY). NÃO existe mais 'última busca' - a lista expõe
--   o histórico completo; possui_busca deriva de ARRAY_LENGTH(busca_ativa) > 0.
-- Versão _dev para validação

{{ config(
    schema="pic",
    alias="mart_pic_prontuario_carioca",
    materialized="table",
    tags = ['daily']
) }}

with

-- ORIGEM 1: Projeto social PIC
familias_projeto_social as (
    select distinct f.id_familia
    from {{ ref('dim_familias') }} f,
    unnest(f.projetos_sociais) as p
    where p.id_projeto_social = 2
      and p.indicador_ativo = 'S'
      and p.data_cancelamento is null
),

-- ORIGEM 2: Evolução aba Pequenos Cariocas
familias_evolucao_aba20 as (
    select distinct e.id_familia
    from {{ ref('fct_evolucoes') }} e
    where e.codigo_abrangencia = 20
      and e.id_familia is not null
),

-- Universo PIC (projeto social + aba 20), sem filtro de idade/CPF
familias_pic as (
    select id_familia from familias_projeto_social
    union distinct
    select id_familia from familias_evolucao_aba20
),

-- Crianças 0-6 com os tratamentos do usuário; flags no nível da criança
criancas_0_6 as (
    select
        u.id_familia,
        u.id_usuario,
        u.nome,
        u.data_nascimento,
        u.cpf,
        coalesce(u.flag_possui_violacao_direito = 'Sim', false) as indicador_violacao_direito,
        coalesce(u.flag_situacao_rua = 'Sim', false) as indicador_situacao_rua,
        u.violacoes
    from {{ ref('dim_usuarios') }} as u
    where
        u.id_familia is not null
        and {{ calc_idade('u.data_nascimento') }} between 0 and 6
),

-- Indicadores por família, restritos às crianças 0-6
flags_criancas as (
    select
        id_familia,
        logical_or(indicador_violacao_direito) as indicador_violacao_direito,
        logical_or(indicador_situacao_rua) as indicador_situacao_rua
    from criancas_0_6
    group by id_familia
),

-- UNIÃO das origens de identificação (PIC, aba 20, violação, situação de rua)
familias_uniao as (
    select
        id_familia,
        'projeto_social' as origem
    from familias_projeto_social
    union all
    select
        id_familia,
        'evolucao_aba_20' as origem
    from familias_evolucao_aba20
    union all
    select
        id_familia,
        'violacao_direito' as origem
    from flags_criancas
    where indicador_violacao_direito
    union all
    select
        id_familia,
        'situacao_rua' as origem
    from flags_criancas
    where indicador_situacao_rua
),

-- Agrega origens por família
familias_origens as (
    select
        id_familia,
        array_agg(distinct origem order by origem) as origem_identificacao
    from familias_uniao
    group by id_familia
),

-- Responsável familiar
responsavel_familiar as (
    select
        f.id_familia,
        struct(
            r.cpf as cpf,
            r.nome as nome
        ) as responsavel_familiar
    from {{ ref('dim_familias') }} f
    left join {{ ref('dim_usuarios') }} r
        on f.id_usuario_responsavel = r.id_usuario
),

-- Ramo PIC: mesma regra histórica (responsável com CPF)
familias_pic_com_rf as (
    select p.id_familia
    from familias_pic as p
    inner join responsavel_familiar as rf on p.id_familia = rf.id_familia
    where rf.responsavel_familiar.cpf is not null
),

-- Novas origens: fora do PIC, com criança 0-6 com violação e/ou situação de rua
familias_novas as (
    select f.id_familia
    from flags_criancas as f
    left join familias_pic as p on f.id_familia = p.id_familia
    where
        p.id_familia is null
        and (f.indicador_violacao_direito or f.indicador_situacao_rua)
),

-- Universo final: PIC histórico + novas origens com criança 0-6
familias_universo as (
    select id_familia from familias_pic_com_rf
    union distinct
    select id_familia from familias_novas
),

-- Membros: PIC exige CPF da criança; novas origens aceitam sem CPF
membros_familia as (
    select
        c.id_familia,
        array_agg(
            struct(
                c.nome as nome,
                {{ calc_idade('c.data_nascimento') }} as idade,
                c.cpf as cpf,
                c.indicador_violacao_direito,
                c.indicador_situacao_rua
            )
            order by c.data_nascimento, c.nome
        ) as membros
    from criancas_0_6 as c
    left join familias_pic as p on c.id_familia = p.id_familia
    where
        p.id_familia is null
        or (c.cpf is not null and c.cpf != '')
    group by c.id_familia
),

-- Descrições de violações por família, restritas às crianças 0-6
violacoes_descricoes as (
    select
        c.id_familia,
        array_agg(distinct v.descricao ignore nulls) as violacao_direito
    from criancas_0_6 as c,
        unnest(c.violacoes) as v
    group by c.id_familia
),

-- Filiação documental (última evolução codigo_abrangencia=24 por família)
filiacao_familia as (
    {{ extrair_formulario(
        source_relation = ref('fct_evolucoes'),
        group_cols = ['id_familia'],
        codigo_abrangencia = 24,
        titulo_formulario = 'Documentação Civil',
        latest_by = 'data_evolucao',
        campos = [
            {'label': '%Há interesse%', 'col': 'interesse_filiacao_completa', 'type': 'exists'},
            {'label': '%Filiação completa%', 'col': 'possui_filiacao_completa', 'type': 'boolean', 'true_value': 'Sim'},
        ]
    ) }}
),

-- Encaminhamentos do formulário Documentação Civil (codigo_abrangencia=24)
documentacao_civil_encaminhamentos as (
    {{ extrair_formulario(
        source_relation = ref('fct_evolucoes'),
        group_cols = ['id_familia'],
        codigo_abrangencia = 24,
        titulo_formulario = 'Documentação Civil',
        campos = [
            {'label': '%Encaminhamentos%', 'col': 'encaminhamentos_documentacao_civil', 'type': 'array_agg'},
        ]
    ) }}
),

-- Buscas Ativas Pequenos Cariocas: UMA LINHA POR EVOLUÇÃO do formulário
-- '1. Pequenos Cariocas - Busca Ativa' (codigo_abrangencia=20), pivotando os
-- campos do formulário (parser HTML). Data digitada inválida (ex.: 06/08/25026)
-- vira NULL NAQUELA evolução via safe.parse_date - a evolução NÃO é descartada.
buscas_ativas_evolucoes as (
    select
        id_familia,
        id_evolucao,
        data_evolucao,
        safe.parse_date('%d/%m/%Y', data_raw) as data,
        array(
            select trim(x)
            from unnest(split(ifnull(tipo_raw, ''), ',')) as x
            where trim(x) != ''
        ) as tipo,
        familia_localizada_raw = 'Sim' as familia_localizada,
        array(
            select trim(x)
            from unnest(split(ifnull(motivo_raw, ''), ',')) as x
            where trim(x) != ''
        ) as motivo_nao_localizada,
        array(
            select trim(x)
            from unnest(split(ifnull(protocolo_raw, ''), ',')) as x
            where trim(x) != ''
        ) as protocolo_violado
    from (
        select
            id_familia,
            id_evolucao,
            data_evolucao,
            max(case when label like '%Data em que a Busca Ativa%' then valor end) as data_raw,
            max(case when label like '%Tipo de busca ativa%' then valor end) as tipo_raw,
            max(case when label like '%Família localizada%' then valor end) as familia_localizada_raw,
            max(case when label like '%Se não, por quê%' then valor end) as motivo_raw,
            max(case when label like '%Protocolo violado%' then valor end) as protocolo_raw
        from {{ extrair_campos_html_evolucao(
            source_relation = ref('fct_evolucoes'),
            id_cols = ['id_evolucao', 'id_familia', 'data_evolucao'],
            col_html = 'descricao_evolucao',
            extra_where = 'codigo_abrangencia = 20'
        ) }}
        where titulo_formulario = '1. Pequenos Cariocas - Busca Ativa'
        group by id_familia, id_evolucao, data_evolucao
    )
),

-- Agrega TODAS as buscas por família em ARRAY<STRUCT>, em ordem cronológica
-- (data_evolucao, id_evolucao). Shape idiomático BigQuery: array de structs
-- direto no valor da coluna (leitura "SELECT família, busca_ativa -> lista").
-- BigQuery não suporta ARRAY<ARRAY>; os campos multi-valor (tipo, motivo,
-- protocolo) são ARRAY<STRING> DENTRO do struct de cada busca - permitido.
buscas_ativas_por_familia as (
    select
        id_familia,
        array_agg(
            struct(
                data,
                tipo,
                familia_localizada,
                motivo_nao_localizada,
                protocolo_violado
            )
            order by data_evolucao, id_evolucao
        ) as busca_ativa
    from buscas_ativas_evolucoes
    group by id_familia
),

-- Junção final
final as (
    select
        rf.responsavel_familiar,
        o.origem_identificacao,
        coalesce(mf.membros, []) as membros,
        coalesce(fi.indicador_violacao_direito, false) as indicador_violacao_direito,
        coalesce(fi.indicador_situacao_rua, false) as indicador_situacao_rua,
        coalesce(vd.violacao_direito, []) as violacao_direito,
        ff.possui_filiacao_completa,
        coalesce(ff.interesse_filiacao_completa, false) as interesse_filiacao_completa,
        coalesce(array_length(dce.encaminhamentos_documentacao_civil) > 0, false) as possui_encaminhamento_documentacao_civil,
        coalesce(dce.encaminhamentos_documentacao_civil, []) as encaminhamentos_documentacao_civil,
        coalesce(baf.busca_ativa, []) as busca_ativa,
        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
    from familias_universo as fo
    left join familias_origens as o on fo.id_familia = o.id_familia
    left join membros_familia as mf on fo.id_familia = mf.id_familia
    left join responsavel_familiar as rf on fo.id_familia = rf.id_familia
    left join flags_criancas as fi on fo.id_familia = fi.id_familia
    left join violacoes_descricoes as vd on fo.id_familia = vd.id_familia
    left join filiacao_familia as ff on fo.id_familia = ff.id_familia
    left join documentacao_civil_encaminhamentos as dce on fo.id_familia = dce.id_familia
    left join buscas_ativas_por_familia as baf on fo.id_familia = baf.id_familia
)

select * from final
