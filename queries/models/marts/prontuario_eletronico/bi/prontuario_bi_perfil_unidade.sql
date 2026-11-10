{{ config(materialized='table', enabled=target.name != 'prod', tags=['prontuario_bi_unidade']) }}
with ciclos as (
    select
        cast(p.id_usuario as string) as id_usuario,
        h.id_unidade,
        h
    from {{ ref('alta_complexidade') }} as p, unnest(p.historico) as h
    where h.id_unidade is not null
    qualify row_number() over (partition by p.id_usuario, h.id_unidade order by if(h.data_saida is null, 0, 1), h.data_entrada desc nulls last, h.id_ciclo desc, to_json_string(h) desc) = 1
),

universo as (
    select
        cast(id_usuario as string) as id_usuario,
        id_unidade
    from {{ ref('prontuario_bi_atendimentos') }}
    union distinct
    select
        cast(id_usuario as string) as id_usuario,
        id_unidade
    from {{ ref('prontuario_bi_evolucoes') }}
    union distinct
    select
        id_usuario,
        id_unidade
    from {{ ref('prontuario_bi_centro_pop') }}
    union distinct
    select
        id_usuario,
        id_unidade
    from {{ ref('prontuario_bi_creas') }}
    union distinct
    select
        id_usuario,
        id_unidade
    from ciclos
),

quantidades as (
    select
        cast(id_usuario as string) as id_usuario,
        id_unidade,
        count(*) as quantidade_atendimentos,
        sum(quantidade_linhas_bi) as quantidade_linhas_bi
    from {{ ref('prontuario_bi_atendimentos') }}
    group by 1, 2
),

evolucoes as (
    select
        cast(id_usuario as string) as id_usuario,
        id_unidade,
        count(*) as quantidade_evolucoes
    from {{ ref('prontuario_bi_evolucoes') }}
    group by 1, 2
)

select
    u.id_usuario,
    u.id_unidade,
    d.nome_unidade,
    c.centro_pop,
    r.creas,
    struct(a.h.id_ciclo, a.h.id_unidade, a.h.nome_unidade, a.h.data_entrada, a.h.data_saida, a.h.motivo_saida, a.h.tempo_permanencia_dias) as alta_complexidade,
    struct(
        u.id_usuario as id_usuario,
        cad.usuario.nome as nome,
        cad.usuario.nome_social as nome_social,
        cad.usuario.filiacao_mae as filiacao_mae,
        cad.usuario.nacionalidade as nacionalidade,
        cad.usuario.condicao_estrangeira as condicao_estrangeira,
        cad.usuario.pais_origem as pais_origem,
        cad.usuario.cpf as cpf,
        cad.usuario.data_nascimento as data_nascimento,
        cad.usuario.sexo as sexo,
        cad.usuario.raca_cor as raca_cor,
        cad.usuario.genero as genero,
        cad.usuario.bairro as bairro,
        cad.usuario.prontuario as prontuario,
        cad.usuario.flag_cadunico as flag_cadunico,
        cad.usuario.pontuacao as pontuacao,
        cad.usuario.grau_dependencia as grau_dependencia,
        cad.usuario.nome_apoiador as nome_apoiador,
        cad.usuario.numero_processo_decisao_apoiada as numero_processo_decisao_apoiada,
        cad.usuario.renda_ativa as renda_ativa,
        cad.usuario.atvd_remunerada as atvd_remunerada,
        cad.usuario.flag_decisao_apoiada as flag_decisao_apoiada,
        cad.usuario.flag_saude_mental_comprometida as flag_saude_mental_comprometida,
        cad.usuario.motivo_acolhimento as motivo_acolhimento,
        cad.usuario.orientacao_sexual as orientacao_sexual,
        cad.usuario.vinculo_trabalhista as vinculo_trabalhista,
        cad.usuario.flag_trabalha as flag_trabalha,
        cad.usuario.profissao as profissao,
        cad.usuario.flag_frequenta_escola as flag_frequenta_escola,
        cad.usuario.escolaridade_indice as escolaridade_indice,
        cad.usuario.flag_recebe_beneficio as flag_recebe_beneficio,
        cad.usuario.flag_curatela as flag_curatela,
        cad.usuario.tipo_curatela as tipo_curatela,
        cad.usuario.flag_deficiencia as flag_deficiencia,
        cad.usuario.flag_situacao_rua as flag_situacao_rua,
        cad.usuario.origem_demanda as origem_demanda,
        cad.usuario.flag_possui_violacao_direito as flag_possui_violacao_direito,
        cad.usuario.uf_nascimento as uf_nascimento,
        cad.usuario.serie_escolar as serie_escolar,
        cad.usuario.data_cadunico_informada as data_cadunico_informada,
        cad.usuario.nis as nis,
        cad.usuario.numero_rg as numero_rg,
        cad.usuario.possui_rg as possui_rg,
        cad.usuario.possui_certidao_nascimento as possui_certidao_nascimento,
        cad.usuario.numero_certidao_nascimento as numero_certidao_nascimento,
        cad.usuario.data_atualizacao as data_atualizacao,
        cad.usuario.data_limite_cadastro_atual as data_limite_cadastro_atual,
        cad.usuario.flag_familia_ambigua as flag_familia_ambigua,
        cad.usuario.flag_nis_ambiguo as flag_nis_ambiguo,
        cad.usuario.flag_rg_ambiguo as flag_rg_ambiguo,
        cad.usuario.flag_certidao_ambigua as flag_certidao_ambigua,
        cad.usuario.logradouro as logradouro,
        cad.usuario.numero as numero,
        cad.usuario.complemento as complemento,
        cad.usuario.referencia_ou_comunidade as referencia_ou_comunidade,
        cad.usuario.idade as idade,
        cad.usuario.tipo_atividade_remunerada as tipo_atividade_remunerada,
        cad.usuario.possui_cpf as possui_cpf,
        cad.usuario.idade_faixa as idade_faixa
    ) as usuario,
    coalesce(q.quantidade_atendimentos, 0) as quantidade_atendimentos,
    coalesce(q.quantidade_linhas_bi, 0) as quantidade_linhas_bi,
    coalesce(e.quantidade_evolucoes, 0) as quantidade_evolucoes
from universo as u
left join {{ pu_ref('dim_unidades') }} as d on u.id_unidade = d.id_unidade
left join {{ ref('prontuario_eletronico') }} as cad on u.id_usuario = cad.usuario.id_usuario
left join quantidades as q on u.id_usuario = q.id_usuario and u.id_unidade = q.id_unidade
left join evolucoes as e on u.id_usuario = e.id_usuario and u.id_unidade = e.id_unidade
left join {{ ref('prontuario_bi_centro_pop') }} as c on u.id_usuario = c.id_usuario and u.id_unidade = c.id_unidade
left join {{ ref('prontuario_bi_creas') }} as r on u.id_usuario = r.id_usuario and u.id_unidade = r.id_unidade
left join ciclos as a on u.id_usuario = a.id_usuario and u.id_unidade = a.id_unidade
