-- Consulta de apoio à migração do BI: uma linha por atendimento/profissional.
-- Fonte publicada em desenvolvimento. Não cria nem substitui tabelas.
-- IDADE usa a data da consulta, como a regra original da programada.
-- PROJETOS_SOCIAIS_USUARIO tem escopo por usuário; ver a matriz de equivalência.
with base as (
    select
        a.id_atendimento_modulo,
        a.nome_unidade as unidade_atendimento,
        a.tipo_atendimento_descricao as nome_atendimento_original,
        nullif(a.cas, '') as cas,
        nullif(a.email_unidade, '') as email_unidade,
        nullif(a.email_cas, '') as email_cas,
        nullif(a.email, '') as email,
        nullif(p.usuario.logradouro, '') as endereco,
        nullif(p.usuario.numero, '') as endereco_numero,
        nullif(p.usuario.complemento, '') as endereco_complemento,
        nullif(p.usuario.referencia_ou_comunidade, '') as referencia_ou_comunidade,
        safe_cast(nullif(prof.id_familia_referencia, '') as int64) as id_familia,
        safe_cast(nullif(prof.id_profissional, '') as int64) as profissional_id,
        nullif(prof.id_atendimento_legado, '') as id_atendimento,
        nullif(a.cadastrante, '') as cadastrante,
        safe_cast(nullif(a.data_cadastro_atendimento, '') as date) as data_cadastro_atendimento,
        case a.origem_modulo when 'usuario' then 'u' when 'familia' then 'f' end as modulo,
        case
            when a.nome_unidade like 'ALBERGUE%' then 'ALBERGUE'
            when a.nome_unidade like 'CRAF%' then 'CENTRAL DE RECEPÇÃO'
            when a.nome_unidade like 'CRAS%' then 'CRAS'
            when a.nome_unidade like 'CREAS%' then 'CREAS'
            when a.nome_unidade like 'CRI%' then 'CENTRAL DE RECEPÇÃO'
            when a.nome_unidade like 'REPÚBLICA%' then 'REPÚBLICA'
            when a.nome_unidade like 'URS%' then 'URS'
            when a.nome_unidade like 'CENTRO POP%' then 'CENTRO POP'
            when a.nome_unidade like 'ESCRITÓRIO%' then 'ESCRITÓRIO SOCIAL'
            when a.nome_unidade like 'LARES%' then 'LARES CARIOCAS'
            else 'UNIDADE CONVENIADA'
        end as tipo_unidade,
        safe_cast(regexp_extract(a.id_atendimento_modulo, r'^(\d+)[uf]$') as int64) as seq_atendimento,
        safe_cast(nullif(a.data_atendimento, '') as date) as data_de_atendimento,
        safe_cast(replace(nullif(a.hora_atendimento, ''), ':', '') as int64) as hora_de_atendimento_original,
        nullif(a.hora_atendimento, '') as hora_de_atendimento,
        safe_cast(p.usuario.id_usuario as int64) as id_usuario,
        nullif(p.usuario.nome, '') as nome_usuario,
        case when p.usuario.cpf = '' then 'CPF não informado' else 'CPF informado' end as documentacao,
        case when p.usuario.cpf != '' then concat(
            substr(lpad(p.usuario.cpf, 11, '0'), 1, 3), '.',
            substr(lpad(p.usuario.cpf, 11, '0'), 4, 3), '.',
            substr(lpad(p.usuario.cpf, 11, '0'), 7, 3), '-',
            substr(lpad(p.usuario.cpf, 11, '0'), 10, 2)
        ) end as numero_documento,
        case when p.usuario.bairro = '' then 'NAO INFORMADO' else p.usuario.bairro end as bairro,
        safe_cast(nullif(p.usuario.data_nascimento, '') as date) as data_nascimento,
        case p.usuario.raca_cor
            when 'Amarelo' then 'Amarela'
            when 'Indígena' then 'Indigena'
            when '' then 'Nao informado'
            else p.usuario.raca_cor
        end as raca_cor,
        nullif(p.usuario.sexo, '') as sexo,
        nullif(prof.nome_profissional, '') as profissional,
        nullif(prof.cbo_principal_descricao, '') as profissional_cbo_original,
        (
            select string_agg(distinct ps.nome_projeto_social, ', ' order by ps.nome_projeto_social)
            from unnest(p.projetos_sociais) as ps
            where ps.indicador_ativo = 'S' and ps.data_cancelamento = ''
        ) as projetos_sociais_usuario
    from `rj-smas-dev.relatorio.planilha_unificada` as p
    cross join unnest(p.atendimentos.registros) as a
    left join unnest(a.profissionais) as prof on true
),

normalizado as (
    select
        *,
        date_diff(current_date(), data_nascimento, year)
        - if(extract(dayofyear from data_nascimento) > extract(dayofyear from current_date()), 1, 0) as idade,
        case
            when profissional_cbo_original like 'Articulador Comunitário%' then 'Articulador Comunitário'
            when profissional_cbo_original like 'Assistente administrativo%' then 'Assistente administrativo'
            when profissional_cbo_original like 'Assistente Social%' then 'Assistente social'
            when profissional_cbo_original like 'Assistente social%' then 'Assistente social'
            when profissional_cbo_original like 'Educador social%' then 'Educador social'
            when profissional_cbo_original like 'Entrevistador Social%' then 'Entrevistador social'
            when profissional_cbo_original like 'Pedagogo%' then 'Pedagogo'
            when profissional_cbo_original like 'Psicólogo%' then 'Psicólogo'
            when profissional_cbo_original like 'Recepcionista%' then 'Recepcionista'
            else profissional_cbo_original
        end as profissional_cbo
    from base
),

apresentacao as (
    select
        *,
        1 as cbo_unico_rank,
        1 as atendimento_unico_rank,
        case
            when nome_atendimento_original like '%Recepção%' then 'Atendimento Recepção'
            when profissional = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
            when
                profissional_cbo in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                and nome_atendimento_original like '%CadÚnico%' then 'Atendimento Recepção'
            when profissional_cbo in ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo') then 'Atendimento Técnico'
            else 'Outros Atendimentos'
        end as tipo_atendimento,
        case
            when
                profissional_cbo in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                and tipo_unidade in ('CRAS', 'CREAS') and nome_atendimento_original like '%CadÚnico%'
                then concat(tipo_unidade, ' - Recepção - Ação CadÚnico')
            else nome_atendimento_original
        end as nome_atendimento,
        case
            when idade < 18 then 'Até 17 anos'
            when idade < 30 then 'De 18 a 29 anos'
            when idade < 45 then 'De 30 a 44 anos'
            when idade < 60 then 'De 45 a 59 anos'
            when idade < 75 then 'De 60 a 74 anos'
            when idade >= 75 then 'Mais de 75 anos'
            else 'Não Informado'
        end as idade_faixa
    from normalizado
)

-- Preserva os aliases públicos da fonte anterior para o conector do BI.
-- noqa: disable=RF06
select
    id_atendimento_modulo as `ID_ATENDIMENTO_MODULO`,
    modulo,
    cas as `CAS`,
    tipo_unidade as `TIPO_UNIDADE`,
    unidade_atendimento as `UNIDADE_ATENDIMENTO`,
    email_unidade as `EMAIL_UNIDADE`,
    email_cas as `EMAIL_CAS`,
    seq_atendimento as `SEQ_ATENDIMENTO`,
    data_de_atendimento as `DATA_DE_ATENDIMENTO`,
    hora_de_atendimento_original as `HORA_DE_ATENDIMENTO_ORIGINAL`,
    hora_de_atendimento as `HORA_DE_ATENDIMENTO`,
    id_usuario as `ID_USUARIO`,
    id_familia as `ID_FAMILIA`,
    nome_usuario as `NOME_USUARIO`,
    documentacao as `DOCUMENTACAO`,
    numero_documento as `NUMERO_DOCUMENTO`,
    endereco as `ENDERECO`,
    endereco_numero as `ENDERECO_NUMERO`,
    endereco_complemento as `ENDERECO_COMPLEMENTO`,
    bairro as `BAIRRO`,
    referencia_ou_comunidade as `REFERENCIA_OU_COMUNIDADE`,
    nome_atendimento_original as `NOME_ATENDIMENTO_ORIGINAL`,
    data_nascimento as `DATA_NASCIMENTO`,
    idade as `IDADE`,
    raca_cor as `RACA_COR`,
    sexo as `SEXO`,
    profissional_id as `PROFISSIONAL_ID`,
    profissional as `PROFISSIONAL`,
    profissional_cbo_original as `PROFISSIONAL_CBO_ORIGINAL`,
    profissional_cbo as `PROFISSIONAL_CBO`,
    cadastrante as `CADASTRANTE`,
    data_cadastro_atendimento as `DATA_CADASTRO_ATENDIMENTO`,
    tipo_atendimento as `TIPO_ATENDIMENTO`,
    nome_atendimento as `NOME_ATENDIMENTO`,
    idade_faixa as `IDADE_FAIXA`,
    id_atendimento as `ID_ATENDIMENTO`,
    email as `EMAIL`,
    cbo_unico_rank,
    atendimento_unico_rank,
    projetos_sociais_usuario as `PROJETOS_SOCIAIS_USUARIO`
from apresentacao
