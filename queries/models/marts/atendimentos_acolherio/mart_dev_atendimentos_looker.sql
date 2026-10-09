{{ config(tags=['acolherio_atendimentos_refresh']) }}

-- noqa: disable=CP02,AL09,ST06,RF02
-- A ordem e a grafia dos aliases finais seguem o contrato externo do Looker.
-- Contrato de 39 colunas do Looker. Grão final: atendimento × profissional.
-- Este mart é um candidato isolado; não grava diretamente na tabela do Looker.
with atendimentos as (
    select
        id_atendimento_modulo,
        id_atendimento,
        id_usuario,
        id_familia,
        id_unidade,
        id_profissional,
        id_login_cadastro,
        id_tipo_atendimento,
        data_atendimento,
        data_cadastro_atendimento,
        hora_atendimento,
        origem_modulo
    from {{ ref('fct_atendimentos') }}
),

familia_usuario as (
    select
        id_paciente,
        id_familia,
        data_saida
    from {{ ref('raw_membros_familia') }}
    qualify row_number() over (
        partition by id_paciente
        order by data_saida desc nulls first, data_entrada desc, id_familia desc
    ) = 1
),

profissionais_cbo as (
    select
        ocupacao.id_profissional,
        array_agg(cbo.descricao ignore nulls order by ocupacao.codigo_cbo)[safe_offset(0)] as cbo_original
    from {{ ref('raw_profissionais_ocupacoes') }} as ocupacao
    left join {{ ref('raw_cbo') }} as cbo on ocupacao.codigo_cbo = cbo.codigo_cbo
    group by ocupacao.id_profissional
),

catalogo_projetos_sociais as (
    select
        id_projeto_social,
        nome_projeto_social
    from unnest(array<struct<id_projeto_social int64, nome_projeto_social string>>[
            (1, 'Territórios Sociais'),
            (2, 'Pequenos Cariocas'),
            (3, 'MCMV - Minha Casa Minha Vida'),
            (4, 'Alimenta Rio'),
            (5, 'Banco de Alimentos/Cozinha Comunitária'),
            (6, 'Banco Carioca de Bolsa de Estudos'),
            (7, 'Projetos de Empreendedorismo'),
            (8, 'PRONASCI Juventude'),
            (9, 'Agente Experiente'),
            (10, 'Amparando Filhos'),
            (11, 'Bora pra Escola'),
            (12, 'DAM+'),
            (13, 'De Mãos Dadas'),
            (14, 'De Volta a Terra Natal'),
            (15, 'Escritório Social'),
            (16, 'Garupa'),
            (17, 'Guarda Subsidiada'),
            (18, 'Jovem Aprendiz'),
            (19, 'Lares Cariocas'),
            (20, 'Moradia com Apoio'),
            (21, 'Novos Rumos'),
            (22, 'Passo a Passo'),
            (23, 'Penas e Medidas Alternativas'),
            (24, 'PETI - Errad. Trab. Infantil'),
            (25, 'Projetos de Qualificação Profissional'),
            (26, 'Reinserção familiar ou comunitária'),
            (27, 'SUAS Acolhendo Talentos'),
            (28, 'Vem com a gente'),
            (29, 'Rio Dignidade'),
            (30, 'Idoso em Família'),
            (31, 'Projovem Urbano'),
            (32, 'Tô de Boa')
    ])
),

projetos_familia as (
    select
        vinculo.id_familia,
        string_agg(
            distinct coalesce(
                catalogo.nome_projeto_social,
                format('Projeto não mapeado (ID %d)', vinculo.id_projeto_social)
            ),
            ', ' order by coalesce(
                catalogo.nome_projeto_social,
                format('Projeto não mapeado (ID %d)', vinculo.id_projeto_social)
            )
        ) as projetos_sociais
    from {{ ref('raw_familias_projetos_sociais') }} as vinculo
    left join catalogo_projetos_sociais as catalogo
        on vinculo.id_projeto_social = catalogo.id_projeto_social
    where vinculo.indicador_ativo = 'S' and vinculo.data_cancelamento is null
    group by vinculo.id_familia
),

base as (
    select
        case a.origem_modulo when 'usuario' then 'u' else 'f' end as modulo,
        unidade.cas,
        case
            when unidade.nome_unidade like 'CENTRO POP%' then 'CENTRO POP'
            else {{ map_tipo_unidade('unidade.nome_unidade') }}
        end as tipo_unidade,
        unidade.nome_unidade as unidade_atendimento,
        unidade.email_unidade,
        {{ email_cas('unidade.cas') }} as email_cas,
        a.id_atendimento as seq_atendimento,
        a.data_atendimento as data_de_atendimento,
        safe_cast(a.hora_atendimento as int64) as hora_de_atendimento_original,
        a.id_usuario,
        coalesce(a.id_familia, familia.id_familia) as id_familia,
        usuario.nome as nome_usuario,
        usuario.cpf,
        usuario.logradouro,
        usuario.numero_endereco,
        usuario.complemento_endereco,
        usuario.bairro,
        usuario.ponto_referencia,
        tipo.descricao as nome_atendimento_original,
        usuario.data_nascimento,
        {{ calc_idade('usuario.data_nascimento') }} as idade,
        usuario.raca_cor,
        usuario.sexo,
        a.id_profissional as profissional_id,
        case
            when trim(upper(profissional.nome)) in (
                'ATENDIMENTO RECEPÇÃO',
                'ATENDIMENTO BUSCA ATIVA',
                'ATENDIMENTO ENTREVISTADOR SOCIAL'
            ) then trim(upper(operador.nome_operador))
            else trim(upper(profissional.nome))
        end as profissional,
        cbo.cbo_original as profissional_cbo_original,
        case
            when cbo.cbo_original like 'Articulador Comunitário%' then 'Articulador Comunitário'
            when cbo.cbo_original like 'Assistente administrativo%' then 'Assistente administrativo'
            when cbo.cbo_original like 'Assistente Social%' then 'Assistente social'
            when cbo.cbo_original like 'Assistente social%' then 'Assistente social'
            when cbo.cbo_original like 'Educador social%' then 'Educador social'
            when cbo.cbo_original like 'Entrevistador Social%' then 'Entrevistador social'
            when cbo.cbo_original like 'Pedagogo%' then 'Pedagogo'
            when cbo.cbo_original like 'Psicólogo%' then 'Psicólogo'
            when cbo.cbo_original like 'Recepcionista%' then 'Recepcionista'
            else cbo.cbo_original
        end as profissional_cbo,
        trim(upper(operador.nome_operador)) as cadastrante,
        a.data_cadastro_atendimento,
        filtro.email as email_filtro
    from atendimentos as a
    inner join {{ ref('raw_usuarios') }} as usuario on a.id_usuario = usuario.id_paciente
    inner join {{ ref('raw_unidades') }} as unidade on a.id_unidade = unidade.id_unidade
    inner join {{ ref('raw_operadores') }} as operador on a.id_login_cadastro = operador.id_login
    left join familia_usuario as familia
        on a.origem_modulo = 'usuario' and a.id_usuario = familia.id_paciente
    left join {{ ref('raw_tipos_atendimento') }} as tipo
        on a.id_tipo_atendimento = tipo.id_tipo_atendimento
    left join {{ ref('raw_profissionais') }} as profissional
        on a.id_profissional = profissional.id_profissional
    left join profissionais_cbo as cbo on a.id_profissional = cbo.id_profissional
    left join {{ ref('raw_sheets_filtro_email_prontuario') }} as filtro
        on unidade.nome_unidade = filtro.unidade_atendimento
    where
        operador.nome_operador not like '%TESTE%'
        and usuario.nome not like '%TESTE%'
        and unidade.nome_unidade not like '%TESTE%'
        -- No SQL vigente, `datsaida` pertence ao vínculo familiar; a data
        -- de saída do atendimento tem o nome distinto `dtsaida`.
        and (a.origem_modulo = 'familia' or familia.data_saida is null)
),

classificados as (
    select
        b.*,
        format(
            '%02d:%02d',
            div(b.hora_de_atendimento_original, 100),
            mod(b.hora_de_atendimento_original, 100)
        ) as hora_de_atendimento,
        case
            when b.cpf is null or b.cpf = ''
                then 'CPF não informado'
            else 'CPF informado'
        end as documentacao,
        case
            when b.cpf is null or b.cpf = '' then null
            else format(
                '%s.%s.%s-%s',
                substr(lpad(b.cpf, 11, '0'), 1, 3),
                substr(lpad(b.cpf, 11, '0'), 4, 3),
                substr(lpad(b.cpf, 11, '0'), 7, 3),
                substr(lpad(b.cpf, 11, '0'), 10, 2)
            )
        end as numero_documento,
        case b.raca_cor
            when '01' then 'Branca'
            when '02' then 'Preta'
            when '03' then 'Parda'
            when '04' then 'Amarela'
            when '05' then 'Indigena'
            else 'Nao informado'
        end as raca_cor_descricao,
        case
            when b.nome_atendimento_original like '%Recepção%' then 'Atendimento Recepção'
            when b.profissional = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
            when
                b.profissional_cbo in (
                    'Administrador', 'Articulador Comunitário', 'Assistente administrativo',
                    'Educador social', 'Orientador social', 'Recepcionista'
                ) and b.nome_atendimento_original like '%CadÚnico%'
                then 'Atendimento Recepção'
            when b.profissional_cbo in ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo')
                then 'Atendimento Técnico'
            else 'Outros Atendimentos'
        end as tipo_atendimento,
        case
            when
                b.profissional_cbo in (
                    'Administrador', 'Articulador Comunitário', 'Assistente administrativo',
                    'Educador social', 'Orientador social', 'Recepcionista'
                ) and b.tipo_unidade = 'CRAS'
                and b.nome_atendimento_original like '%CadÚnico%'
                then 'CRAS - Recepção - Ação CadÚnico'
            when
                b.profissional_cbo in (
                    'Administrador', 'Articulador Comunitário', 'Assistente administrativo',
                    'Educador social', 'Orientador social', 'Recepcionista'
                ) and b.tipo_unidade = 'CREAS'
                and b.nome_atendimento_original like '%CadÚnico%'
                then 'CREAS - Recepção - Ação CadÚnico'
            else b.nome_atendimento_original
        end as nome_atendimento,
        case
            when b.idade < 18 then 'Até 17 anos'
            when b.idade < 30 then 'De 18 a 29 anos'
            when b.idade < 45 then 'De 30 a 44 anos'
            when b.idade < 60 then 'De 45 a 59 anos'
            when b.idade < 75 then 'De 60 a 74 anos'
            when b.idade >= 75 then 'Mais de 75 anos'
            else 'Não Informado'
        end as idade_faixa,
        concat(b.seq_atendimento, '-', b.profissional_id) as id_atendimento,
        concat(b.email_cas, ',', b.email_unidade, ',', b.email_filtro) as email
    from base as b
),

numerados as (
    select
        c.*,
        row_number() over (
            partition by c.id_atendimento
            order by c.data_cadastro_atendimento, c.id_familia
        ) as cbo_unico_rank,
        row_number() over (
            partition by
                c.profissional_id, c.hora_de_atendimento,
                c.data_de_atendimento, c.nome_atendimento_original,
                c.id_usuario, c.unidade_atendimento
            order by c.data_cadastro_atendimento, c.id_atendimento
        ) as atendimento_unico_rank
    from classificados as c
)

select
    modulo,
    cas as CAS,
    tipo_unidade as TIPO_UNIDADE,
    unidade_atendimento as UNIDADE_ATENDIMENTO,
    email_unidade as EMAIL_UNIDADE,
    email_cas as EMAIL_CAS,
    seq_atendimento as SEQ_ATENDIMENTO,
    data_de_atendimento as DATA_DE_ATENDIMENTO,
    hora_de_atendimento_original as HORA_DE_ATENDIMENTO_ORIGINAL,
    hora_de_atendimento as HORA_DE_ATENDIMENTO,
    id_usuario as ID_USUARIO,
    n.id_familia as ID_FAMILIA,
    nome_usuario as NOME_USUARIO,
    documentacao as DOCUMENTACAO,
    numero_documento as NUMERO_DOCUMENTO,
    trim(regexp_replace(logradouro, r'[,\s;]+', ' ')) as ENDERECO,
    trim(regexp_replace(numero_endereco, r'[,\s;]+', ' ')) as ENDERECO_NUMERO,
    trim(regexp_replace(complemento_endereco, r'[,\s;]+', ' ')) as ENDERECO_COMPLEMENTO,
    case when bairro = '' then 'NAO INFORMADO' else bairro end as BAIRRO,
    trim(regexp_replace(ponto_referencia, r'[,\s;]+', ' ')) as REFERENCIA_OU_COMUNIDADE,
    nome_atendimento_original as NOME_ATENDIMENTO_ORIGINAL,
    data_nascimento as DATA_NASCIMENTO,
    cast(idade as int64) as IDADE,
    raca_cor_descricao as RACA_COR,
    sexo as SEXO,
    profissional_id as PROFISSIONAL_ID,
    profissional as PROFISSIONAL,
    profissional_cbo_original as PROFISSIONAL_CBO_ORIGINAL,
    profissional_cbo as PROFISSIONAL_CBO,
    cadastrante as CADASTRANTE,
    data_cadastro_atendimento as DATA_CADASTRO_ATENDIMENTO,
    tipo_atendimento as TIPO_ATENDIMENTO,
    nome_atendimento as NOME_ATENDIMENTO,
    idade_faixa as IDADE_FAIXA,
    id_atendimento as ID_ATENDIMENTO,
    email as EMAIL,
    cbo_unico_rank,
    atendimento_unico_rank,
    projetos.projetos_sociais as PROJETOS_SOCIAIS
from numerados as n
left join projetos_familia as projetos on n.id_familia = projetos.id_familia
where cbo_unico_rank = 1 and atendimento_unico_rank = 1
