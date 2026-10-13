{{ config(alias='atendimentos', tags=['acolherio_atendimentos_refresh']) }}

-- noqa: disable=CP02,AL09,ST06,RF02
-- As primeiras colunas preservam a ordem e a grafia dos aliases legados.
-- Preserva as 39 colunas legadas e acrescenta atributos para filtros no Looker.
-- Grão final: um atendimento do módulo, com todos os profissionais em um array.
-- Filtros de análise e de deduplicação são aplicados no Looker.
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
        origem_modulo,
        data_saida,
        flag_cancelado,
        flag_profissional_principal
    from {{ ref('int_atendimentos_completos') }}
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

vinculos_familia_usuario as (
    select
        id_paciente,
        array_agg(struct(
            id_membro, id_familia, data_entrada, data_saida,
            id_familia_nova, id_motivo_saida, parentesco_responsavel
        ) order by data_entrada, id_membro) as vinculos_familiares
    from {{ ref('raw_membros_familia') }}
    group by id_paciente
),

filtro_emails as (
    select
        unidade_atendimento,
        string_agg(distinct email, ',' order by email) as email
    from {{ ref('raw_sheets_filtro_email_prontuario') }}
    group by unidade_atendimento
),

profissionais_cbo as (
    select
        ocupacao.id_profissional,
        array_agg(cbo.descricao ignore nulls order by ocupacao.codigo_cbo)[safe_offset(0)] as cbo_original,
        array_agg(struct(ocupacao.codigo_cbo, cbo.descricao) order by ocupacao.codigo_cbo) as cbos
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
            distinct if(
                vinculo.indicador_ativo = 'S' and vinculo.data_cancelamento is null,
                coalesce(
                    catalogo.nome_projeto_social,
                    format('Projeto não mapeado (ID %d)', vinculo.id_projeto_social)
                ),
                null
            ),
            ', ' order by if(
                vinculo.indicador_ativo = 'S' and vinculo.data_cancelamento is null,
                coalesce(
                    catalogo.nome_projeto_social,
                    format('Projeto não mapeado (ID %d)', vinculo.id_projeto_social)
                ),
                null
            )
        ) as projetos_sociais,
        array_agg(struct(
            vinculo.id_projeto_social,
            coalesce(
                catalogo.nome_projeto_social,
                format('Projeto não mapeado (ID %d)', vinculo.id_projeto_social)
            ) as nome_projeto_social,
            vinculo.id_unidade, vinculo.id_login_cadastro,
            vinculo.data_cadastro, vinculo.data_cancelamento, vinculo.indicador_ativo
        ) order by vinculo.id_projeto_social, vinculo.data_cadastro) as projetos_sociais_historico
    from {{ ref('raw_familias_projetos_sociais') }} as vinculo
    left join catalogo_projetos_sociais as catalogo
        on vinculo.id_projeto_social = catalogo.id_projeto_social
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
        a.flag_profissional_principal,
        a.id_atendimento_modulo,
        a.id_unidade,
        a.id_login_cadastro,
        a.id_tipo_atendimento,
        a.flag_cancelado,
        a.data_saida as data_saida_atendimento,
        familia.data_saida as data_saida_vinculo_familiar,
        vinculos.vinculos_familiares,
        cbo.cbos as profissional_cbos,
        profissional.nome as profissional_nome_cadastral,
        usuario.id_paciente is not null as flag_usuario_cadastrado,
        unidade.id_unidade is not null as flag_unidade_cadastrada,
        operador.id_login is not null as flag_operador_cadastrado,
        profissional.id_profissional is not null as flag_profissional_cadastrado,
        coalesce(upper(usuario.nome) like '%TESTE%', false) as flag_usuario_teste,
        coalesce(upper(unidade.nome_unidade) like '%TESTE%', false) as flag_unidade_teste,
        coalesce(upper(operador.nome_operador) like '%TESTE%', false) as flag_operador_teste,
        coalesce(upper(profissional.nome) like '%TESTE%', false) as flag_profissional_teste,
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
    left join {{ ref('raw_usuarios') }} as usuario on a.id_usuario = usuario.id_paciente
    left join {{ ref('raw_unidades') }} as unidade on a.id_unidade = unidade.id_unidade
    left join {{ ref('raw_operadores') }} as operador on a.id_login_cadastro = operador.id_login
    left join familia_usuario as familia
        on a.origem_modulo = 'usuario' and a.id_usuario = familia.id_paciente
    left join {{ ref('raw_tipos_atendimento') }} as tipo
        on a.id_tipo_atendimento = tipo.id_tipo_atendimento
    left join {{ ref('raw_profissionais') }} as profissional
        on a.id_profissional = profissional.id_profissional
    left join profissionais_cbo as cbo on a.id_profissional = cbo.id_profissional
    left join vinculos_familia_usuario as vinculos on a.id_usuario = vinculos.id_paciente
    left join filtro_emails as filtro
        on unidade.nome_unidade = filtro.unidade_atendimento
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
        concat(coalesce(b.email_cas, ''), ',', coalesce(b.email_unidade, ''), ',', coalesce(b.email_filtro, '')) as email
    from base as b
),

detalhes_profissionais as (
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
        email as EMAIL,
        projetos.projetos_sociais as PROJETOS_SOCIAIS,
        id_atendimento_modulo as ID_ATENDIMENTO_MODULO,
        id_unidade as ID_UNIDADE,
        id_login_cadastro as ID_LOGIN_CADASTRO,
        id_tipo_atendimento as ID_TIPO_ATENDIMENTO,
        flag_cancelado as FLAG_CANCELADO,
        data_saida_atendimento as DATA_SAIDA_ATENDIMENTO,
        data_saida_vinculo_familiar as DATA_SAIDA_VINCULO_FAMILIAR,
        vinculos_familiares as VINCULOS_FAMILIARES,
        profissional_cbos as PROFISSIONAL_CBOS,
        profissional_nome_cadastral as PROFISSIONAL_NOME_CADASTRAL,
        email_filtro as EMAIL_PLANILHA,
        flag_usuario_cadastrado as FLAG_USUARIO_CADASTRADO,
        flag_unidade_cadastrada as FLAG_UNIDADE_CADASTRADA,
        flag_operador_cadastrado as FLAG_OPERADOR_CADASTRADO,
        flag_profissional_cadastrado as FLAG_PROFISSIONAL_CADASTRADO,
        flag_usuario_teste as FLAG_USUARIO_TESTE,
        flag_unidade_teste as FLAG_UNIDADE_TESTE,
        flag_operador_teste as FLAG_OPERADOR_TESTE,
        flag_profissional_teste as FLAG_PROFISSIONAL_TESTE,
        projetos.projetos_sociais_historico as PROJETOS_SOCIAIS_HISTORICO,
        flag_profissional_principal
    from classificados as n
    left join projetos_familia as projetos on n.id_familia = projetos.id_familia
),

por_atendimento as (
    select
        ID_ATENDIMENTO_MODULO,
        -- O registro principal sempre é preservado, inclusive quando seu ID é NULL.
        array_agg(d order by flag_profissional_principal desc, PROFISSIONAL_ID asc limit 1)[offset(0)] as principal,
        array_agg(struct(
            PROFISSIONAL_ID as id_profissional,
            PROFISSIONAL as nome,
            PROFISSIONAL_NOME_CADASTRAL as nome_cadastral,
            flag_profissional_principal as flag_principal,
            PROFISSIONAL_CBOS as cbos,
            PROFISSIONAL_CBO_ORIGINAL as cbo_original,
            PROFISSIONAL_CBO as cbo,
            TIPO_ATENDIMENTO as tipo_atendimento,
            NOME_ATENDIMENTO as nome_atendimento,
            FLAG_PROFISSIONAL_CADASTRADO as flag_cadastrado,
            FLAG_PROFISSIONAL_TESTE as flag_teste
        ) order by flag_profissional_principal desc, PROFISSIONAL_ID asc) as profissionais,
        countif(PROFISSIONAL_ID is not null) as quantidade_profissionais,
        logical_or(FLAG_PROFISSIONAL_TESTE) as flag_algum_profissional_teste,
        case
            when countif(TIPO_ATENDIMENTO = 'Atendimento Técnico') > 0 then 'Atendimento Técnico'
            when countif(TIPO_ATENDIMENTO = 'Atendimento Recepção') > 0 then 'Atendimento Recepção'
            else 'Outros Atendimentos'
        end as tipo_atendimento_consolidado
    from detalhes_profissionais as d
    group by ID_ATENDIMENTO_MODULO
),

numerados as (
    select
        a.*,
        row_number() over (
            partition by
                a.principal.ID_USUARIO, a.principal.ID_UNIDADE,
                a.principal.DATA_DE_ATENDIMENTO, a.principal.HORA_DE_ATENDIMENTO,
                a.principal.NOME_ATENDIMENTO_ORIGINAL
            order by a.principal.DATA_CADASTRO_ATENDIMENTO, a.ID_ATENDIMENTO_MODULO
        ) as rank_atendimento
    from por_atendimento as a
)

select
    principal.modulo,
    principal.CAS,
    principal.TIPO_UNIDADE,
    principal.UNIDADE_ATENDIMENTO,
    principal.EMAIL_UNIDADE,
    principal.EMAIL_CAS,
    principal.SEQ_ATENDIMENTO,
    principal.DATA_DE_ATENDIMENTO,
    principal.HORA_DE_ATENDIMENTO_ORIGINAL,
    principal.HORA_DE_ATENDIMENTO,
    principal.ID_USUARIO,
    principal.ID_FAMILIA,
    principal.NOME_USUARIO,
    principal.DOCUMENTACAO,
    principal.NUMERO_DOCUMENTO,
    principal.ENDERECO,
    principal.ENDERECO_NUMERO,
    principal.ENDERECO_COMPLEMENTO,
    principal.BAIRRO,
    principal.REFERENCIA_OU_COMUNIDADE,
    principal.NOME_ATENDIMENTO_ORIGINAL,
    principal.DATA_NASCIMENTO,
    principal.IDADE,
    principal.RACA_COR,
    principal.SEXO,
    principal.PROFISSIONAL_ID,
    principal.PROFISSIONAL,
    principal.PROFISSIONAL_CBO_ORIGINAL,
    principal.PROFISSIONAL_CBO,
    principal.CADASTRANTE,
    principal.DATA_CADASTRO_ATENDIMENTO,
    a.tipo_atendimento_consolidado as TIPO_ATENDIMENTO,
    principal.NOME_ATENDIMENTO,
    principal.IDADE_FAIXA,
    a.ID_ATENDIMENTO_MODULO as ID_ATENDIMENTO,
    principal.EMAIL,
    1 as cbo_unico_rank,
    a.rank_atendimento as atendimento_unico_rank,
    principal.PROJETOS_SOCIAIS,
    {{ dbt_utils.generate_surrogate_key(['a.ID_ATENDIMENTO_MODULO']) }} as ID_ATENDIMENTO_SK,
    principal.ID_ATENDIMENTO_MODULO,
    principal.ID_UNIDADE,
    principal.ID_LOGIN_CADASTRO,
    principal.ID_TIPO_ATENDIMENTO,
    principal.FLAG_CANCELADO,
    principal.DATA_SAIDA_ATENDIMENTO,
    principal.DATA_SAIDA_VINCULO_FAMILIAR,
    principal.VINCULOS_FAMILIARES,
    principal.PROFISSIONAL_CBOS,
    principal.PROFISSIONAL_NOME_CADASTRAL,
    principal.EMAIL_PLANILHA,
    principal.FLAG_USUARIO_CADASTRADO,
    principal.FLAG_UNIDADE_CADASTRADA,
    principal.FLAG_OPERADOR_CADASTRADO,
    principal.FLAG_PROFISSIONAL_CADASTRADO,
    principal.FLAG_USUARIO_TESTE,
    principal.FLAG_UNIDADE_TESTE,
    principal.FLAG_OPERADOR_TESTE,
    principal.FLAG_PROFISSIONAL_TESTE,
    principal.PROJETOS_SOCIAIS_HISTORICO,
    a.profissionais as PROFISSIONAIS,
    a.quantidade_profissionais as QUANTIDADE_PROFISSIONAIS,
    a.flag_algum_profissional_teste as FLAG_ALGUM_PROFISSIONAL_TESTE
from numerados as a
