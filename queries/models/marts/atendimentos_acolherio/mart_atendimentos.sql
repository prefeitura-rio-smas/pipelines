{{ config(alias='atendimentos', tags=['acolherio_atendimentos_refresh']) }}

-- noqa: disable=CP02,AL09,ST06
-- Uma linha por atendimento do módulo; os filtros são definidos no Looker.
with atendimentos as (
    select
        *,
        safe_cast(hora_atendimento as int64) as hora_original
    from {{ ref('int_atendimentos_completos') }}
    where flag_profissional_principal
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

cbos as (
    select
        ocupacao.id_profissional,
        ocupacao.codigo_cbo,
        cbo.descricao as cbo_original,
        case
            when cbo.descricao like 'Administrador%' then 'Administrador'
            when cbo.descricao like 'Advogado%' then 'Advogado'
            when cbo.descricao like 'Articulador Comunitário%' then 'Articulador Comunitário'
            when cbo.descricao like 'Assistente administrativo%' then 'Assistente administrativo'
            when cbo.descricao like 'Assistente Social%' or cbo.descricao like 'Assistente social%' then 'Assistente social'
            when cbo.descricao like 'Educador social%' then 'Educador social'
            when cbo.descricao like 'Entrevistador Social%' then 'Entrevistador social'
            when cbo.descricao like 'Orientador social%' then 'Orientador social'
            when cbo.descricao like 'Pedagogo%' then 'Pedagogo'
            when cbo.descricao like 'Psicólogo%' then 'Psicólogo'
            when cbo.descricao like 'Recepcionista%' then 'Recepcionista'
            else cbo.descricao
        end as cbo
    from {{ ref('raw_profissionais_ocupacoes') }} as ocupacao
    left join {{ ref('raw_cbo') }} as cbo on ocupacao.codigo_cbo = cbo.codigo_cbo
),

profissionais_cbo as (
    select
        id_profissional,
        array_agg(cbo_original ignore nulls order by codigo_cbo)[safe_offset(0)] as cbo_original,
        array_agg(cbo ignore nulls order by codigo_cbo)[safe_offset(0)] as cbo,
        array_agg(struct(codigo_cbo, cbo_original as descricao) order by codigo_cbo) as cbos,
        coalesce(logical_or(cbo in ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo')), false) as flag_cbo_tecnico,
        coalesce(logical_or(cbo in (
            'Administrador', 'Articulador Comunitário', 'Assistente administrativo',
            'Educador social', 'Orientador social', 'Recepcionista'
        )), false) as flag_cbo_recepcao
    from cbos
    group by id_profissional
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

projetos as (
    select
        vinculo.*,
        coalesce(
            catalogo.nome_projeto_social,
            format('Projeto não mapeado (ID %d)', vinculo.id_projeto_social)
        ) as nome_projeto_social,
        vinculo.indicador_ativo = 'S' and vinculo.data_cancelamento is null as flag_ativo
    from {{ ref('raw_familias_projetos_sociais') }} as vinculo
    left join catalogo_projetos_sociais as catalogo on vinculo.id_projeto_social = catalogo.id_projeto_social
),

projetos_familia as (
    select
        id_familia,
        string_agg(
            distinct if(flag_ativo, nome_projeto_social, null),
            ', ' order by if(flag_ativo, nome_projeto_social, null)
        ) as projetos_sociais,
        array_agg(struct(
            id_projeto_social, nome_projeto_social, id_unidade, id_login_cadastro,
            data_cadastro, data_cancelamento, indicador_ativo
        ) order by id_projeto_social, data_cadastro) as projetos_sociais_historico
    from projetos
    group by id_familia
),

base as (
    select
        a.* except (id_familia),
        coalesce(a.id_familia, familia.id_familia) as id_familia,
        familia.data_saida as data_saida_vinculo_familiar,
        vinculos.vinculos_familiares,
        unidade.cas,
        case
            when unidade.nome_unidade like 'CENTRO POP%' then 'CENTRO POP'
            else {{ map_tipo_unidade('unidade.nome_unidade') }}
        end as tipo_unidade,
        unidade.nome_unidade as unidade_atendimento,
        unidade.email_unidade,
        {{ email_cas('unidade.cas') }} as email_cas,
        filtro.email as email_planilha,
        usuario.nome as nome_usuario,
        usuario.cpf,
        usuario.logradouro,
        usuario.numero_endereco,
        usuario.complemento_endereco,
        usuario.bairro,
        usuario.ponto_referencia,
        usuario.data_nascimento,
        {{ calc_idade('usuario.data_nascimento') }} as idade,
        usuario.raca_cor,
        usuario.sexo,
        tipo.descricao as nome_atendimento_original,
        trim(upper(operador.nome_operador)) as cadastrante,
        format('%02d:%02d', div(a.hora_original, 100), mod(a.hora_original, 100)) as hora,
        usuario.id_paciente is not null as flag_usuario_cadastrado,
        unidade.id_unidade is not null as flag_unidade_cadastrada,
        operador.id_login is not null as flag_operador_cadastrado,
        coalesce(upper(usuario.nome) like '%TESTE%', false) as flag_usuario_teste,
        coalesce(upper(unidade.nome_unidade) like '%TESTE%', false) as flag_unidade_teste,
        coalesce(upper(operador.nome_operador) like '%TESTE%', false) as flag_operador_teste
    from atendimentos as a
    left join {{ ref('raw_usuarios') }} as usuario on a.id_usuario = usuario.id_paciente
    left join {{ ref('raw_unidades') }} as unidade on a.id_unidade = unidade.id_unidade
    left join {{ ref('raw_operadores') }} as operador on a.id_login_cadastro = operador.id_login
    left join {{ ref('raw_tipos_atendimento') }} as tipo on a.id_tipo_atendimento = tipo.id_tipo_atendimento
    left join familia_usuario as familia on a.origem_modulo = 'usuario' and a.id_usuario = familia.id_paciente
    left join vinculos_familia_usuario as vinculos on a.id_usuario = vinculos.id_paciente
    left join filtro_emails as filtro on unidade.nome_unidade = filtro.unidade_atendimento
),

profissionais as (
    select
        a.id_atendimento_modulo,
        a.id_profissional,
        a.flag_profissional_principal,
        b.tipo_unidade,
        b.nome_atendimento_original,
        case
            when trim(upper(p.nome)) in (
                'ATENDIMENTO RECEPÇÃO', 'ATENDIMENTO BUSCA ATIVA', 'ATENDIMENTO ENTREVISTADOR SOCIAL'
            ) then b.cadastrante
            else trim(upper(p.nome))
        end as nome,
        p.nome as nome_cadastral,
        cbo.cbo_original,
        cbo.cbo,
        cbo.cbos,
        coalesce(cbo.flag_cbo_tecnico, false) as flag_cbo_tecnico,
        coalesce(cbo.flag_cbo_recepcao, false) as flag_cbo_recepcao,
        p.id_profissional is not null as flag_cadastrado,
        coalesce(upper(p.nome) like '%TESTE%', false) as flag_teste
    from {{ ref('int_atendimentos_completos') }} as a
    inner join base as b on a.id_atendimento_modulo = b.id_atendimento_modulo
    left join {{ ref('raw_profissionais') }} as p on a.id_profissional = p.id_profissional
    left join profissionais_cbo as cbo on a.id_profissional = cbo.id_profissional
),

tipos_profissionais as (
    select
        p.*,
        case
            when p.nome_atendimento_original like '%Recepção%' or p.nome = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
            when p.flag_cbo_tecnico then 'Atendimento Técnico'
            when p.flag_cbo_recepcao and p.nome_atendimento_original like '%CadÚnico%' then 'Atendimento Recepção'
            else 'Outros Atendimentos'
        end as tipo_atendimento
    from profissionais as p
),

profissionais_classificados as (
    select
        p.id_atendimento_modulo,
        struct(
            p.id_profissional,
            p.nome,
            p.nome_cadastral,
            p.flag_profissional_principal as flag_principal,
            p.cbos,
            p.cbo_original,
            p.cbo,
            p.tipo_atendimento,
            case
                when
                    p.tipo_atendimento = 'Atendimento Recepção' and p.flag_cbo_recepcao
                    and p.tipo_unidade in ('CRAS', 'CREAS') and p.nome_atendimento_original like '%CadÚnico%'
                    then concat(p.tipo_unidade, ' - Recepção - Ação CadÚnico')
                else p.nome_atendimento_original
            end as nome_atendimento,
            p.flag_cadastrado,
            p.flag_teste
        ) as profissional
    from tipos_profissionais as p
),

profissionais_por_atendimento as (
    select
        id_atendimento_modulo,
        array_agg(profissional order by profissional.flag_principal desc, profissional.id_profissional asc) as profissionais,
        array_agg(profissional order by profissional.flag_principal desc, profissional.id_profissional asc limit 1)[offset(0)] as principal,
        string_agg(distinct nullif(profissional.nome, ''), ', ' order by nullif(profissional.nome, '')) as nomes_profissionais,
        countif(profissional.id_profissional is not null) as quantidade_profissionais,
        countif(not profissional.flag_principal) > 0 as flag_atendimento_compartilhado,
        logical_or(profissional.flag_teste) as flag_algum_profissional_teste,
        case
            when countif(profissional.tipo_atendimento = 'Atendimento Técnico') > 0 then 'Atendimento Técnico'
            when countif(profissional.tipo_atendimento = 'Atendimento Recepção') > 0 then 'Atendimento Recepção'
            else 'Outros Atendimentos'
        end as tipo_atendimento
    from profissionais_classificados
    group by id_atendimento_modulo
),

numerados as (
    select
        b.*,
        row_number() over (
            partition by b.id_usuario, b.id_unidade, b.data_atendimento, b.hora, b.nome_atendimento_original
            order by b.data_cadastro_atendimento, b.id_atendimento_modulo
        ) as rank_atendimento
    from base as b
)

select
    case b.origem_modulo when 'usuario' then 'u' else 'f' end as modulo,
    b.cas as CAS,
    b.tipo_unidade as TIPO_UNIDADE,
    b.unidade_atendimento as UNIDADE_ATENDIMENTO,
    b.email_unidade as EMAIL_UNIDADE,
    b.email_cas as EMAIL_CAS,
    b.id_atendimento as SEQ_ATENDIMENTO,
    b.data_atendimento as DATA_DE_ATENDIMENTO,
    b.hora_original as HORA_DE_ATENDIMENTO_ORIGINAL,
    b.hora as HORA_DE_ATENDIMENTO,
    b.id_usuario as ID_USUARIO,
    b.id_familia as ID_FAMILIA,
    b.nome_usuario as NOME_USUARIO,
    case when b.cpf is null or b.cpf = '' then 'CPF não informado' else 'CPF informado' end as DOCUMENTACAO,
    case
        when b.cpf is null or b.cpf = '' then null
        else format(
            '%s.%s.%s-%s',
            substr(lpad(b.cpf, 11, '0'), 1, 3), substr(lpad(b.cpf, 11, '0'), 4, 3),
            substr(lpad(b.cpf, 11, '0'), 7, 3), substr(lpad(b.cpf, 11, '0'), 10, 2)
        )
    end as NUMERO_DOCUMENTO,
    trim(regexp_replace(b.logradouro, r'[,\s;]+', ' ')) as ENDERECO,
    trim(regexp_replace(b.numero_endereco, r'[,\s;]+', ' ')) as ENDERECO_NUMERO,
    trim(regexp_replace(b.complemento_endereco, r'[,\s;]+', ' ')) as ENDERECO_COMPLEMENTO,
    case when b.bairro = '' then 'NAO INFORMADO' else b.bairro end as BAIRRO,
    trim(regexp_replace(b.ponto_referencia, r'[,\s;]+', ' ')) as REFERENCIA_OU_COMUNIDADE,
    b.nome_atendimento_original as NOME_ATENDIMENTO_ORIGINAL,
    b.data_nascimento as DATA_NASCIMENTO,
    cast(b.idade as int64) as IDADE,
    case b.raca_cor
        when '01' then 'Branca'
        when '02' then 'Preta'
        when '03' then 'Parda'
        when '04' then 'Amarela'
        when '05' then 'Indigena'
        else 'Nao informado'
    end as RACA_COR,
    b.sexo as SEXO,
    p.principal.id_profissional as PROFISSIONAL_ID,
    p.principal.nome as PROFISSIONAL,
    p.principal.cbo_original as PROFISSIONAL_CBO_ORIGINAL,
    p.principal.cbo as PROFISSIONAL_CBO,
    b.cadastrante as CADASTRANTE,
    b.data_cadastro_atendimento as DATA_CADASTRO_ATENDIMENTO,
    p.tipo_atendimento as TIPO_ATENDIMENTO,
    p.principal.nome_atendimento as NOME_ATENDIMENTO,
    case
        when b.idade < 18 then 'Até 17 anos'
        when b.idade < 30 then 'De 18 a 29 anos'
        when b.idade < 45 then 'De 30 a 44 anos'
        when b.idade < 60 then 'De 45 a 59 anos'
        when b.idade < 75 then 'De 60 a 74 anos'
        when b.idade >= 75 then 'Mais de 75 anos'
        else 'Não Informado'
    end as IDADE_FAIXA,
    b.id_atendimento_modulo as ID_ATENDIMENTO,
    concat(coalesce(b.email_cas, ''), ',', coalesce(b.email_unidade, ''), ',', coalesce(b.email_planilha, '')) as EMAIL,
    1 as cbo_unico_rank,
    b.rank_atendimento as atendimento_unico_rank,
    projetos.projetos_sociais as PROJETOS_SOCIAIS,
    {{ dbt_utils.generate_surrogate_key(['b.id_atendimento_modulo']) }} as ID_ATENDIMENTO_SK,
    b.id_atendimento_modulo as ID_ATENDIMENTO_MODULO,
    b.id_unidade as ID_UNIDADE,
    b.id_login_cadastro as ID_LOGIN_CADASTRO,
    b.id_tipo_atendimento as ID_TIPO_ATENDIMENTO,
    b.flag_cancelado as FLAG_CANCELADO,
    b.data_saida as DATA_SAIDA_ATENDIMENTO,
    b.data_saida_vinculo_familiar as DATA_SAIDA_VINCULO_FAMILIAR,
    b.vinculos_familiares as VINCULOS_FAMILIARES,
    p.principal.cbos as PROFISSIONAL_CBOS,
    p.principal.nome_cadastral as PROFISSIONAL_NOME_CADASTRAL,
    b.email_planilha as EMAIL_PLANILHA,
    b.flag_usuario_cadastrado as FLAG_USUARIO_CADASTRADO,
    b.flag_unidade_cadastrada as FLAG_UNIDADE_CADASTRADA,
    b.flag_operador_cadastrado as FLAG_OPERADOR_CADASTRADO,
    p.principal.flag_cadastrado as FLAG_PROFISSIONAL_CADASTRADO,
    b.flag_usuario_teste as FLAG_USUARIO_TESTE,
    b.flag_unidade_teste as FLAG_UNIDADE_TESTE,
    b.flag_operador_teste as FLAG_OPERADOR_TESTE,
    p.principal.flag_teste as FLAG_PROFISSIONAL_TESTE,
    projetos.projetos_sociais_historico as PROJETOS_SOCIAIS_HISTORICO,
    p.profissionais as PROFISSIONAIS,
    p.quantidade_profissionais as QUANTIDADE_PROFISSIONAIS,
    p.flag_algum_profissional_teste as FLAG_ALGUM_PROFISSIONAL_TESTE,
    p.nomes_profissionais as PROFISSIONAIS_NOMES,
    p.flag_atendimento_compartilhado as FLAG_ATENDIMENTO_COMPARTILHADO,
    b.flag_usuario_teste or b.flag_unidade_teste or b.flag_operador_teste
    or p.flag_algum_profissional_teste as FLAG_REGISTRO_TESTE,
    servicos.servicos_socioassistenciais as SERVICOS_SOCIOASSISTENCIAIS,
    servicos.servicos_socioassistenciais_historico as SERVICOS_SOCIOASSISTENCIAIS_HISTORICO,
    coalesce(perfil.flag_possui_violacao_direito, 'Não Informado') as FLAG_POSSUI_VIOLACAO_DIREITO,
    perfil.violacoes as VIOLACOES,
    perfil.violacoes_detalhes as VIOLACOES_DETALHES,
    coalesce(perfil.flag_recebe_beneficio, 'Não Informado') as FLAG_RECEBE_BENEFICIO,
    perfil.beneficios as BENEFICIOS,
    perfil.beneficios_detalhes as BENEFICIOS_DETALHES,
    coalesce(perfil.flag_deficiencia, 'Não Informado') as FLAG_DEFICIENCIA,
    perfil.deficiencias as DEFICIENCIAS,
    perfil.deficiencias_detalhes as DEFICIENCIAS_DETALHES,
    coalesce(perfil.flag_situacao_rua, 'Não Informado') as FLAG_SITUACAO_RUA,
    coalesce(perfil.flag_possui_comorbidade, 'Não Informado') as FLAG_POSSUI_COMORBIDADE,
    perfil.comorbidades as COMORBIDADES,
    perfil.data_informacao_comorbidades as DATA_INFORMACAO_COMORBIDADES,
    perfil.local_tratamento_comorbidades as LOCAL_TRATAMENTO_COMORBIDADES,
    perfil.comorbidades_detalhes as COMORBIDADES_DETALHES
from numerados as b
left join profissionais_por_atendimento as p on b.id_atendimento_modulo = p.id_atendimento_modulo
left join projetos_familia as projetos on b.id_familia = projetos.id_familia
left join {{ ref('int_servicos_familia_completos') }} as servicos on b.id_familia = servicos.id_familia
left join {{ ref('int_perfil_social_usuarios') }} as perfil on b.id_usuario = perfil.id_usuario
