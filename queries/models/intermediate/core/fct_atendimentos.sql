-- Um evento por id_atendimento_modulo; participantes em PROFISSIONAIS.
-- Flags e ranks do dataviz são atributos, não filtros da base compartilhada.

-- Cadastro atual por atendimento, com equipe e perfil social agregados.
-- A mart preserva o grão; flags e ranks permitem filtrar no dataviz.

-- noqa: disable=CP02,AL09,ST06
with atendimentos as (
    select
        *,
        safe_cast(hora_atendimento as int64) as hora_original
    from {{ ref('int_atendimentos') }}
    where flag_profissional_principal
),

familias as (
    select
        id_paciente,
        array_agg(struct(id_familia, data_saida) order by data_saida is null desc, data_saida desc, data_entrada desc, id_familia desc limit 1)[offset(0)] as principal,
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
            when cbo.descricao like 'Articulador Comunitário%' then 'Articulador Comunitário'
            when cbo.descricao like 'Assistente administrativo%' then 'Assistente administrativo'
            when cbo.descricao like 'Assistente Social%' or cbo.descricao like 'Assistente social%' then 'Assistente social'
            when cbo.descricao like 'Educador social%' then 'Educador social'
            when cbo.descricao like 'Entrevistador Social%' then 'Entrevistador social'
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
        array_agg(cbo_original ignore nulls order by codigo_cbo, cbo_original)[safe_offset(0)] as cbo_original,
        array_agg(cbo ignore nulls order by codigo_cbo, cbo_original)[safe_offset(0)] as cbo,
        array_agg(struct(codigo_cbo, cbo_original as descricao) order by codigo_cbo, cbo_original) as cbos
    from cbos
    group by id_profissional
),

base as (
    select
        a.* except (id_familia),
        a.id_familia as id_familia_origem,
        coalesce(a.id_familia, if(a.origem_modulo = 'usuario', familia.principal.id_familia, null)) as id_familia,
        if(a.origem_modulo = 'usuario', familia.principal.data_saida, null) as data_saida_vinculo_familiar,
        familia.vinculos_familiares,
        unidade.cas,
        case
            when unidade.nome_unidade like 'CENTRO POP%' then 'CENTRO POP'
            when unidade.nome_unidade like 'ESCRITÓRIO%' then 'ESCRITÓRIO SOCIAL'
            when unidade.nome_unidade like 'LARES%' then 'LARES CARIOCAS'
            else {{ map_tipo_unidade('unidade.nome_unidade') }}
        end as tipo_unidade,
        unidade.nome_unidade as unidade_atendimento,
        unidade.email_unidade,
        -- A tabela programada também atende unidades da gerência (CAS GE).
        case
            when unidade.cas = 'GE' then 'gdados.smas@prefeitura.rio'
            else {{ email_cas('unidade.cas') }}
        end as email_cas,
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
        coalesce(operador.nome_operador like '%TESTE%', false) as flag_operador_teste_legado,
        coalesce(upper(operador.nome_operador) like '%TESTE%', false) as flag_operador_teste,
        -- Preserva os filtros da V2; NULL também fica fora do universo analítico.
        coalesce(
            usuario.nome not like '%TESTE%'
            and unidade.nome_unidade not like '%TESTE%'
            and operador.nome_operador not like '%TESTE%'
            and (a.origem_modulo != 'usuario' or familia.principal.data_saida is null),
            false
        ) as flag_elegivel_dataviz
    from atendimentos as a
    left join {{ ref('raw_usuarios') }} as usuario on a.id_usuario = usuario.id_paciente
    left join {{ ref('raw_unidades') }} as unidade on a.id_unidade = unidade.id_unidade
    left join {{ ref('raw_operadores') }} as operador on a.id_login_cadastro = operador.id_login
    left join {{ ref('raw_tipos_atendimento') }} as tipo on a.id_tipo_atendimento = tipo.id_tipo_atendimento
    left join familias as familia on a.id_usuario = familia.id_paciente
    left join filtro_emails as filtro on unidade.nome_unidade = filtro.unidade_atendimento
),

profissionais as (
    select
        a.id_atendimento_modulo,
        a.id_profissional,
        a.flag_profissional_principal,
        b.tipo_unidade,
        b.nome_atendimento_original,
        b.id_usuario,
        b.hora,
        b.data_atendimento,
        b.unidade_atendimento,
        b.flag_elegivel_dataviz,
        b.data_cadastro_atendimento,
        b.id_atendimento,
        b.origem_modulo,
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
        -- Classificação pelo mesmo CBO exibido no campo escalar.
        coalesce(cbo.cbo in ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo'), false) as flag_cbo_tecnico,
        coalesce(cbo.cbo in (
            'Administrador', 'Articulador Comunitário', 'Assistente administrativo',
            'Educador social', 'Orientador social', 'Recepcionista'
        ), false) as flag_cbo_recepcao,
        p.id_profissional is not null as flag_cadastrado,
        coalesce(upper(p.nome) like '%TESTE%', false) as flag_teste
    from {{ ref('int_atendimentos') }} as a
    inner join base as b on a.id_atendimento_modulo = b.id_atendimento_modulo
    left join {{ ref('raw_profissionais') }} as p on a.id_profissional = p.id_profissional
    left join profissionais_cbo as cbo on a.id_profissional = cbo.id_profissional
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
            {{ classificar_atendimento('p') }} as tipo_atendimento,
            case
                when
                    p.flag_cbo_recepcao
                    and p.tipo_unidade in ('CRAS', 'CREAS') and p.nome_atendimento_original like '%CadÚnico%'
                    then concat(p.tipo_unidade, ' - Recepção - Ação CadÚnico')
                else p.nome_atendimento_original
            end as nome_atendimento,
            p.flag_cadastrado,
            p.flag_teste,
            -- Mantém os registros, mas prioriza o universo V2 para o filtro opcional.
            row_number() over (
                partition by
                    if(p.flag_cadastrado, p.id_profissional, null), p.hora, p.data_atendimento,
                    p.nome_atendimento_original, p.id_usuario, p.unidade_atendimento
                order by
                    p.flag_elegivel_dataviz desc, p.data_cadastro_atendimento asc,
                    p.id_atendimento asc, p.origem_modulo asc
            ) as atendimento_unico_rank
        ) as profissional
    from profissionais as p
),

profissionais_por_atendimento as (
    select
        id_atendimento_modulo,
        array_agg(profissional order by profissional.flag_principal desc, profissional.id_profissional asc) as profissionais,
        any_value(if(profissional.flag_principal, profissional, null)) as principal,
        string_agg(distinct nullif(profissional.nome, ''), ', ' order by nullif(profissional.nome, '')) as nomes_profissionais,
        countif(profissional.id_profissional is not null) as quantidade_profissionais,
        countif(not profissional.flag_principal) > 0 as flag_atendimento_compartilhado,
        logical_or(profissional.flag_teste) as flag_algum_profissional_teste
    from profissionais_classificados
    group by id_atendimento_modulo
)

select
    case b.origem_modulo when 'usuario' then 'u' else 'f' end as modulo,
    b.origem_modulo,
    b.flag_operador_teste_legado,
    b.data_atendimento,
    b.hora_atendimento,
    b.nome_atendimento_original as tipo_atendimento_descricao,
    p.principal.id_profissional as id_profissional,
    if(
        b.flag_usuario_cadastrado and b.nome_usuario not like '%TESTE%',
        {{ dbt_utils.generate_surrogate_key(['b.id_usuario']) }}, null
    ) as id_usuario_sk,
    if(
        b.flag_unidade_cadastrada and b.unidade_atendimento not like '%TESTE%',
        {{ dbt_utils.generate_surrogate_key(['b.id_unidade']) }}, null
    ) as id_unidade_sk,
    if(
        p.principal.flag_cadastrado and upper(p.principal.nome_cadastral) not like '%TESTE%',
        {{ dbt_utils.generate_surrogate_key(['p.principal.id_profissional']) }}, null
    ) as id_profissional_sk,
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
    b.id_familia_origem as ID_FAMILIA,
    b.id_familia as ID_FAMILIA_CONTEXTO,
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
    p.principal.tipo_atendimento as TIPO_ATENDIMENTO,
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
    b.id_atendimento as ID_ATENDIMENTO,
    concat(b.email_cas, ',', b.email_unidade, ',', b.email_planilha) as EMAIL,
    projetos.projetos_sociais_descricao as PROJETOS_SOCIAIS,
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
    b.flag_elegivel_dataviz as FLAG_ELEGIVEL_DATAVIZ,
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
    coalesce(perfil.perfil_social.flag_possui_violacao_direito, 'Não Informado') as FLAG_POSSUI_VIOLACAO_DIREITO,
    perfil.perfil_social.violacoes as VIOLACOES,
    perfil.perfil_social.violacoes_detalhes as VIOLACOES_DETALHES,
    coalesce(perfil.perfil_social.flag_recebe_beneficio, 'Não Informado') as FLAG_RECEBE_BENEFICIO,
    perfil.perfil_social.beneficios as BENEFICIOS,
    perfil.perfil_social.beneficios_detalhes as BENEFICIOS_DETALHES,
    coalesce(perfil.perfil_social.flag_deficiencia, 'Não Informado') as FLAG_DEFICIENCIA,
    perfil.perfil_social.deficiencias as DEFICIENCIAS,
    perfil.perfil_social.deficiencias_detalhes as DEFICIENCIAS_DETALHES,
    coalesce(perfil.perfil_social.flag_situacao_rua, 'Não Informado') as FLAG_SITUACAO_RUA,
    coalesce(perfil.perfil_social.flag_possui_comorbidade, 'Não Informado') as FLAG_POSSUI_COMORBIDADE,
    perfil.perfil_social.comorbidades as COMORBIDADES,
    perfil.perfil_social.data_informacao_comorbidades as DATA_INFORMACAO_COMORBIDADES,
    perfil.perfil_social.local_tratamento_comorbidades as LOCAL_TRATAMENTO_COMORBIDADES,
    perfil.perfil_social.comorbidades_detalhes as COMORBIDADES_DETALHES
from base as b
left join profissionais_por_atendimento as p on b.id_atendimento_modulo = p.id_atendimento_modulo
left join {{ ref('int_familias_projetos_sociais') }} as projetos on b.id_familia = projetos.id_familia
left join {{ ref('int_servicos_agregados') }} as servicos on b.id_familia = servicos.id_familia
left join {{ ref('int_usuarios') }} as perfil on b.id_usuario = perfil.id_usuario
