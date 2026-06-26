{{ config(
    materialized = 'table',
    schema = 'alta_complexidade'
) }}

-- ============================================================
-- mart_acolhimento.sql
-- Mart principal de acolhimentos institucionais (alta complexidade).
-- Grão: 1 linha por ciclo de acolhimento.
-- Substitui a query agendada dashboard_acolherio.alta_complexidade.
-- ============================================================

WITH
-- Agrega deficiências por usuário (explodidas → STRING_AGG)
deficiencias AS (
    SELECT
        id_usuario_sk,
        STRING_AGG(deficiencia_label, ', ' ORDER BY deficiencia_label) AS tipo_deficiencia
    FROM {{ ref('int_deficiencias_agregadas') }}
    GROUP BY id_usuario_sk
),

-- Agrega benefícios por usuário (explodidos → STRING_AGG)
beneficios AS (
    SELECT
        id_usuario_sk,
        STRING_AGG(beneficio_label, ', ' ORDER BY beneficio_label) AS tipo_beneficio
    FROM {{ ref('int_beneficios_agregados') }}
    GROUP BY id_usuario_sk
),

-- Contagem de acolhidos ativos por unidade
vagas_ocupadas_por_unidade AS (
    SELECT
        id_unidade_sk,
        COUNT(*) AS vagas_ocupadas
    FROM {{ ref('fct_acolhimentos') }}
    WHERE flag_em_acolhimento = 1
    GROUP BY id_unidade_sk
),

-- Fato de acolhimentos
acolhimentos AS (
    SELECT * FROM {{ ref('fct_acolhimentos') }}
),

-- Dimensão de usuários
usuarios AS (
    SELECT * FROM {{ ref('dim_usuarios') }}
),

-- Dimensão de unidades
unidades AS (
    SELECT * FROM {{ ref('dim_unidades') }}
),

-- Planilha de e-mails por unidade
emails AS (
    SELECT * FROM {{ ref('raw_sheets_filtro_email_prontuario') }}
),

-- Dados cadastrais adicionais
raw_usuarios_extra AS (
    SELECT id_paciente, data_cadastro, id_login_cadastro
    FROM {{ ref('raw_usuarios') }}
),

-- Detalhes adicionais (renda_beneficio numeric)
raw_detalhes AS (
    SELECT id_paciente, renda_beneficio, renda_ativa
    FROM {{ ref('raw_usuarios_detalhes') }}
),

-- Operadores
raw_oper AS (
    SELECT id_login, nome_operador
    FROM {{ ref('raw_operadores') }}
),

-- Junção principal
base AS (
    SELECT
        -- Chaves
        a.id_usuario_sk,
        a.id_unidade_sk,
        a.id_acolhimento_sk,

        -- IDs originais
        g.id_usuario                   AS seqpac,
        a.id_ciclo                     AS seqciclo,

        -- Dados do usuário
        g.nome                         AS nome_usuario,
        g.nome_social,
        g.cpf,
        g.filiacao_mae,
        g.data_nascimento,
        g.sexo,
        g.raca_cor,
        g.genero,
        g.orientacao_sexual,
        g.bairro,
        g.nacionalidade,
        g.condicao_estrangeira,
        g.pais_origem                  AS pais_origem_descricao,
        g.flag_cadunico,
        g.flag_possui_violacao_direito AS flag_violacao_direito,
        g.flag_saude_mental_comprometida AS diagnostico_saude_mental,
        g.flag_deficiencia             AS deficiencia,
        g.flag_situacao_rua            AS situacao_de_rua,
        g.motivo_acolhimento,
        g.origem_demanda,
        g.vinculo_trabalhista,
        g.profissao,
        g.flag_recebe_beneficio,
        g.flag_curatela,
        g.tipo_curatela                AS curatela,
        g.flag_trabalha                AS atvd_remunerada,
        g.escolaridade_indice,
        g.flag_frequenta_escola,

        -- Dados de acolhimento
        a.data_entrada,
        a.data_saida                   AS data_desligamento,
        a.motivo_saida,
        a.flag_em_acolhimento,

        -- Dados da unidade
        u.nome_unidade                 AS unidade,
        u.nome_tipo,
        u.esfera,
        u.flag_unidade_ativa           AS unidade_ativa,
        u.total_vagas                  AS vagas_totais,
        u.vagas_bloqueadas,
        u.cas,
        u.email_unidade,
        u.flag_eixo_adulto,
        u.flag_eixo_familia,
        u.flag_eixo_idoso,

        -- Dados suplementares
        rue.data_cadastro              AS data_cadastro_usuario,
        rd.renda_beneficio             AS renda_beneficio_numeric,

        -- Operador
        op.nome_operador               AS operador,

        -- Vagas ocupadas
        COALESCE(vou.vagas_ocupadas, 0) AS vagas_ocupadas_calc,

        -- Email da planilha
        em.email                       AS email_planilha

    FROM acolhimentos a
    LEFT JOIN usuarios g ON a.id_usuario_sk = g.id_usuario_sk
    LEFT JOIN unidades u ON a.id_unidade_sk = u.id_unidade_sk
    LEFT JOIN raw_usuarios_extra rue ON g.id_usuario = rue.id_paciente
    LEFT JOIN raw_detalhes rd ON g.id_usuario = rd.id_paciente
    LEFT JOIN raw_oper op ON rue.id_login_cadastro = op.id_login
    LEFT JOIN vagas_ocupadas_por_unidade vou ON a.id_unidade_sk = vou.id_unidade_sk
    LEFT JOIN emails em ON TRIM(u.nome_unidade) = em.unidade_atendimento
),

-- ROW_NUMBER para sequência por unidade
-- Também calcula email_cas aqui para evitar referência cruzada no SELECT final
com_row_number AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_unidade_sk
            ORDER BY data_entrada ASC, seqciclo ASC
        ) AS rn_unidade,
        DATE_DIFF(
            COALESCE(data_desligamento, CURRENT_DATE()),
            data_entrada,
            DAY
        ) AS tempo_permanencia_calc,
        CASE
            WHEN REGEXP_CONTAINS(COALESCE(cas, ''), r'(\d+)')
                THEN CASE LOWER(REGEXP_EXTRACT(cas, r'(\d+)'))
                    WHEN '10' THEN 'cas10@prefeitura.rio'
                    WHEN '09' THEN 'cas9@prefeitura.rio'
                    WHEN '08' THEN 'cas8@prefeitura.rio'
                    WHEN '07' THEN 'cas7@prefeitura.rio'
                    WHEN '06' THEN 'cas6@prefeitura.rio'
                    WHEN '05' THEN 'cas5@prefeitura.rio'
                    WHEN '04' THEN 'cas4@prefeitura.rio'
                    WHEN '03' THEN 'cas3@prefeitura.rio'
                    WHEN '02' THEN 'cas2@prefeitura.rio'
                    WHEN '01' THEN 'cas1@prefeitura.rio'
                END
        END AS email_cas
    FROM base
),

final AS (
    SELECT
        -- IDs
        seqpac,
        seqciclo,

        -- Nome
        nome_usuario,
        COALESCE(nome_social, '')      AS nome_social,

        -- CPF
        COALESCE(cpf, '')              AS cpf,
        CASE
            WHEN cpf IS NOT NULL AND cpf != ''
                THEN FORMAT('%s.%s.%s-%s',
                    SUBSTR(cpf, 1, 3),  SUBSTR(cpf, 4, 3),
                    SUBSTR(cpf, 7, 3),  SUBSTR(cpf, 10, 2))
            ELSE '000.000.000-00'
        END                            AS cpf_formatado,

        -- Filiação
        COALESCE(filiacao_mae, '')     AS filiacao_mae,

        -- Data nascimento / idade
        data_nascimento,
        DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) AS idade,

        -- Unidade
        unidade,

        -- Tipo de unidade
        CASE
            WHEN LOWER(COALESCE(nome_tipo, '')) LIKE 'albergue%'  THEN 'Albergue'
            WHEN LOWER(COALESCE(nome_tipo, '')) LIKE 'craf%'      THEN 'Central de Recepção'
            WHEN LOWER(COALESCE(nome_tipo, '')) LIKE 'cri%'       THEN 'Central de Recepção'
            WHEN LOWER(COALESCE(nome_tipo, '')) LIKE 'república%' THEN 'República'
            WHEN LOWER(COALESCE(nome_tipo, '')) LIKE 'urs%'       THEN 'URS'
            ELSE 'Unidade Conveniada'
        END                            AS tipo_unidade,

        -- Datas
        data_entrada,
        data_desligamento,
        COALESCE(data_entrada, data_entrada)           AS data_auxiliar,
        COALESCE(data_desligamento, data_desligamento) AS data_desligamento_auxiliar,

        -- Status
        CASE
            WHEN flag_em_acolhimento = 1 THEN 'Acolhido'
            ELSE 'Desligado'
        END                            AS status_acolhimento,

        -- Motivo desligamento
        CASE
            WHEN motivo_saida = '2' THEN 'Decisão da direção/equipe - Conflito com profissional da unid.'
            WHEN motivo_saida = '3' THEN 'Decisão da direção/equipe - Regras da instituição'
            WHEN motivo_saida = '4' THEN 'Voluntário - Conflito com outro acolhido'
            WHEN motivo_saida = '5' THEN 'Voluntário - Conflito com profissional da unid.'
            WHEN motivo_saida = '7' THEN 'Voluntário - Regras da instituição'
            WHEN motivo_saida = '8' THEN 'Voluntário - Abstinência'
            WHEN motivo_saida = 'C' THEN 'Decisão da direção/equipe - Conflito com outro acolhido'
            WHEN motivo_saida = 'D' THEN 'Voluntário - Sem motivo identificado'
            WHEN motivo_saida = 'E' THEN 'Acolhido'
            WHEN motivo_saida = 'F' THEN 'Reinserção comunitária'
            WHEN motivo_saida = 'G' THEN 'Reinserção em família'
            WHEN motivo_saida = 'H' THEN 'Afastamento de cri/adol por medida protetiva'
            WHEN motivo_saida = 'I' THEN 'Solicitação de vaga'
            WHEN motivo_saida = 'J' THEN 'Reinserção PVTN'
            WHEN motivo_saida = 'K' THEN 'Mudança para outro município'
            WHEN motivo_saida = 'L' THEN 'Transferência para clínica de apoio a saúde'
            WHEN motivo_saida = 'N' THEN 'Transferência para delegacia policial'
            WHEN motivo_saida = 'O' THEN 'Óbito'
            WHEN motivo_saida = 'T' THEN 'Transferência para outra unid.'
            WHEN motivo_saida = 'X' THEN 'Fechado pela Unificação'
            WHEN motivo_saida = 'Y' THEN 'Demanda por serviço diurno'
            WHEN motivo_saida = 'Z' THEN 'Demanda por serviço noturno'
            WHEN (motivo_saida IS NULL OR motivo_saida = '') AND flag_em_acolhimento = 1
                THEN 'Não desligado'
            WHEN (motivo_saida IS NULL OR motivo_saida = '') AND flag_em_acolhimento = 0
                THEN 'Acolhido'
            ELSE 'Acolhido'
        END                            AS motivo_desligamento,

        -- Motivo reinserção
        CASE
            WHEN motivo_saida = 'F' THEN 'Deslig. por reinserção comunitária'
            WHEN motivo_saida = 'G' THEN 'Deslig. por reinserção em família de origem ou família extensa'
            WHEN motivo_saida = 'J' THEN 'Reinserção através do projeto de volta a terra natal'
            WHEN (motivo_saida IS NULL OR motivo_saida = '') AND flag_em_acolhimento = 1
                THEN 'Null'
            WHEN flag_em_acolhimento = 0 AND motivo_saida NOT IN ('F', 'G', 'J')
                THEN 'Outros motivos'
            ELSE 'Null'
        END                            AS motivo_reinsercao,

        -- Raça/Cor
        COALESCE(raca_cor, 'Não Informado')             AS raca_cor,

        -- Sexo
        COALESCE(sexo, 'Não Informado')                 AS sexo,

        -- Gênero
        COALESCE(genero, 'Não Informado')               AS genero,

        -- Orientação sexual
        COALESCE(orientacao_sexual, 'Não Informado')    AS orientacao_sexual,

        -- Data cadastro
        data_cadastro_usuario,

        -- Bairro
        COALESCE(bairro, '')                            AS bairro,

        -- Origem demanda
        COALESCE(origem_demanda, 'Não Informado')       AS origem_demanda,

        -- Operador
        operador,

        -- Flag recebe benefício
        COALESCE(flag_recebe_beneficio, 'Não Informado') AS flag_recebe_beneficio,

        -- Atividade remunerada
        CASE
            WHEN COALESCE(atvd_remunerada, '') = 'S' THEN 'Sim'
            WHEN COALESCE(atvd_remunerada, '') = 'N' THEN 'Não'
            ELSE COALESCE(atvd_remunerada, 'Não')
        END                            AS atvd_remunerada,

        -- Flag trabalho
        CASE
            WHEN COALESCE(atvd_remunerada, '') = 'S' THEN 'Sim'
            ELSE 'Não'
        END                            AS flag_trabalho,

        -- Vínculo trabalhista
        COALESCE(vinculo_trabalhista, '')               AS vinculo_trabalhista,

        -- Profissão
        COALESCE(profissao, '')                         AS profissao,

        -- Frequenta escola
        COALESCE(flag_frequenta_escola, 'Não Informado') AS frequenta_escola,

        -- Escolaridade
        COALESCE(escolaridade_indice, 'Não Informado')  AS escolaridade,

        -- Flag CadÚnico
        COALESCE(flag_cadunico, 'Não Informado')        AS flag_cadunico,

        -- Renda benefício
        CASE
            WHEN COALESCE(renda_beneficio_numeric, 0) > 0 THEN 'Sim'
            WHEN renda_beneficio_numeric IS NOT NULL    THEN 'Não'
            ELSE 'Não Informado'
        END                            AS renda_beneficio,

        -- Nacionalidade
        COALESCE(nacionalidade, 'Não Informado')        AS nacionalidade,

        -- Condição estrangeira
        condicao_estrangeira,

        -- País de origem
        COALESCE(pais_origem_descricao, 'Brasil')       AS pais_origem_descricao,

        -- Curatela
        CASE
            WHEN flag_curatela = 'Sim' THEN COALESCE(curatela, 'Não Informado')
            ELSE 'Não Informado'
        END                            AS curatela,

        -- Diagnóstico saúde mental
        COALESCE(diagnostico_saude_mental, 'Não Informado') AS diagnostico_saude_mental,

        -- Saúde mental
        COALESCE(diagnostico_saude_mental, 'Não Informado') AS saude_mental,

        -- Deficiência
        CASE
            WHEN COALESCE(deficiencia, '') = 'S' THEN 'Sim'
            WHEN COALESCE(deficiencia, '') = 'N' THEN 'Não'
            ELSE COALESCE(deficiencia, 'Não')
        END                            AS deficiencia,

        -- Situação de rua
        CASE
            WHEN COALESCE(situacao_de_rua, '') = 'S' THEN 'Sim'
            WHEN COALESCE(situacao_de_rua, '') = 'N' THEN 'Não'
            ELSE COALESCE(situacao_de_rua, 'Não')
        END                            AS situacao_de_rua,

        -- Motivo acolhimento
        COALESCE(motivo_acolhimento, 'Não Informado')   AS motivo_acolhimento,

        -- Flag violação direito
        COALESCE(flag_violacao_direito, 'Não Informado') AS flag_violacao_direito,

        -- Unidade ativa
        CASE
            WHEN unidade_ativa IS TRUE THEN 'Sim'
            WHEN unidade_ativa IS FALSE THEN 'Não'
            ELSE 'Não'
        END                            AS unidade_ativa,

        -- Esfera
        COALESCE(esfera, '')           AS esfera,

        -- Vagas
        vagas_totais,
        COALESCE(vagas_bloqueadas, 0)  AS vagas_bloqueadas,
        vagas_ocupadas_calc            AS vagas_ocupadas,
        GREATEST(
            COALESCE(vagas_totais, 0) - COALESCE(vagas_bloqueadas, 0) - vagas_ocupadas_calc,
            0
        )                              AS vagas_livres,

        -- Taxa ocupação
        CASE
            WHEN COALESCE(vagas_totais, 0) > 0
                THEN ROUND((vagas_ocupadas_calc * 100.0) / vagas_totais, 2)
        END                            AS taxa_ocupacao,

        -- CAS
        cas,

        -- Email CAS (calculado na CTE com_row_number)
        email_cas,

        -- Email planilha
        email_planilha,

        -- Email unidade
        COALESCE(email_unidade, '')    AS email_unidade,

        -- Eixo
        CASE
            WHEN flag_eixo_adulto = 'Sim' AND flag_eixo_familia = 'Sim' AND flag_eixo_idoso = 'Sim'
                THEN 'Eixo adulto, idoso e família'
            WHEN flag_eixo_adulto = 'Sim' AND flag_eixo_familia = 'Sim'
                THEN 'Eixo adulto e família'
            WHEN flag_eixo_adulto = 'Sim' AND flag_eixo_idoso = 'Sim'
                THEN 'Eixo adulto e idoso'
            WHEN flag_eixo_familia = 'Sim' AND flag_eixo_idoso = 'Sim'
                THEN 'Eixo família e idoso'
            WHEN flag_eixo_adulto = 'Sim' THEN 'Eixo adulto'
            WHEN flag_eixo_familia = 'Sim' THEN 'Eixo família'
            WHEN flag_eixo_idoso = 'Sim' THEN 'Eixo idoso'
            WHEN cas IS NULL THEN 'Outra unidades SMAS'
            ELSE 'Outros'
        END                            AS eixo,

        -- Deficiencia agregada
        d.tipo_deficiencia,

        -- Benefício agregado
        b.tipo_beneficio,

        -- Email concatenado
        CASE
            WHEN email_cas IS NOT NULL
                THEN CONCAT(
                    COALESCE(email_cas, ''),
                    CASE WHEN email_planilha IS NOT NULL AND email_planilha != ''
                        THEN ',' || email_planilha ELSE '' END,
                    CASE WHEN COALESCE(email_unidade, '') != ''
                        THEN ',' || email_unidade ELSE '' END
                )
            WHEN COALESCE(email_unidade, '') != ''
                THEN email_unidade
        END                            AS email,

        -- Faixa etária
        CASE
            WHEN data_nascimento IS NULL THEN 'Não informada'
            WHEN DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) < 4
                THEN 'De 0 a 3 anos - Bebê'
            WHEN DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) BETWEEN 4 AND 11
                THEN 'De 04 a 11 anos - Criança'
            WHEN DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) BETWEEN 12 AND 17
                THEN 'De 12 a 17 anos - Adolescente'
            WHEN DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) BETWEEN 18 AND 59
                THEN 'De 18 a 59 anos - Adulto'
            WHEN DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) >= 60
                THEN 'Mais de 60 anos - Idoso'
            ELSE 'Não informada'
        END                            AS faixa_etaria,

        -- Vagas únicas (ROW_NUMBER por unidade)
        rn_unidade                     AS vagas_unicas,

        -- Vagas únicas bloqueadas
        COALESCE(vagas_bloqueadas, 0)  AS vagas_unicas_bloqueadas,

        -- Vagas únicas ocupadas
        vagas_ocupadas_calc            AS vagas_unicas_ocupadas,

        -- Vagas únicas livres
        GREATEST(
            COALESCE(vagas_totais, 0) - COALESCE(vagas_bloqueadas, 0) - vagas_ocupadas_calc,
            0
        )                              AS vagas_unicas_livres,

        -- Tempo permanência
        tempo_permanencia_calc         AS tempo_permanencia,

        -- Status CPF
        CASE
            WHEN cpf IS NOT NULL AND cpf != '' THEN 'CPF informado'
            ELSE 'CPF não informado'
        END                            AS status_cpf

    FROM com_row_number base
    LEFT JOIN deficiencias d ON base.id_usuario_sk = d.id_usuario_sk
    LEFT JOIN beneficios b ON base.id_usuario_sk = b.id_usuario_sk
)

SELECT * FROM final
