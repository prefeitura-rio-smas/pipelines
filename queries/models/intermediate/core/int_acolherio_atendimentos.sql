{{ config(materialized='table', tags=['planilha_unificada']) }}
-- Materializa a seleção uma vez para que os consumidores compartilhem os mesmos desempates.
-- Seleção de linhas de atendimento e profissional a partir dos raws do AcolheRio.
-- Aplica os rankings da busca programada com desempate determinístico.
WITH base_atendimentos AS (
    SELECT
        b.id_atendimento,
        b.modulo,
        b.data_atendimento,
        cast(b.hora_atendimento AS int64) AS hora_atendimento,
        b.id_tipo_atendimento,
        b.id_profissional,
        b.id_paciente,
        b.id_unidade,
        b.data_cadastro_atendimento,
        b.flag_cancelado,
        b.id_profissional_compartilhado,
        b.id_login_cadastro,
        f.id_familia
    FROM {{ ref('raw_atendimentos_usuarios') }} AS b
    LEFT JOIN {{ ref('raw_membros_familia') }} AS f ON b.id_paciente = f.id_paciente
    WHERE f.data_saida IS null

    UNION ALL

    SELECT
        id_atendimento,
        modulo,
        data_atendimento,
        cast(hora_atendimento AS int64) AS hora_atendimento,
        id_tipo_atendimento,
        id_profissional,
        id_paciente,
        id_unidade,
        data_cadastro_atendimento,
        flag_cancelado,
        id_profissional_compartilhado,
        id_login_cadastro,
        id_familia
    FROM {{ ref('raw_atendimentos_familias') }}
),

atendimentos_explodidos AS (
    -- Mantém o profissional principal e cada entrada não vazia da lista original.
    SELECT b.* REPLACE ({{ normalizar_profissional_atendimento('b.id_profissional') }} AS id_profissional)
    FROM base_atendimentos AS b

    UNION ALL

    SELECT b.* REPLACE (safe_cast(trim(regexp_replace(prof_id, r'^0+', '')) AS int64) AS id_profissional)
    FROM base_atendimentos AS b
    CROSS JOIN unnest(split(b.id_profissional_compartilhado)) AS prof_id
    WHERE prof_id != ''
),

enriquecidos AS (
    SELECT
        b.modulo,
        e.cas,
        CASE
            WHEN e.nome_unidade LIKE 'ALBERGUE%' THEN 'ALBERGUE'
            WHEN e.nome_unidade LIKE 'CRAF%' THEN 'CENTRAL DE RECEPÇÃO'
            WHEN e.nome_unidade LIKE 'CRAS%' THEN 'CRAS'
            WHEN e.nome_unidade LIKE 'CREAS%' THEN 'CREAS'
            WHEN e.nome_unidade LIKE 'CRI%' THEN 'CENTRAL DE RECEPÇÃO'
            WHEN e.nome_unidade LIKE 'REPÚBLICA%' THEN 'REPÚBLICA'
            WHEN e.nome_unidade LIKE 'URS%' THEN 'URS'
            WHEN e.nome_unidade LIKE 'CENTRO POP%' THEN 'CENTRO POP'
            WHEN e.nome_unidade LIKE 'ESCRITÓRIO%' THEN 'ESCRITÓRIO SOCIAL'
            WHEN e.nome_unidade LIKE 'LARES%' THEN 'LARES CARIOCAS'
            ELSE 'UNIDADE CONVENIADA'
        END AS tipo_unidade,
        e.nome_unidade AS unidade_atendimento,
        e.email_unidade,
        coalesce({{ email_cas('e.cas') }}, 'gdados.smas@prefeitura.rio') AS email_cas,
        b.id_atendimento AS seq_atendimento,
        b.data_atendimento AS data_de_atendimento,
        b.hora_atendimento AS hora_de_atendimento_original,
        format('%02d:%02d', div(b.hora_atendimento, 100), mod(b.hora_atendimento, 100)) AS hora_de_atendimento,
        b.id_paciente AS id_usuario,
        b.id_familia,
        p.nome AS nome_usuario,
        CASE WHEN p.cpf IS null OR p.cpf = '' THEN 'CPF não informado' ELSE 'CPF informado' END AS documentacao,
        CASE
            WHEN p.cpf IS null OR p.cpf = ''
                THEN null
            ELSE
                concat(
                    substring(lpad(p.cpf, 11, '0'), 1, 3), '.',
                    substring(lpad(p.cpf, 11, '0'), 4, 3), '.',
                    substring(lpad(p.cpf, 11, '0'), 7, 3), '-',
                    substring(lpad(p.cpf, 11, '0'), 10, 2)
                )
        END AS numero_documento,
        trim(regexp_replace(p.logradouro, r'[,\s;]+', ' ')) AS endereco,
        trim(regexp_replace(p.numero_endereco, r'[,\s;]+', ' ')) AS endereco_numero,
        trim(regexp_replace(p.complemento_endereco, r'[,\s;]+', ' ')) AS endereco_complemento,
        CASE
            WHEN p.bairro = '' THEN 'NAO INFORMADO'
            ELSE p.bairro
        END AS bairro,
        trim(regexp_replace(p.ponto_referencia, r'[,\s;]+', ' ')) AS referencia_ou_comunidade,
        c.descricao AS nome_atendimento_original,
        p.data_nascimento,
        date_diff(current_date(), p.data_nascimento, YEAR)
        - if(extract(DAYOFYEAR FROM p.data_nascimento) > extract(DAYOFYEAR FROM current_date()), 1, 0) AS idade,
        CASE
            WHEN p.raca_cor = '01' THEN 'Branca'
            WHEN p.raca_cor = '02' THEN 'Preta'
            WHEN p.raca_cor = '03' THEN 'Parda'
            WHEN p.raca_cor = '04' THEN 'Amarela'
            WHEN p.raca_cor = '05' THEN 'Indigena'
            ELSE 'Nao informado'
        END AS raca_cor,
        p.sexo,
        q.id_profissional AS profissional_id,

        CASE
            WHEN trim(upper(q.nome)) IN ('ATENDIMENTO RECEPÇÃO', 'ATENDIMENTO BUSCA ATIVA', 'ATENDIMENTO ENTREVISTADOR SOCIAL')
                THEN trim(upper(s.nome_operador))
            ELSE trim(upper(q.nome))
        END AS profissional,

        cb.descricao AS profissional_cbo_original,
        CASE
            WHEN cb.descricao LIKE 'Articulador Comunitário%' THEN 'Articulador Comunitário'
            WHEN cb.descricao LIKE 'Assistente administrativo%' THEN 'Assistente administrativo'
            WHEN cb.descricao LIKE 'Assistente Social%' THEN 'Assistente social'
            WHEN cb.descricao LIKE 'Assistente social%' THEN 'Assistente social'
            WHEN cb.descricao LIKE 'Educador social%' THEN 'Educador social'
            WHEN cb.descricao LIKE 'Entrevistador Social%' THEN 'Entrevistador social'
            WHEN cb.descricao LIKE 'Pedagogo%' THEN 'Pedagogo'
            WHEN cb.descricao LIKE 'Psicólogo%' THEN 'Psicólogo'
            WHEN cb.descricao LIKE 'Recepcionista%' THEN 'Recepcionista'
            ELSE cb.descricao
        END AS profissional_cbo,
        trim(upper(s.nome_operador)) AS cadastrante,
        b.data_cadastro_atendimento,
        b.id_unidade AS _id_unidade,
        b.id_tipo_atendimento AS _id_tipo_atendimento,
        b.flag_cancelado AS _flag_cancelado

    FROM atendimentos_explodidos AS b
    INNER JOIN {{ ref('raw_usuarios') }} AS p ON b.id_paciente = p.id_paciente
    LEFT JOIN {{ ref('raw_tipos_atendimento') }} AS c ON b.id_tipo_atendimento = c.id_tipo_atendimento
    LEFT JOIN {{ ref('raw_unidades') }} AS e ON b.id_unidade = e.id_unidade
    LEFT JOIN {{ ref('raw_operadores') }} AS s ON b.id_login_cadastro = s.id_login
    LEFT JOIN {{ ref('raw_profissionais') }} AS q ON b.id_profissional = q.id_profissional
    LEFT JOIN {{ ref('raw_profissionais_ocupacoes') }} AS v ON q.id_profissional = v.id_profissional
    LEFT JOIN {{ ref('raw_cbo') }} AS cb ON v.codigo_cbo = cb.codigo_cbo

    WHERE
        s.nome_operador NOT LIKE '%TESTE%'
        AND p.nome NOT LIKE '%TESTE%'
        AND e.nome_unidade NOT LIKE '%TESTE%'
),

classificados AS (
    SELECT
        a.*,
        CASE
            WHEN a.nome_atendimento_original LIKE '%Recepção%' THEN 'Atendimento Recepção'
            WHEN a.profissional = 'ATENDIMENTO RECEPÇÃO' THEN 'Atendimento Recepção'
            WHEN
                a.profissional_cbo IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                AND a.nome_atendimento_original LIKE '%CadÚnico%'
                THEN 'Atendimento Recepção'
            WHEN a.profissional_cbo IN ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo') THEN 'Atendimento Técnico'
            ELSE 'Outros Atendimentos'
        END AS tipo_atendimento,
        CASE
            WHEN
                a.profissional_cbo IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                AND a.tipo_unidade = 'CRAS'
                AND a.nome_atendimento_original LIKE '%CadÚnico%'
                THEN 'CRAS - Recepção - Ação CadÚnico'
            WHEN
                a.profissional_cbo IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                AND a.tipo_unidade = 'CREAS'
                AND a.nome_atendimento_original LIKE '%CadÚnico%'
                THEN 'CREAS - Recepção - Ação CadÚnico'
            ELSE a.nome_atendimento_original
        END AS nome_atendimento,
        CASE
            WHEN a.idade < 18 THEN 'Até 17 anos'
            WHEN a.idade >= 18 AND a.idade < 30 THEN 'De 18 a 29 anos'
            WHEN a.idade >= 30 AND a.idade < 45 THEN 'De 30 a 44 anos'
            WHEN a.idade >= 45 AND a.idade < 60 THEN 'De 45 a 59 anos'
            WHEN a.idade >= 60 AND a.idade < 75 THEN 'De 60 a 74 anos'
            WHEN a.idade >= 75 THEN 'Mais de 75 anos'
            ELSE 'Não Informado'
        END AS idade_faixa,

        concat(a.seq_atendimento, '-', a.profissional_id) AS id_atendimento,
        concat(a.email_cas, ',', a.email_unidade, ',', z.email) AS email
    FROM enriquecidos AS a
    LEFT JOIN {{ ref('raw_sheets_filtro_email_prontuario') }} AS z ON a.unidade_atendimento = z.unidade_atendimento
),

-- Seleção de projetos ativos no cadastro familiar, inclusive para atendimentos antigos.
projetos_ativos AS (
    SELECT
        id_familia,
        coalesce(
            {{ map_nome_projeto_social('id_projeto_social') }},
            format('Projeto não mapeado (ID %d)', id_projeto_social)
        ) AS nome_projeto
    FROM {{ ref('raw_familias_projetos_sociais') }}
    WHERE indicador_ativo = 'S' AND data_cancelamento IS null
),

projetos_por_familia AS (
    SELECT
        id_familia,
        string_agg(DISTINCT nome_projeto, ', ' ORDER BY nome_projeto) AS projetos_sociais
    FROM projetos_ativos
    GROUP BY id_familia
),

candidatos AS (
    SELECT
        a.*,
        p.projetos_sociais
    FROM classificados AS a
    LEFT JOIN projetos_por_familia AS p ON a.id_familia = p.id_familia
),

classificacao AS (
    {{ selecionar_participacoes_atendimento('candidatos') }}
)

SELECT
    a.* EXCEPT (projetos_sociais),
    a.projetos_sociais
FROM classificacao AS a
