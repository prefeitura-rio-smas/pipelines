-- Desempates estáveis do PR #163; publicar somente após validação de paridade.
CREATE OR REPLACE TABLE `rj-smas-dev.dashboard_acolherio.dev_atendimentos` AS (
    WITH base_atendimentos AS (
        SELECT
            b.seqatend,
            'u' AS modulo,
            b.dtentrada,
            cast(b.horaent AS int64) AS horaent,
            b.dtsaida,
            b.seqtpatend,
            b.seqprof,
            b.seqpac,
            b.sequs,
            b.datcadast,
            b.indlocalatend,
            b.indatendcanc,
            b.dsclstprof,
            b.seqlogincad,
            f.seqfamil
        FROM rj-smas.brutos_acolherio_staging.gh_atendimentos AS b
        LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros AS f ON b.seqpac = f.seqpac
        WHERE f.datsaida IS null

        UNION ALL

        SELECT
            seqatend,
            'f' AS modulo,
            dtentrada,
            cast(horaent AS int64) AS horaent,
            dtsaida,
            seqtpatend,
            seqprof,
            seqpac,
            sequs,
            datcadast,
            indlocalatend,
            indatendcanc,
            dsclstprof,
            seqlogincad,
            seqfamil
        FROM rj-smas.brutos_acolherio_staging.gh_atend_familia
    ),

    atendimentos_explodidos AS (
    -- Linhas originais
        SELECT
            seqatend,
            modulo,
            safe_cast(trim(cast(seqprof AS string)) AS int64) AS seqprof,
            dtentrada,
            horaent,
            dtsaida,
            seqtpatend,
            seqpac,
            seqfamil,
            sequs,
            datcadast,
            indlocalatend,
            indatendcanc,
            dsclstprof,
            seqlogincad
        FROM base_atendimentos

        UNION ALL

        -- Linhas explodidas da lista de profissionais
        SELECT
            seqatend,
            modulo,
            safe_cast(trim(regexp_replace(prof_id, r'^0+', '')) AS int64) AS seqprof,
            dtentrada,
            horaent,
            dtsaida,
            seqtpatend,
            seqpac,
            seqfamil,
            sequs,
            datcadast,
            indlocalatend,
            indatendcanc,
            dsclstprof,
            seqlogincad
        FROM base_atendimentos,
            unnest(split(dsclstprof)) AS prof_id
        WHERE prof_id != ''
    ),

    final AS (
        SELECT
            b.modulo,
            b.sequs AS _id_unidade,
            b.seqtpatend AS _id_tipo_atendimento,
            b.indatendcanc AS _flag_cancelado,
            cb.codcbo AS _codigo_cbo,
            e.apus AS cas,
            CASE
                WHEN e.dscus LIKE 'ALBERGUE%' THEN 'ALBERGUE'
                WHEN e.dscus LIKE 'CRAF%' THEN 'CENTRAL DE RECEPÇÃO'
                WHEN e.dscus LIKE 'CRAS%' THEN 'CRAS'
                WHEN e.dscus LIKE 'CREAS%' THEN 'CREAS'
                WHEN e.dscus LIKE 'CRI%' THEN 'CENTRAL DE RECEPÇÃO'
                WHEN e.dscus LIKE 'REPÚBLICA%' THEN 'REPÚBLICA'
                WHEN e.dscus LIKE 'URS%' THEN 'URS'
                WHEN e.dscus LIKE 'CENTRO POP%' THEN 'CENTRO POP'
                WHEN e.dscus LIKE 'ESCRITÓRIO%' THEN 'ESCRITÓRIO SOCIAL'
                WHEN e.dscus LIKE 'LARES%' THEN 'LARES CARIOCAS'
                ELSE 'UNIDADE CONVENIADA'
            END AS tipo_unidade,
            e.dscus AS unidade_atendimento,
            e.emailprof AS email_unidade,
            CASE
                WHEN e.apus = '10' THEN 'cas10@prefeitura.rio'
                WHEN e.apus = '09' THEN 'cas9@prefeitura.rio'
                WHEN e.apus = '08' THEN 'cas8@prefeitura.rio'
                WHEN e.apus = '07' THEN 'cas7@prefeitura.rio'
                WHEN e.apus = '06' THEN 'cas6@prefeitura.rio'
                WHEN e.apus = '05' THEN 'cas5@prefeitura.rio'
                WHEN e.apus = '04' THEN 'cas4@prefeitura.rio'
                WHEN e.apus = '03' THEN 'cas3@prefeitura.rio'
                WHEN e.apus = '02' THEN 'cas2@prefeitura.rio'
                WHEN e.apus = '01' THEN 'cas1@prefeitura.rio'
                ELSE 'gdados.smas@prefeitura.rio'
            END AS email_cas,
            b.seqatend AS seq_atendimento,
            b.dtentrada AS data_de_atendimento,
            b.horaent AS hora_de_atendimento_original,
            format('%02d:%02d', div(b.horaent, 100), mod(b.horaent, 100)) AS hora_de_atendimento,
            b.seqpac AS id_usuario,
            b.seqfamil AS id_familia,
            p.dscnomepac AS nome_usuario,
            CASE WHEN p.numcpfpac IS null OR p.numcpfpac = '' THEN 'CPF não informado' ELSE 'CPF informado' END AS documentacao,
            CASE
                WHEN p.numcpfpac IS null OR p.numcpfpac = ''
                    THEN null
                ELSE
                    concat(
                        substring(lpad(p.numcpfpac, 11, '0'), 1, 3), '.',
                        substring(lpad(p.numcpfpac, 11, '0'), 4, 3), '.',
                        substring(lpad(p.numcpfpac, 11, '0'), 7, 3), '-',
                        substring(lpad(p.numcpfpac, 11, '0'), 10, 2)
                    )
            END AS numero_documento,
            trim(regexp_replace(p.dslogradouro, r'[,\s;]+', ' ')) AS endereco,
            trim(regexp_replace(p.numend, r'[,\s;]+', ' ')) AS endereco_numero,
            trim(regexp_replace(p.complend, r'[,\s;]+', ' ')) AS endereco_complemento,
            CASE
                WHEN p.dscbairroender = '' THEN 'NAO INFORMADO'
                ELSE p.dscbairroender
            END AS bairro,
            trim(regexp_replace(p.pontorefe, r'[,\s;]+', ' ')) AS referencia_ou_comunidade,
            c.descatend AS nome_atendimento_original,
            p.datnascim AS data_nascimento,
            date_diff(current_date(), p.datnascim, YEAR)
            - if(extract(DAYOFYEAR FROM p.datnascim) > extract(DAYOFYEAR FROM current_date()), 1, 0) AS idade,
            CASE
                WHEN p.racacor = '01' THEN 'Branca'
                WHEN p.racacor = '02' THEN 'Preta'
                WHEN p.racacor = '03' THEN 'Parda'
                WHEN p.racacor = '04' THEN 'Amarela'
                WHEN p.racacor = '05' THEN 'Indigena'
                ELSE 'Nao informado'
            END AS raca_cor,
            p.indsexo AS sexo,
            q.seqprof AS profissional_id,

            CASE
                WHEN trim(upper(q.nomeprof)) IN ('ATENDIMENTO RECEPÇÃO', 'ATENDIMENTO BUSCA ATIVA', 'ATENDIMENTO ENTREVISTADOR SOCIAL') THEN trim(upper(s.nompess)) ELSE trim(upper(q.nomeprof))
            END AS profissional,

            cb.dsccbo AS profissional_cbo_original,
            CASE
                WHEN cb.dsccbo LIKE 'Articulador Comunitário%' THEN 'Articulador Comunitário'
                WHEN cb.dsccbo LIKE 'Assistente administrativo%' THEN 'Assistente administrativo'
                WHEN cb.dsccbo LIKE 'Assistente Social%' THEN 'Assistente social'
                WHEN cb.dsccbo LIKE 'Assistente social%' THEN 'Assistente social'
                WHEN cb.dsccbo LIKE 'Educador social%' THEN 'Educador social'
                WHEN cb.dsccbo LIKE 'Entrevistador Social%' THEN 'Entrevistador social'
                WHEN cb.dsccbo LIKE 'Pedagogo%' THEN 'Pedagogo'
                WHEN cb.dsccbo LIKE 'Psicólogo%' THEN 'Psicólogo'
                WHEN cb.dsccbo LIKE 'Recepcionista%' THEN 'Recepcionista'
                ELSE cb.dsccbo
            END AS profissional_cbo,
            trim(upper(s.nompess)) AS cadastrante,
            b.datcadast AS data_cadastro_atendimento

        FROM atendimentos_explodidos AS b
        INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac AS p ON b.seqpac = p.seqpac
        LEFT JOIN rj-smas.brutos_acolherio_staging.gh_tpatendimentos AS c ON b.seqtpatend = c.seqtpatend
        LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us AS e ON b.sequs = e.sequs
        LEFT JOIN rj-smas.brutos_acolherio_staging.gh_contas AS s ON b.seqlogincad = s.seqlogin
        LEFT JOIN rj-smas.brutos_acolherio_staging.gh_prof AS q ON b.seqprof = q.seqprof
        LEFT JOIN rj-smas.brutos_acolherio_staging.gh_profocup AS v ON q.seqprof = v.seqprof
        LEFT JOIN rj-smas.brutos_acolherio_staging.gh_cbo AS cb ON v.codcbo = cb.codcbo

        WHERE
            s.nompess NOT LIKE '%TESTE%'
            AND p.dscnomepac NOT LIKE '%TESTE%'
            AND e.dscus NOT LIKE '%TESTE%'

        ORDER BY
            e.apus,
            e.dscus,
            b.dtentrada,
            b.horaent
    ),

    filtro_email AS (
        SELECT
            a.*,
            CASE
                WHEN a.nome_atendimento_original LIKE '%Recepção%' THEN 'Atendimento Recepção'
                WHEN a.profissional = 'ATENDIMENTO RECEPÇÃO' THEN 'Atendimento Recepção'
                --CORRECAO DE REGISTROS FEITOS EM SETEMBRO/2025 (QUANDO APARECIAM MUITAS OPÇÕES DE ATENDIMENTO DE CADUNICO PARA PNM)
                WHEN
                    a.profissional_cbo IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                    AND a.nome_atendimento_original LIKE '%CadÚnico%'
                    THEN 'Atendimento Recepção'
                --FIM DA CORRECAO DE REGISTROS FEITOS EM SETEMBRO/2025
                WHEN a.profissional_cbo IN ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo') THEN 'Atendimento Técnico' -- alinhado com SUBPSB em 15/01/2026
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
        FROM final AS a
        LEFT JOIN rj-smas-dev.dashboard_acolherio.filtro_email_dev AS z ON a.unidade_atendimento = z.unidade_atendimento
    ),

    catalogo_projetos_sociais AS (
        SELECT *
        FROM unnest(array<struct<id_projeto_social int64, nome_projeto_social string>>[
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

    -- Situação atual do cadastro familiar, inclusive para atendimentos antigos.
    -- Um registro pode continuar com indativo = 'S' após o cancelamento.
    projetos_sociais_por_familia AS (
        SELECT
            vinculo.seqfamil AS id_familia,
            string_agg(
                DISTINCT coalesce(
                    catalogo.nome_projeto_social,
                    format('Projeto não mapeado (ID %d)', vinculo.seqprojsoc)
                ),
                ', ' ORDER BY coalesce(
                    catalogo.nome_projeto_social,
                    format('Projeto não mapeado (ID %d)', vinculo.seqprojsoc)
                )
            ) AS projetos_sociais
        FROM `rj-smas.brutos_acolherio_staging.gh_famil_projsociais` AS vinculo
        LEFT JOIN catalogo_projetos_sociais AS catalogo
            ON vinculo.seqprojsoc = catalogo.id_projeto_social
        WHERE
            vinculo.indativo = 'S'
            AND vinculo.datcancel IS null
        GROUP BY vinculo.seqfamil
    ),

    base AS (
        SELECT DISTINCT * FROM filtro_email
    )

    SELECT
        base.* EXCEPT (_id_unidade, _id_tipo_atendimento, _flag_cancelado, _codigo_cbo),
        row_number() OVER (PARTITION BY base.id_atendimento ORDER BY base.data_cadastro_atendimento ASC NULLS FIRST, base.modulo ASC NULLS FIRST, base.seq_atendimento ASC NULLS FIRST, base.id_usuario ASC NULLS FIRST, base.id_familia ASC NULLS FIRST, base._codigo_cbo ASC NULLS FIRST, base.profissional_id ASC NULLS FIRST, to_json_string((
            SELECT AS STRUCT
                base._codigo_cbo,
                base._flag_cancelado,
                base._id_tipo_atendimento,
                base._id_unidade,
                base.bairro,
                base.cadastrante,
                base.cas,
                base.data_cadastro_atendimento,
                base.data_de_atendimento,
                base.data_nascimento,
                base.documentacao,
                base.email,
                base.email_cas,
                base.email_unidade,
                base.endereco,
                base.endereco_complemento,
                base.endereco_numero,
                base.hora_de_atendimento,
                base.hora_de_atendimento_original,
                base.id_atendimento,
                base.id_familia,
                base.id_usuario,
                base.idade,
                base.idade_faixa,
                base.modulo,
                base.nome_atendimento,
                base.nome_atendimento_original,
                base.nome_usuario,
                base.numero_documento,
                base.profissional,
                base.profissional_cbo,
                base.profissional_cbo_original,
                base.profissional_id,
                base.raca_cor,
                base.referencia_ou_comunidade,
                base.seq_atendimento,
                base.sexo,
                base.tipo_atendimento,
                base.tipo_unidade,
                base.unidade_atendimento
        ))) AS cbo_unico_rank, -- resolve problema de duplicada de atendimento quando o profissional tem 2 cbo`s associados
        row_number() OVER (PARTITION BY base.profissional_id, base.hora_de_atendimento, base.data_de_atendimento, base.nome_atendimento_original, base.id_usuario, base.unidade_atendimento ORDER BY base.data_cadastro_atendimento ASC NULLS FIRST, base.modulo ASC NULLS FIRST, base.seq_atendimento ASC NULLS FIRST, base.id_usuario ASC NULLS FIRST, base.id_familia ASC NULLS FIRST, base._codigo_cbo ASC NULLS FIRST, base.profissional_id ASC NULLS FIRST, to_json_string((
            SELECT AS STRUCT
                base._codigo_cbo,
                base._flag_cancelado,
                base._id_tipo_atendimento,
                base._id_unidade,
                base.bairro,
                base.cadastrante,
                base.cas,
                base.data_cadastro_atendimento,
                base.data_de_atendimento,
                base.data_nascimento,
                base.documentacao,
                base.email,
                base.email_cas,
                base.email_unidade,
                base.endereco,
                base.endereco_complemento,
                base.endereco_numero,
                base.hora_de_atendimento,
                base.hora_de_atendimento_original,
                base.id_atendimento,
                base.id_familia,
                base.id_usuario,
                base.idade,
                base.idade_faixa,
                base.modulo,
                base.nome_atendimento,
                base.nome_atendimento_original,
                base.nome_usuario,
                base.numero_documento,
                base.profissional,
                base.profissional_cbo,
                base.profissional_cbo_original,
                base.profissional_id,
                base.raca_cor,
                base.referencia_ou_comunidade,
                base.seq_atendimento,
                base.sexo,
                base.tipo_atendimento,
                base.tipo_unidade,
                base.unidade_atendimento
        ))) AS atendimento_unico_rank, -- rank do agrupamento de atendimentos que tem todos os itens listados em comum, i.e. atendimentos repetidos
        projetos.projetos_sociais
    FROM base
    LEFT JOIN projetos_sociais_por_familia AS projetos
        ON base.id_familia = projetos.id_familia
    QUALIFY
        cbo_unico_rank = 1  -- filtrando apenas pelo primeiro cbo
        AND atendimento_unico_rank = 1 -- filtrando apenas por atendimentos nao repetidos
);
