{% macro normalizar_profissional_atendimento(coluna) -%}
    safe_cast(trim(cast({{ coluna }} as string)) as int64)
{%- endmacro %}

{% macro selecionar_atendimentos() %}
-- Seleção de linhas de atendimento e profissional a partir dos raws do AcolheRio.
-- Replica os rankings publicados, inclusive a chave de CBO sem o módulo e os empates sem ordem definida.
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
        b.data_cadastro_atendimento

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
    LEFT JOIN {{ source('dashboard_acolherio', 'filtro_email_dev') }} AS z ON a.unidade_atendimento = z.unidade_atendimento
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

classificacao AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY id_atendimento
            ORDER BY data_cadastro_atendimento
        ) AS cbo_unico_rank,
        row_number() OVER (
            PARTITION BY
                profissional_id, hora_de_atendimento, data_de_atendimento,
                nome_atendimento_original, id_usuario, unidade_atendimento
        ) AS atendimento_unico_rank
    FROM classificados
    QUALIFY cbo_unico_rank = 1 AND atendimento_unico_rank = 1
)

SELECT
    a.*,
    p.projetos_sociais
FROM classificacao AS a
LEFT JOIN projetos_por_familia AS p ON a.id_familia = p.id_familia
{% endmacro %}

{% macro agrupar_atendimentos() %}
with linhas_preparadas as (
    with eventos_origem as (
        select
            modulo,
            id_atendimento,
            id_paciente as id_usuario,
            id_unidade,
            id_tipo_atendimento,
            flag_cancelado,
            data_atendimento
        from {{ ref('raw_atendimentos_usuarios') }}
        union all
        select
            modulo,
            id_atendimento,
            id_paciente as id_usuario,
            id_unidade,
            id_tipo_atendimento,
            flag_cancelado,
            data_atendimento
        from {{ ref('raw_atendimentos_familias') }}
    ),

    contextos as (
        select
            modulo,
            id_atendimento,
            if(
                count(distinct to_json_string(struct(id_unidade, id_tipo_atendimento, flag_cancelado))) > 1,
                error('Contextos internos conflitantes para atendimento selecionado'),
                array_agg(struct(id_unidade, id_tipo_atendimento, flag_cancelado) limit 1)[offset(0)]
            ) as contexto
        from eventos_origem
        group by modulo, id_atendimento
    ),

    cbos as (
        select
            p.id_profissional,
            c.descricao,
            if(count(distinct p.codigo_cbo) = 1, max(p.codigo_cbo), null) as codigo_cbo
        from {{ ref('raw_profissionais_ocupacoes') }} as p
        inner join {{ ref('raw_cbo') }} as c on p.codigo_cbo = c.codigo_cbo
        group by p.id_profissional, c.descricao
    ),

    selecionados as (
        select
            s.*,
            e.contexto.id_unidade as _id_unidade,
            e.contexto.id_tipo_atendimento as _id_tipo_atendimento,
            e.contexto.flag_cancelado as _flag_cancelado,
            c.codigo_cbo as _codigo_cbo
        from (
    {{ selecionar_atendimentos() }}
) as s
        left join contextos as e on s.modulo = e.modulo and s.seq_atendimento = e.id_atendimento
        left join cbos as c on s.profissional_id = c.id_profissional and s.profissional_cbo_original = c.descricao
    ),

    enriquecidos as (
        {{ aplicar_filtro_email('selecionados', ref('raw_unidades'), source('dashboard_acolherio', 'filtro_email_dev')) }}
    ),

    datas as (
        select
            modulo,
            id_atendimento,
            id_usuario,
            id_unidade,
            if(count(distinct data_atendimento) > 1, error('Datas conflitantes para atendimento'), min(data_atendimento)) as data_atendimento
        from eventos_origem
        group by modulo, id_atendimento, id_usuario, id_unidade
    )

    select
        b.modulo,
        b.cas,
        b.tipo_unidade,
        b.unidade_atendimento,
        b.email_unidade,
        b.email_cas,
        b.seq_atendimento,
        datas.data_atendimento as data_de_atendimento,
        b.hora_de_atendimento,
        b.id_usuario,
        b.id_familia,
        b.nome_atendimento_original,
        b.profissional_id,
        b.profissional,
        b.profissional_cbo_original,
        b.profissional_cbo,
        b.cadastrante,
        b.data_cadastro_atendimento,
        b.tipo_atendimento,
        b.nome_atendimento,
        b.id_atendimento,
        b.email,
        b.projetos_sociais,
        b._id_unidade,
        b._id_tipo_atendimento,
        b._flag_cancelado,
        b._codigo_cbo
    from enriquecidos as b
    left join datas on
        b.modulo = datas.modulo and b.seq_atendimento = datas.id_atendimento
        and b.id_usuario = datas.id_usuario and b._id_unidade = datas.id_unidade

),

eventos as (
    select  -- noqa: ST06
        id_usuario,
        seq_atendimento as id_atendimento,
        _id_unidade as id_unidade,
        unidade_atendimento as nome_unidade,
        _id_tipo_atendimento as id_tipo_atendimento,
        nome_atendimento_original as tipo_atendimento_descricao,
        data_de_atendimento as data_atendimento,
        hora_de_atendimento as hora_atendimento,
        _flag_cancelado as flag_cancelado,
        cas,
        email_unidade,
        email_cas,
        email,
        tipo_unidade,
        if(tipo_unidade in ('CRAS', 'CREAS', 'CENTRO POP') and unidade_atendimento != 'Negado', 'Sim', 'Não') as elegivel_indicador,
        cadastrante,
        data_cadastro_atendimento,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        case modulo when 'u' then 'usuario' when 'f' then 'familia' end as origem_modulo,
        case tipo_unidade
            when 'CRAS' then 'cras' when 'CREAS' then 'creas'
            when 'CENTRO POP' then 'centro_pop' else 'outros'
        end as tipo_servico,
        coalesce(string_agg(distinct cast(id_familia as string), ', ' order by cast(id_familia as string)), '') as id_familia,
        max(projetos_sociais) as projetos_sociais_familia,
        case
            when countif(tipo_atendimento = 'Atendimento Recepção') > 0 then 'Atendimento Recepção'
            when countif(tipo_atendimento = 'Atendimento Técnico') > 0 then 'Atendimento Técnico'
            else 'Outros Atendimentos'
        end as classificacao_atendimento,
        array_agg(struct(
            profissional_id as id_profissional,
            profissional as nome_profissional,
            array(
                select cast(codigo as string)
                from unnest([_codigo_cbo]) as codigo
                where codigo is not null
            ) as codigos_cbo,
            array(
                select descricao
                from unnest([profissional_cbo_original]) as descricao
                where descricao is not null
            ) as descricoes_cbo,
            cast(_codigo_cbo as string) as cbo_principal_codigo,
            profissional_cbo_original as cbo_principal_descricao,
            profissional_cbo,
            tipo_atendimento as classificacao_atendimento,
            nome_atendimento,
            id_familia as id_familia_referencia,
            id_atendimento as id_atendimento_legado
        ) order by profissional_id, id_atendimento) as profissionais,
        if(
            count(distinct nullif(trim(cast(profissional_id as string)), '')) >= 2,
            'Sim',
            'Não'
        ) as atendimento_compartilhado
    from linhas_preparadas
    where {{ nao_cancelado('_flag_cancelado') }}
    group by
        id_usuario, id_atendimento_modulo, id_atendimento, origem_modulo,
        id_unidade, nome_unidade, tipo_servico, id_tipo_atendimento,
        tipo_atendimento_descricao, data_atendimento, hora_atendimento, flag_cancelado,
        cas, email_unidade, email_cas, email, cadastrante, data_cadastro_atendimento, tipo_unidade
)

select
    id_usuario,
    array_agg((select as struct e.* except (id_usuario)) order by data_atendimento, hora_atendimento, id_atendimento_modulo) as atendimentos
from eventos as e
group by id_usuario
{% endmacro %}
