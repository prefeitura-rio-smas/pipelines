{{ config(
    tags = ['daily'],
    alias = var('centro_pop_mart_alias', 'mart_planilha_unificada_centro_pop')
) }}
-- Planilha Unificada Centro POP: uma linha por usuário × unidade × mês.
-- Atendimentos permanecem detalhados no array atendimentos; as contagens mensais
-- aparecem uma única vez. Formulários são observados até o fechamento do mês.
-- Campos cadastrais e CadÚnico são snapshots atuais, sem reconstrução histórica.

with centro_pop as (
    -- Uma linha por unidade. Agrega nomes distintos em ordem estável para
    -- não multiplicar atendimentos nem descartar eventuais divergências.
    select
        id_unidade,
        string_agg(distinct nome_unidade, ' | ' order by nome_unidade) as nome_unidade
    from {{ ref('dim_unidades') }}
    where tipo_unidade = 'Centro POP'
    group by id_unidade
),

-- O fato tem uma linha por atendimento × profissional compartilhado.
atendimentos_candidatos as (
    select
        a.id_usuario,
        a.id_unidade,
        a.id_atendimento_modulo,
        a.id_atendimento,
        a.tipo_atendimento_descricao as nome_atendimento,
        dp.nome as profissional,
        safe_cast(a.data_atendimento as date) as data_atendimento,
        case
            -- Profissional de nível superior = grupo CBO 2 (descrições da fonte têm sufixo " CENTRO POP")
            when substr(trim(cast(dp.cbo_principal_codigo as string)), 1, 1) = '2' then 'Atendimento Técnico'
            when a.tipo_atendimento_descricao like '%Recepção%' then 'Atendimento Recepção'
            when dp.nome = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
            when
                dp.cbo_principal_descricao in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                and a.tipo_atendimento_descricao like '%CadÚnico%'
                then 'Atendimento Recepção'
            else 'Outros Atendimentos'
        end as tipo_atendimento
    from {{ ref('fct_atendimentos') }} as a
    inner join centro_pop as c on a.id_unidade = c.id_unidade
    left join {{ ref('dim_profissionais') }} as dp on a.id_profissional_sk = dp.id_profissional_sk
    where (a.flag_cancelado is null or a.flag_cancelado != 'S')
),

-- Deduplica apenas cópias idênticas dos atributos do fato. Se um ID tiver
-- usuários, unidades, datas ou nomes divergentes, as linhas continuam distintas
-- para que a auditoria dos detalhes identifique o conflito.
atendimentos_eventos as (
    select
        id_atendimento_modulo,
        id_atendimento,
        id_usuario,
        id_unidade,
        data_atendimento,
        nome_atendimento
    from atendimentos_candidatos
    group by id_atendimento_modulo, id_atendimento, id_usuario, id_unidade, data_atendimento, nome_atendimento
),

-- Profissionais compartilhados são agregados separadamente dos atributos do fato.
atendimentos_profissionais as (
    select
        id_atendimento_modulo,
        array_agg(distinct profissional ignore nulls order by profissional) as profissionais_atendimento,
        case
            when countif(tipo_atendimento = 'Atendimento Técnico') > 0 then 'Atendimento Técnico'
            when countif(tipo_atendimento = 'Atendimento Recepção') > 0 then 'Atendimento Recepção'
            else 'Outros Atendimentos'
        end as tipo_atendimento
    from atendimentos_candidatos
    group by id_atendimento_modulo
),

atendimentos as (
    select
        e.id_atendimento_modulo,
        e.id_atendimento,
        e.id_usuario,
        e.id_unidade,
        e.data_atendimento,
        e.nome_atendimento,
        p.profissionais_atendimento,
        p.tipo_atendimento
    from atendimentos_eventos as e
    left join atendimentos_profissionais as p on e.id_atendimento_modulo = p.id_atendimento_modulo
),

atendimentos_mes as (
    select
        id_usuario,
        id_unidade,
        date_trunc(data_atendimento, month) as mes_referencia,
        count(*) as qtd_atendimentos_total_mes,
        countif(tipo_atendimento = 'Atendimento Técnico') as qtd_atendimentos_tecnico_mes,
        countif(tipo_atendimento = 'Atendimento Recepção') as qtd_atendimentos_recepcao_mes,
        countif(tipo_atendimento = 'Outros Atendimentos') as qtd_atendimentos_outros_mes,
        countif(nome_atendimento is not null and lower(trim(nome_atendimento)) not in (
            'centro pop - plano de acompanhamento indiv. (pai)',
            'centro pop - plano de atendimento individual (pai)'
        )) as qtd_atendimentos_pontuais_mes,
        countif(nome_atendimento is null) as qtd_atendimentos_sem_tipo_mes,
        min(data_atendimento) as data_primeiro_atendimento_mes,
        max(data_atendimento) as data_ultimo_atendimento_mes,
        max(if(tipo_atendimento = 'Atendimento Técnico', data_atendimento, null)) as data_ultimo_atendimento_tecnico_mes,
        array_agg(struct(
            id_atendimento_modulo,
            id_atendimento,
            data_atendimento,
            nome_atendimento,
            tipo_atendimento,
            profissionais_atendimento
        ) order by data_atendimento, id_atendimento_modulo) as atendimentos
    from atendimentos
    group by id_usuario, id_unidade, mes_referencia
),

usuarios_mes as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['id_usuario', 'id_unidade', 'mes_referencia']) }} as id_usuario_unidade_mes,
        last_day(mes_referencia) as data_referencia
    from atendimentos_mes
),

familia_usuario as (
    select
        id_usuario_responsavel as id_usuario,
        id_familia
    from {{ ref('dim_familias') }}
    qualify row_number() over (
        partition by id_usuario_responsavel
        order by data_ultima_modificacao desc, id_familia desc
    ) = 1
),

usuarios as (
    select
        du.*,
        ru.uf_nascimento,
        sm.serie_escolar,
        safe_cast(det.data_cadunico as date) as data_cadunico_informada
    from {{ ref('dim_usuarios') }} as du
    left join {{ ref('raw_usuarios') }} as ru on du.id_usuario = ru.id_paciente
    left join {{ ref('raw_usuarios_saude_mental') }} as sm on du.id_usuario = sm.id_paciente
    left join {{ ref('raw_usuarios_detalhes') }} as det on du.id_usuario = det.id_paciente
),

-- IDs de evolução são qualificados pelo módulo para não misturar registros
-- da família e do usuário que tenham o mesmo número na origem.
evolucoes_centro_pop as (
    select
        e.id_unidade,
        e.codigo_abrangencia,
        e.descricao_evolucao,
        e.data_evolucao,
        concat(e.origem_modulo, ':', cast(e.id_evolucao as string)) as id_evolucao,
        coalesce(du.id_usuario, e.id_paciente_familia) as id_paciente
    from {{ ref('fct_evolucoes') }} as e
    left join {{ ref('dim_usuarios') }} as du on e.id_usuario_sk = du.id_usuario_sk
    where
        e.codigo_abrangencia = 29
        and e.data_cancelamento is null
        and coalesce(du.id_usuario, e.id_paciente_familia) is not null
),

campos_centro_pop as (
    select *
    from {{ extrair_campos_html_evolucao(
        source_relation = 'evolucoes_centro_pop',
        id_cols = ['id_paciente', 'id_unidade', 'id_evolucao', 'data_evolucao']
    ) }}
    where titulo_formulario in (
        'Centro POP - Plano de Atendimento Individual (PAI)',
        'Centro POP - Atendimento Social',
        'Centro POP - Desligamento PAI'
    )
),

formularios_centro_pop as (
    select
        id_paciente,
        id_unidade,
        id_evolucao,
        data_evolucao,
        titulo_formulario,
        max(if(label = 'Data do atendimento', nullif(valor, 'undefined'), null)) as data_atendimento_formulario,
        max(if(label = 'Possui referências familiares?', nullif(valor, 'undefined'), null)) as possui_referencias_familiares,
        max(if(label = 'Território da referência familiar', nullif(valor, 'undefined'), null)) as territorio_referencia_familia,
        max(if(label = 'Existe possibilidade de reinserção familiar?', nullif(valor, 'undefined'), null)) as possibilidade_reinsercao_familiar,
        max(if(label = 'Motivo secundário da ida às ruas', nullif(valor, 'undefined'), null)) as motivo_secundario_permanencia_rua,
        max(if(label = 'Data do desligamento', nullif(valor, 'undefined'), null)) as data_desligamento_formulario,
        max(if(label = 'Motivo do desligamento', nullif(valor, 'undefined'), null)) as motivo_desligamento,
        max(if(label = 'Outros', nullif(valor, 'undefined'), null)) as motivo_desligamento_outros
    from campos_centro_pop
    group by id_paciente, id_unidade, id_evolucao, data_evolucao, titulo_formulario
),

pai_registros as (
    select
        *,
        coalesce(
            safe.parse_date('%d/%m/%Y', regexp_extract(data_atendimento_formulario, r'^\d{2}/\d{2}/\d{4}')),
            safe_cast(regexp_extract(data_atendimento_formulario, r'^\d{4}-\d{2}-\d{2}') as date),
            safe_cast(data_evolucao as date)
        ) as data_inclusao
    from formularios_centro_pop
    where titulo_formulario = 'Centro POP - Plano de Atendimento Individual (PAI)'
),

pai_inclusao_mes as (
    select
        a.id_usuario_unidade_mes,
        min(p.data_inclusao) as data_inclusao_acompanhamento
    from usuarios_mes as a
    inner join pai_registros as p
        on
            a.id_usuario = p.id_paciente
            and a.id_unidade = p.id_unidade
            and safe_cast(p.data_evolucao as date) <= a.data_referencia
            and a.data_referencia >= p.data_inclusao
    group by a.id_usuario_unidade_mes
),

atendimento_social_form as (
    select * from formularios_centro_pop
    where titulo_formulario = 'Centro POP - Atendimento Social'
),

desligamento_form as (
    select * from formularios_centro_pop
    where titulo_formulario = 'Centro POP - Desligamento PAI'
),

atendimento_social_mes as (
    {{ ultimo_registro_ate_evento(
        relacao_evento = 'usuarios_mes', relacao_registros = 'atendimento_social_form',
        pares_chave = [
            {'evento': 'id_usuario', 'registro': 'id_paciente'},
            {'evento': 'id_unidade', 'registro': 'id_unidade'}
        ],
        id_evento = 'id_usuario_unidade_mes', data_evento = 'data_referencia',
        data_registro = 'data_evolucao', id_registro = 'id_evolucao',
        colunas_saida = ['possui_referencias_familiares', 'territorio_referencia_familia',
                        'possibilidade_reinsercao_familiar', 'motivo_secundario_permanencia_rua']
    ) }}
),

desligamento_mes as (
    {{ ultimo_registro_ate_evento(
        relacao_evento = 'usuarios_mes', relacao_registros = 'desligamento_form',
        pares_chave = [
            {'evento': 'id_usuario', 'registro': 'id_paciente'},
            {'evento': 'id_unidade', 'registro': 'id_unidade'}
        ],
        id_evento = 'id_usuario_unidade_mes', data_evento = 'data_referencia',
        data_registro = 'data_evolucao', id_registro = 'id_evolucao',
        colunas_saida = ['data_desligamento_formulario', 'motivo_desligamento', 'motivo_desligamento_outros']
    ) }}
),

-- Demandas e encaminhamentos são registros do mês, e não apenas os campos
-- de uma única evolução. O snapshot social acima conserva o último perfil.
campos_mensais as (
    select
        a.id_usuario_unidade_mes,
        case
            when
                c.label in ('Demanda inicial', 'Identificação das demandas apresentadas pelo usuário')
                and nullif(trim(c.valor), 'undefined') != ''
                then concat(c.titulo_formulario, ': ', c.valor)
        end as demanda,
        case
            when c.label like 'Encaminhamentos%' and nullif(trim(c.valor), 'undefined') != ''
                then concat(c.titulo_formulario, ' / ', c.label, ': ', c.valor)
        end as encaminhamento,
        case
            when nullif(nullif(trim(regexp_replace(c.observacoes, r'<[^>]*>', '')), 'undefined'), '') is not null
                then concat(c.titulo_formulario, ': ', trim(regexp_replace(c.observacoes, r'<[^>]*>', '')))
        end as observacao
    from usuarios_mes as a
    inner join campos_centro_pop as c
        on
            a.id_usuario = c.id_paciente
            and a.id_unidade = c.id_unidade
            and safe_cast(c.data_evolucao as date) between a.mes_referencia and a.data_referencia
),

formularios_mes as (
    select
        id_usuario_unidade_mes,
        string_agg(distinct demanda, ' | ' order by demanda) as demandas,
        string_agg(distinct encaminhamento, ' | ' order by encaminhamento) as encaminhamentos,
        string_agg(distinct observacao, ' | ' order by observacao) as observacoes
    from campos_mensais
    group by id_usuario_unidade_mes
),

-- Perfil individual: última resposta conhecida até o fechamento do mês.
evolucoes_cadastro as (
    select
        e.codigo_abrangencia,
        e.descricao_evolucao,
        e.data_evolucao,
        du.id_usuario as id_paciente,
        concat(e.origem_modulo, ':', cast(e.id_evolucao as string)) as id_evolucao
    from {{ ref('fct_evolucoes') }} as e
    inner join {{ ref('dim_usuarios') }} as du on e.id_usuario_sk = du.id_usuario_sk
    where e.codigo_abrangencia = 1 and e.data_cancelamento is null
),

situacao_saude as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_cadastro',
        group_cols = ['id_paciente', 'id_evolucao', 'data_evolucao'],
        codigo_abrangencia = 1,
        titulo_formulario = 'Situação de Saúde',
        campos = [
            {'label': 'Faz uso de substâncias psicoativas?', 'col': 'uso_substancias', 'type': 'string'},
            {'label': 'Situação', 'col': 'situacao_saude', 'type': 'string'},
            {'label': 'Local onde faz tratamento', 'col': 'local_tratamento', 'type': 'string'}
        ]
    ) }}
),

situacao_saude_mes as (
    {{ ultimo_registro_ate_evento(
        relacao_evento = 'usuarios_mes', relacao_registros = 'situacao_saude',
        pares_chave = [{'evento': 'id_usuario', 'registro': 'id_paciente'}],
        id_evento = 'id_usuario_unidade_mes', data_evento = 'data_referencia',
        data_registro = 'data_evolucao', id_registro = 'id_evolucao',
        colunas_saida = ['uso_substancias', 'situacao_saude', 'local_tratamento']
    ) }}
),

questionario_situacao_usuario as (
    select
        q.id_prontuario as id_usuario,
        q.id_evolucao,
        ql.data_resposta,
        ql.resposta as motivo_ida_ruas
    from {{ ref('raw_evolucoes_questionario') }} as q
    inner join {{ ref('raw_evolucoes_questionario_lista') }} as ql on q.id_evolucao = ql.id_evolucao
    where q.id_template = 3 and ql.id_questao = 18
),

questionario_situacao_usuario_mes as (
    {{ ultimo_registro_ate_evento(
        relacao_evento = 'usuarios_mes', relacao_registros = 'questionario_situacao_usuario',
        pares_chave = [{'evento': 'id_usuario', 'registro': 'id_usuario'}],
        id_evento = 'id_usuario_unidade_mes', data_evento = 'data_referencia',
        data_registro = 'data_resposta', id_registro = 'id_evolucao',
        colunas_saida = ['motivo_ida_ruas']
    ) }}
),

-- CadÚnico é associado por CPF, sem confundir sua família com a do prontuário.
documentos_cadunico as (
    select
        s.cpf_normalizado,
        if(count(distinct s.nis) = 1, max(s.nis), null) as nis,
        if(
            count(distinct safe_cast(trim(s.id_familia) as int64)) = 1,
            max(safe_cast(trim(s.id_familia) as int64)),
            null
        ) as id_familia_cadunico,
        if(
            count(distinct s.numero_rg_normalizado) = 1,
            max(s.numero_rg_normalizado),
            null
        ) as numero_rg,
        if(
            count(distinct s.numero_rg_normalizado) = 1,
            'Sim',
            'Não Informado'
        ) as flag_possui_rg,
        if(logical_or(safe_cast(s.id_certidao_civil as int64) = 1), 'Sim', 'Não Informado') as flag_possui_certidao_nascimento,
        if(
            count(distinct if(
                safe_cast(s.id_certidao_civil as int64) = 1,
                nullif(trim(s.id_termi_matricula_certidao), ''),
                null
            )) = 1,
            max(if(
                safe_cast(s.id_certidao_civil as int64) = 1,
                nullif(trim(s.id_termi_matricula_certidao), ''),
                null
            )),
            null
        ) as numero_certidao_nascimento
    from (
        select
            d.id_familia,
            d.id_certidao_civil,
            d.id_termi_matricula_certidao,
            m.nis,
            nullif(regexp_replace(coalesce(d.cpf, ''), r'[^0-9]', ''), '') as cpf_normalizado,
            case
                when nullif(trim(d.rg), '') is null then null
                when regexp_replace(trim(d.rg), r'^0+', '') = '' then '0'
                else regexp_replace(trim(d.rg), r'^0+', '')
            end as numero_rg_normalizado
        from {{ ref('raw_documento_pessoa') }} as d
        left join {{ ref('raw_identificacao_membro') }} as m on d.id_membro_familia = m.id_membro_familia
        where nullif(regexp_replace(coalesce(d.cpf, ''), r'[^0-9]', ''), '') is not null
    ) as s
    group by s.cpf_normalizado
),

oficinas as (
    select
        p.id_usuario,
        p.id_unidade,
        date_trunc(p.data_presenca, month) as mes_referencia,
        count(distinct p.id_atividade) as qtd_oficinas
    from {{ ref('fct_presencas_usuarios') }} as p
    inner join centro_pop as c on p.id_unidade = c.id_unidade
    group by p.id_usuario, p.id_unidade, mes_referencia
),

cadunico_atualizacao as (
    select
        data_atualizacao,
        safe_cast(trim(id_familia) as int64) as id_familia_cadunico,
        safe_cast(data_limite_catastro_atual as date) as data_limite_cadastro_atual
    from {{ ref('raw_identificacao_controle') }}
    where safe_cast(trim(id_familia) as int64) is not null
    qualify row_number() over (
        partition by id_familia
        order by data_atualizacao desc, data_alteracao desc, data_limite_catastro_atual desc
    ) = 1
),

final as (
    select
        a.id_usuario_unidade_mes,
        a.id_usuario,
        fam.id_familia,
        a.id_unidade,
        coalesce(c.nome_unidade, 'Não Informado') as nome_centro_pop,
        a.mes_referencia,
        a.data_referencia,
        a.data_primeiro_atendimento_mes,
        a.data_ultimo_atendimento_mes,
        a.data_ultimo_atendimento_tecnico_mes,
        a.atendimentos,
        array(
            select as struct
                t.nome_atendimento,
                t.tipo_atendimento,
                count(*) as quantidade
            from unnest(a.atendimentos) as t
            group by t.nome_atendimento, t.tipo_atendimento
            order by t.nome_atendimento, t.tipo_atendimento
        ) as atendimentos_por_tipo,
        coalesce(nullif(array_to_string(array(
            select distinct t.nome_atendimento from unnest(a.atendimentos) as t
            where t.nome_atendimento is not null
            order by t.nome_atendimento
        ), ' | '), ''), 'Não Informado') as nome_atendimento,
        coalesce(nullif(array_to_string(array(
            select distinct p from unnest(a.atendimentos) as t
            cross join unnest(t.profissionais_atendimento) as p
            order by p
        ), ', '), ''), 'Não Informado') as profissionais_atendimento,
        coalesce(nullif(trim(u.nome), ''), 'Não Informado') as nome_usuario,
        coalesce(nullif(trim(u.nome_social), ''), 'Não Informado') as nome_social,
        case
            when a.qtd_atendimentos_pontuais_mes > 0 then 'Sim'
            when a.qtd_atendimentos_sem_tipo_mes > 0 then 'Não Informado'
            else 'Não'
        end as flag_atendido_pontualmente,
        {{ map_flag_boolean('pim.data_inclusao_acompanhamento is not null') }} as flag_inserido_acompanhamento,
        {{ map_flag_boolean('pim.data_inclusao_acompanhamento is not null') }} as flag_possui_plano_individual,
        pim.data_inclusao_acompanhamento,
        a.qtd_atendimentos_total_mes,
        a.qtd_atendimentos_tecnico_mes,
        a.qtd_atendimentos_recepcao_mes,
        a.qtd_atendimentos_outros_mes,
        a.qtd_atendimentos_pontuais_mes,
        coalesce(nullif(q.motivo_ida_ruas, 'undefined'), 'Não Informado') as motivo_principal_permanencia_rua,
        coalesce(nullif(asf.motivo_secundario_permanencia_rua, ''), 'Não Informado') as motivo_secundario_permanencia_rua,
        u.data_nascimento,
        safe_cast({{ calc_idade('u.data_nascimento', 'a.data_referencia') }} as int64) as idade,
        coalesce(u.filiacao_mae, 'Não Informado') as filiacao_mae,
        if(nullif(trim(u.cpf), '') is not null, 'Sim', 'Não Informado') as flag_possui_cpf,
        coalesce(nullif(trim(u.cpf), ''), '-') as numero_cpf,
        coalesce(dc.flag_possui_rg, 'Não Informado') as flag_possui_rg,
        coalesce(dc.numero_rg, '-') as numero_rg,
        coalesce(dc.flag_possui_certidao_nascimento, 'Não Informado') as flag_possui_certidao_nascimento,
        coalesce(dc.numero_certidao_nascimento, '-') as numero_certidao_nascimento,
        coalesce(u.genero, 'Não Informado') as genero,
        coalesce(u.orientacao_sexual, 'Não Informado') as orientacao_sexual,
        coalesce(u.raca_cor, 'Não Informado') as raca_cor,
        coalesce(u.uf_nascimento, 'Não Informado') as naturalidade,
        case
            when lower(trim(u.flag_frequenta_escola)) in ('s', 'sim') then 'Sim'
            when lower(trim(u.flag_frequenta_escola)) in ('n', 'nao', 'não') then 'Não'
            else 'Não Informado'
        end as flag_estuda,
        case
            when lower(trim(u.flag_frequenta_escola)) in ('s', 'sim') then coalesce(u.serie_escolar, 'Não Informado')
            else 'Não Informado'
        end as ano_cursando,
        coalesce(u.escolaridade_indice, 'Não Informado') as nivel_escolaridade,
        case
            when lower(trim(u.flag_deficiencia)) in ('s', 'sim') then 'Sim'
            when lower(trim(u.flag_deficiencia)) in ('n', 'nao', 'não') then 'Não'
            else 'Não Informado'
        end as flag_deficiencia,
        coalesce({{ map_coluna_tipo_deficiencia('u.tipo_deficiencia') }}, 'Não Informado') as tipo_deficiencia,
        case
            when lower(trim(nullif(ss.uso_substancias, 'undefined'))) in ('s', 'sim') then 'Sim'
            when lower(trim(nullif(ss.uso_substancias, 'undefined'))) in ('n', 'nao', 'não') then 'Não'
            else 'Não Informado'
        end as flag_uso_substancias_psicoativas,
        case
            when lower(trim(ss.situacao_saude)) in ('n', 'nao', 'não') then 'Não'
            when nullif(trim(ss.situacao_saude), 'undefined') != '' then 'Sim'
            when u.flag_saude_mental_comprometida in (
                'A', 'D', 'Pessoa com aparente agravo de saúde mental',
                'Pessoa com diagnóstico (laudo médico) de doença mental'
            ) then 'Sim'
            else 'Não Informado'
        end as flag_problema_saude,
        case
            when lower(trim(ss.local_tratamento)) in ('n', 'nao', 'não') then 'Não'
            when nullif(trim(ss.local_tratamento), 'undefined') != '' then 'Sim'
            else 'Não Informado'
        end as flag_acompanhamento_saude,
        case
            when lower(trim(asf.possui_referencias_familiares)) in ('s', 'sim') then 'Sim'
            when lower(trim(asf.possui_referencias_familiares)) in ('n', 'nao', 'não') then 'Não'
            else 'Não Informado'
        end as flag_possui_vinculo_familiar,
        coalesce(nullif(trim(asf.territorio_referencia_familia), ''), 'Não Informado') as territorio_referencia_familia,
        case
            when lower(trim(asf.possibilidade_reinsercao_familiar)) in ('s', 'sim') then 'Sim'
            when lower(trim(asf.possibilidade_reinsercao_familiar)) in ('n', 'nao', 'não') then 'Não'
            else 'Não Informado'
        end as flag_possibilidade_reinsercao_familiar,
        case
            when lower(trim(u.flag_trabalha)) in ('s', 'sim') then 'Sim'
            when lower(trim(u.flag_trabalha)) in ('n', 'nao', 'não') then 'Não'
            else 'Não Informado'
        end as flag_exerce_atividade_renda,
        coalesce(nullif(trim(u.profissao), ''), 'Não Informado') as atividade_renda_qual,
        u.renda_ativa as valor_renda,
        cast('Não Informado' as string) as flag_empregabilidade_imediata,
        cast('Não Informado' as string) as capacidade_habilidades,
        cast('Não Informado' as string) as flag_interesse_curso,
        case
            when lower(trim(u.flag_recebe_beneficio)) in ('s', 'sim') then 'Sim'
            when lower(trim(u.flag_recebe_beneficio)) in ('n', 'nao', 'não') then 'Não'
            else 'Não Informado'
        end as flag_possui_beneficio,
        coalesce(u.tipo_beneficio, 'Não Informado') as tipo_beneficio,
        coalesce(nullif(array_to_string(array(
            select if(b.descricao is null, b.codigo, concat(b.codigo, ' - ', b.descricao))
            from unnest(u.beneficio) as b
            order by b.codigo
        ), ', '), ''), 'Não Informado') as beneficio,
        case
            when lower(trim(u.flag_cadunico)) in ('s', 'sim') then 'Sim'
            when lower(trim(u.flag_cadunico)) in ('n', 'nao', 'não') then 'Não'
            else 'Não Informado'
        end as flag_possui_cadunico,
        u.data_cadunico_informada,
        ic.data_atualizacao as data_atualizacao_cadunico,
        ic.data_limite_cadastro_atual as data_limite_atualizacao_cadunico,
        case
            when ic.data_limite_cadastro_atual is null or ic.data_atualizacao is null or ic.data_atualizacao > a.data_referencia then 'Não Informado'
            when ic.data_limite_cadastro_atual >= a.data_referencia then 'Sim'
            else 'Não'
        end as flag_cadunico_atualizado,
        coalesce(nullif(dc.nis, ''), 'Não Informado') as nis_usuario,
        {{ map_flag_boolean('nullif(ofc.qtd_oficinas, 0) > 0') }} as flag_participacao_oficinas,
        nullif(ofc.qtd_oficinas, 0) as qtd_oficinas_participadas_mes,
        coalesce(fm.demandas, 'Não Informado') as demandas,
        coalesce(fm.encaminhamentos, 'Não Informado') as encaminhamentos,
        cast('Não Informado' as string) as resultado_acesso,
        cast('Não Informado' as string) as resultado_descricao,
        coalesce(
            safe.parse_date('%d/%m/%Y', regexp_extract(df.data_desligamento_formulario, r'^\d{2}/\d{2}/\d{4}')),
            safe_cast(regexp_extract(df.data_desligamento_formulario, r'^\d{4}-\d{2}-\d{2}') as date)
        ) as data_desligamento,
        coalesce(nullif(array_to_string([
            nullif(df.motivo_desligamento, ''), nullif(df.motivo_desligamento_outros, '')
        ], '; '), ''), 'Não Informado') as motivo_desligamento,
        coalesce(fm.observacoes, 'Não Informado') as observacoes,
        coalesce(u.flag_situacao_rua, 'Não Informado') as flag_situacao_rua,
        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
    from usuarios_mes as a
    inner join centro_pop as c on a.id_unidade = c.id_unidade
    left join usuarios as u on a.id_usuario = u.id_usuario
    left join familia_usuario as fam on a.id_usuario = fam.id_usuario
    left join pai_inclusao_mes as pim on a.id_usuario_unidade_mes = pim.id_usuario_unidade_mes
    left join atendimento_social_mes as asf on a.id_usuario_unidade_mes = asf.id_usuario_unidade_mes
    left join desligamento_mes as df on a.id_usuario_unidade_mes = df.id_usuario_unidade_mes
    left join formularios_mes as fm on a.id_usuario_unidade_mes = fm.id_usuario_unidade_mes
    left join situacao_saude_mes as ss on a.id_usuario_unidade_mes = ss.id_usuario_unidade_mes
    left join questionario_situacao_usuario_mes as q on a.id_usuario_unidade_mes = q.id_usuario_unidade_mes
    left join documentos_cadunico as dc
        on nullif(regexp_replace(coalesce(u.cpf, ''), r'[^0-9]', ''), '') = dc.cpf_normalizado
    left join cadunico_atualizacao as ic on dc.id_familia_cadunico = ic.id_familia_cadunico
    left join oficinas as ofc
        on a.id_usuario = ofc.id_usuario and a.id_unidade = ofc.id_unidade and a.mes_referencia = ofc.mes_referencia
    where u.nome is null or lower(u.nome) not like '%teste%'
)

select
    *,
    row_number() over (order by mes_referencia, id_unidade, id_usuario) as numero
from final
