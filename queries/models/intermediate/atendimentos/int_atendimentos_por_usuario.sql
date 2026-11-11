{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with eventos as (
    select
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
        {{ pu_ids_familias('id_familia') }} as id_familia,
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
        ) order by profissional_id, id_atendimento) as profissionais
    from {{ ref('int_atendimentos_bi') }}
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
