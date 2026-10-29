{{ config(enabled=target.name != 'prod', tags=['planilha_unificada', 'centro_pop_mensal']) }}
select
    mes_referencia,
    nome_centro_pop,
    usuario.nome as nome_usuario,
    usuario.nome_social,
    ultimo_tecnico as data_ultimo_atendimento_tecnico,
    quantidade as quantidade_atendimentos_mes,
    usuario.filiacao_mae,
    usuario.genero,
    usuario.orientacao_sexual,
    usuario.raca_cor,
    usuario.uf_nascimento as naturalidade,
    usuario.escolaridade_indice as nivel_escolaridade,
    '' as flag_empregabilidade_imediata,
    '' as capacidade_habilidades,
    '' as flag_interesse_curso,
    usuario.flag_cadunico as flag_possui_cadunico,
    usuario.nis as nis_usuario,
    '' as resultado_acesso,
    '' as resultado_descricao,
    row_number() over (order by mes_referencia, id_unidade, id_usuario) as numero,
    array_to_string(profissionais, ', ') as profissionais_atendimento,
    case when pontuais > 0 then 'Sim' when tipos_desconhecidos = 0 then 'Não' else '' end as flag_atendido_pontualmente,
    if(pai is not null, 'Sim', '') as flag_inserido_acompanhamento,
    (
        select min(safe_cast(substr(h.data_inclusao_pai, 1, 10) as date)) from unnest(historico) as h
        where h.tipo_registro = 'pai' and safe_cast(substr(h.data_inclusao_pai, 1, 10) as date) <= last_day(mes_referencia)
    ) as data_inclusao_acompanhamento,
    if(pai is not null, 'Sim', '') as flag_possui_plano_individual,
    coalesce(situacao_rua.motivo_principal_permanencia_rua, '') as motivo_principal_permanencia_rua,
    coalesce(atendimento_social.motivo_secundario_permanencia_rua, '') as motivo_secundario_permanencia_rua,
    safe_cast(substr(usuario.data_nascimento, 1, 10) as date) as data_nascimento,
    date_diff(last_day(mes_referencia), safe_cast(substr(usuario.data_nascimento, 1, 10) as date), year) - if(format_date('%m%d', last_day(mes_referencia)) < format_date('%m%d', safe_cast(substr(usuario.data_nascimento, 1, 10) as date)), 1, 0) as idade,
    concat('CPF: ', coalesce(nullif(usuario.cpf, ''), '-'), ' (', if(usuario.cpf != '', 'Sim', 'Não Informado'), '); RG: ', coalesce(nullif(usuario.numero_rg, ''), '-'), ' (', if(usuario.numero_rg != '', 'Sim', 'Não Informado'), '); RCN: ', coalesce(nullif(usuario.numero_certidao_nascimento, ''), '-'), ' (', if(usuario.possui_certidao_nascimento = 'true', 'Sim', 'Não Informado'), ')') as documentacao,
    case lower(trim(usuario.flag_frequenta_escola)) when 's' then 'Sim' when 'sim' then 'Sim' when 'n' then 'Não' when 'não' then 'Não' when 'nao' then 'Não' else '' end as flag_estuda,
    if(lower(usuario.flag_frequenta_escola) in ('s', 'sim'), usuario.serie_escolar, '') as ano_cursando,
    concat(coalesce(nullif({{ map_resposta_sim_nao('usuario.flag_deficiencia') }}, 'Não Informado'), ''), ': ', array_to_string(array(
        select d.descricao from unnest(usuario.deficiencia) as d
        order by d.descricao
    ), ', ')) as deficiencia,
    coalesce(saude.uso_substancias_psicoativas, '') as flag_uso_substancias_psicoativas,
    case
        when exists (
            select 1 from unnest(historico) as h
            where h.tipo_registro = 'saude' and h.possui_problema_saude = 'Sim'
        ) then 'Sim'
        else
            coalesce(saude.possui_problema_saude, ''
            )
    end as flag_problema_saude,
    coalesce(saude.realiza_acompanhamento_saude, '') as flag_acompanhamento_saude,
    coalesce(atendimento_social.possui_referencias_familiares, '') as flag_possui_vinculo_familiar,
    coalesce(atendimento_social.territorio_referencia_familia, '') as territorio_referencia_familia,
    coalesce(atendimento_social.possibilidade_reinsercao_familiar, '') as flag_possibilidade_reinsercao_familiar,
    concat(coalesce(nullif({{ map_resposta_sim_nao('usuario.flag_trabalha') }}, 'Não Informado'), ''), ': ', usuario.profissao) as atividade_renda,
    safe_cast(replace(usuario.renda_ativa, ',', '.') as numeric) as valor_renda,
    concat(coalesce(nullif({{ map_resposta_sim_nao('usuario.flag_recebe_beneficio') }}, 'Não Informado'), ''), ': ', array_to_string(array(
        select b.descricao from unnest(usuario.beneficio) as b
        order by b.descricao
    ), ', ')) as beneficios,
    case when safe_cast(substr(usuario.data_atualizacao, 1, 10) as date) > last_day(mes_referencia) or safe_cast(substr(usuario.data_atualizacao, 1, 10) as date) is null then '' when safe_cast(substr(usuario.data_limite_cadastro_atual, 1, 10) as date) >= last_day(mes_referencia) then 'Sim' when safe_cast(substr(usuario.data_limite_cadastro_atual, 1, 10) as date) < last_day(mes_referencia) then 'Não' else '' end as flag_cadunico_atualizado,
    if(array_length(participacoes) > 0, 'Sim', '') as flag_participacao_oficinas,
    nullif(array_length(participacoes), 0) as qtd_oficinas_participadas_mes,
    array_to_string(array(
        select distinct valor from unnest(historico) as h, unnest(h.demandas) as valor
        where h.id_unidade = c.id_unidade and date_trunc(safe_cast(substr(h.data_registro, 1, 10) as date), month) = c.mes_referencia and valor != ''
        order by valor
    ), ' | ') as demandas,
    array_to_string(array(
        select distinct valor from unnest(historico) as h, unnest(h.encaminhamentos) as enc, unnest([concat(enc.label, ': ', enc.valor)]) as valor
        where h.id_unidade = c.id_unidade and date_trunc(safe_cast(substr(h.data_registro, 1, 10) as date), month) = c.mes_referencia and valor != ''
        order by valor
    ), ' | ') as encaminhamentos,
    safe_cast(substr(desligamento.data_desligamento, 1, 10) as date) as data_desligamento,
    array_to_string([nullif(desligamento.motivo_desligamento, ''), nullif(desligamento.motivo_desligamento_outros, '')], '; ') as motivo_desligamento,
    array_to_string(array(
        select distinct valor from unnest(historico) as h, unnest([h.observacoes]) as valor
        where h.id_unidade = c.id_unidade and date_trunc(safe_cast(substr(h.data_registro, 1, 10) as date), month) = c.mes_referencia and valor != ''
        order by valor
    ), ' | ') as observacoes
from {{ ref('int_centro_pop_mensal') }} as c
