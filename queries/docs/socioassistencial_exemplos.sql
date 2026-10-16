-- BigQuery Standard SQL. Substituir projeto.dataset pela relação do ambiente.
-- São exemplos de extração, sujeitos à matriz de cobertura e homologação.
declare mes date default date '2026-09-01';

-- Volume de registros e pessoas atendidas são medidas diferentes.
select
    id_unidade,
    count(*) as registros_atendimento,
    count(distinct id_usuario) as pessoas_distintas
from `projeto.dataset.mart_socioassistencial_unificada`
where tipo_registro = 'ATENDIMENTO' and competencia = mes
group by id_unidade;

-- Dataviz: recorte V2 por integrante, sem filtrar antes pelo rank do principal.
-- COUNT aqui conta participações profissionais, não atendimentos.
select
    m.id_unidade,
    p.nome,
    count(*) as participacoes_profissionais,
    count(distinct m.id_registro) as atendimentos
from `projeto.dataset.mart_socioassistencial_unificada` as m,
    unnest(m.atendimento.profissionais) as p
where
    m.tipo_registro = 'ATENDIMENTO'
    and m.competencia = mes
    and m.atendimento.flag_elegivel_dataviz
    and p.atendimento_unico_rank = 1
group by m.id_unidade, p.nome;

-- Estoque e ingressos por unidade original do vínculo, sem exigir atendimento.
-- Não é a reprodução automática do A1/A2 legado: ver diferenças de atribuição.
select
    id_unidade,
    acompanhamento.nome_servico,
    count(distinct if(acompanhamento.flag_ativo_fechamento, id_familia, null)) as familias_no_fechamento,
    count(distinct if(acompanhamento.flag_novo_no_mes, id_familia, null)) as familias_ingressantes
from `projeto.dataset.mart_socioassistencial_unificada`
where tipo_registro = 'ACOMPANHAMENTO_MENSAL' and competencia = mes
group by id_unidade, acompanhamento.nome_servico;

-- Base C1/C6 com chaves de módulo. O universo de testes/serviço deve ser homologado.
select
    id_unidade,
    count(distinct if(
        not regexp_contains(atendimento.nome_atendimento_original, '(?i)recepção'),
        id_registro, null
    )) as atendimentos_exceto_recepcao,
    count(distinct if(
        regexp_contains(atendimento.nome_atendimento_original, '(?i)domiciliar'),
        id_registro, null
    )) as domiciliares
from `projeto.dataset.mart_socioassistencial_unificada`
where
    tipo_registro = 'ATENDIMENTO' and competencia = mes
    and (atendimento.flag_cancelado is null or atendimento.flag_cancelado != 'S')
group by id_unidade;

-- Participações, pessoas e atividades: três unidades de contagem.
select
    id_unidade,
    count(*) as participacoes,
    count(distinct id_usuario) as participantes,
    count(distinct atividade.id_atividade) as atividades_distintas,
    count(distinct if(atividade.flag_scfv and idade_na_referencia between 0 and 6, id_usuario, null)) as criancas_scfv_0_6
from `projeto.dataset.mart_socioassistencial_unificada`
where tipo_registro = 'ATIVIDADE_COLETIVA' and competencia = mes
group by id_unidade;

-- Encaminhamentos: declaração CadÚnico combinada, nunca inclusão/atualização inferidas.
select
    id_unidade,
    count(distinct coalesce(
        concat('familia:', cast(id_familia as string)),
        concat('usuario:', cast(id_usuario as string))
    )) as sujeitos_com_declaracao_cadunico
from `projeto.dataset.mart_socioassistencial_unificada`
where
    tipo_registro = 'ENCAMINHAMENTO' and competencia = mes
    and encaminhamento.data_cancelamento is null
    and regexp_contains(encaminhamento.destinos_informados, '(?i)Cadastro/Atualização Cadúnico')
group by id_unidade;

-- Evidência de plano PAI conhecida até o fechamento: não acompanhamento ativo.
select
    id_usuario,
    id_unidade,
    min(data_evento) as primeiro_registro_pai_conhecido
from `projeto.dataset.mart_socioassistencial_unificada`
where
    tipo_registro = 'FORMULARIO'
    and data_evento <= last_day(mes)
    and formulario.data_cancelamento is null
    and formulario.titulo_formulario = 'Centro POP - Plano de Atendimento Individual (PAI)'
group by id_usuario, id_unidade;

-- Auditoria ArcGIS. Nenhuma destas medidas é certificada como RMA L1/F1.
select
    abordagem.unidade_informada,
    count(*) as ocorrencias_repeat,
    count(distinct concat(cast(abordagem.ano_cluster as string), ':', abordagem.id_pessoa_cluster_anual)) as clusters_anuais,
    countif(abordagem.flag_contabilizavel_rma is null) as elegibilidade_pendente
from `projeto.dataset.mart_socioassistencial_unificada`
where tipo_registro = 'ABORDAGEM' and competencia = mes
group by abordagem.unidade_informada;

-- Planilha: contexto mensal repetido nos eventos. Oficinas exigem MAX por grupo,
-- e não SUM entre atendimentos. Nº deve ser gerado na apresentação.
select
    p.id_usuario_unidade_mes,
    max(p.qtd_oficinas_participadas_mes) as oficinas_no_mes,
    max(p.valor_renda) as renda_cadastral
from `projeto.dataset.mart_socioassistencial_unificada` as m,
    unnest(m.planilha_centro_pop) as p
where m.tipo_registro = 'ATENDIMENTO' and m.competencia = mes
group by p.id_usuario_unidade_mes;
