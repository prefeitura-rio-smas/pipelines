{{ config(tags = ['planilha_unificada']) }}

select p.usuario.id_usuario
from {{ ref('planilha_unificada') }} as p
where
    p.usuario is null
    or p.usuario.id_usuario is null
    or p.usuario.nome is null
    or p.usuario.nome_social is null
    or p.usuario.filiacao_mae is null
    or p.usuario.nacionalidade is null
    or p.usuario.condicao_estrangeira is null
    or p.usuario.pais_origem is null
    or p.usuario.cpf is null
    or p.usuario.data_nascimento is null
    or p.usuario.sexo is null
    or p.usuario.raca_cor is null
    or p.usuario.genero is null
    or p.usuario.bairro is null
    or p.usuario.prontuario is null
    or p.usuario.flag_cadunico is null
    or p.usuario.pontuacao is null
    or p.usuario.grau_dependencia is null
    or p.usuario.nome_apoiador is null
    or p.usuario.numero_processo_decisao_apoiada is null
    or p.usuario.renda_ativa is null
    or p.usuario.atvd_remunerada is null
    or p.usuario.flag_decisao_apoiada is null
    or p.usuario.flag_saude_mental_comprometida is null
    or p.usuario.motivo_acolhimento is null
    or p.usuario.orientacao_sexual is null
    or p.usuario.vinculo_trabalhista is null
    or p.usuario.flag_trabalha is null
    or p.usuario.profissao is null
    or p.usuario.flag_frequenta_escola is null
    or p.usuario.escolaridade_indice is null
    or p.usuario.flag_recebe_beneficio is null
    or p.usuario.flag_curatela is null
    or p.usuario.tipo_curatela is null
    or p.usuario.flag_deficiencia is null
    or p.usuario.flag_situacao_rua is null
    or p.usuario.origem_demanda is null
    or p.usuario.flag_possui_violacao_direito is null
    or p.usuario.nome_unidade_referencia is null
    or p.usuario.cadastro_complementar is null
    or p.usuario.cadastro_complementar.uf_nascimento is null
    or p.usuario.cadastro_complementar.serie_escolar is null
    or p.usuario.cadastro_complementar.data_cadunico_informada is null
    or p.usuario.documentos_cadunico is null
    or p.usuario.documentos_cadunico.nis is null
    or p.usuario.documentos_cadunico.numero_rg is null
    or p.usuario.documentos_cadunico.possui_rg is null
    or p.usuario.documentos_cadunico.possui_certidao_nascimento is null
    or p.usuario.documentos_cadunico.numero_certidao_nascimento is null
    or p.usuario.documentos_cadunico.data_atualizacao is null
    or p.usuario.documentos_cadunico.data_limite_cadastro_atual is null
    or p.usuario.documentos_cadunico.flag_familia_ambigua is null
    or p.usuario.documentos_cadunico.flag_nis_ambiguo is null
    or p.usuario.documentos_cadunico.flag_rg_ambiguo is null
    or p.usuario.documentos_cadunico.flag_certidao_ambigua is null
    or exists (
        select 1 from unnest(p.usuario.beneficio) as item1
        where item1.descricao is null
    )
    or exists (
        select 1 from unnest(p.usuario.deficiencia) as item1
        where item1.descricao is null
    )
    or exists (
        select 1 from unnest(p.usuario.violacoes) as item1
        where item1.descricao is null or item1.origem is null
    )
    or exists (
        select 1 from unnest(p.familias) as item0
        where item0.id_familia is null or item0.nome_responsavel is null or item0.flag_responsavel_atual is null or item0.flag_ativo is null or item0.data_ultima_modificacao is null or exists (
            select 1 from unnest(item0.vinculos) as item1
            where item1.data_entrada is null or item1.data_saida is null or item1.parentesco_responsavel is null or item1.motivo_saida is null) or exists (
            select 1 from unnest(item0.servicos) as item1
            where item1.nome_unidade is null or item1.data_cadastro is null or item1.data_cancelamento is null or item1.nome_servico is null
        )
    )
    or exists (
        select 1 from unnest(p.projetos_sociais) as item0
        where item0.nome_projeto_social is null or item0.nome_unidade is null or item0.indicador_ativo is null or item0.data_cadastro is null or item0.data_cancelamento is null
    )
    or p.atendimentos is null
    or p.atendimentos.quantidade is null
    or exists (
        select 1 from unnest(p.atendimentos.registros) as item1
        where item1.id_atendimento_modulo is null or item1.origem_modulo is null or item1.nome_unidade is null or item1.tipo_atendimento_descricao is null or item1.data_atendimento is null or item1.hora_atendimento is null or item1.flag_cancelado is null or exists (
            select 1 from unnest(item1.profissionais) as item2
            where item2.nome_profissional is null or exists (
                select 1 from unnest(item2.descricoes_cbo) as item3
                where item3 is null
            ) or item2.cbo_principal_descricao is null
        )
    )
    or p.evolucoes is null
    or p.evolucoes.quantidade is null
    or exists (
        select 1 from unnest(p.evolucoes.registros) as item1
        where item1.id_evolucao_modulo is null or item1.origem_modulo is null or item1.id_familia is null or item1.nome_unidade is null or item1.data_evolucao is null or item1.data_cancelamento is null or item1.tipo_evolucao is null or item1.nome_profissional is null or item1.nome_atividade is null or item1.descricao_evolucao is null or exists (
            select 1 from unnest(item1.formularios) as item2
            where item2.titulo_formulario is null or item2.observacoes is null or exists (
                select 1 from unnest(item2.campos) as item3
                where item3.label is null or item3.valor is null
            )
        )
    )
