{% macro prontuario_email_autorizado(lista, email) -%}
exists (
    select 1
    from unnest(split(regexp_replace(coalesce({{ lista }}, ''), r'[;\r\n]', ','), ',')) as destinatario
    where lower(trim(destinatario)) = {{ email }}
)
{%- endmacro %}

{% macro prontuario_consumo_seguro(relacao, identidade) -%}
-- noqa: disable=ST06
-- Projeção explícita do contrato; campos sem proveniência segura são suprimidos.
with identidade as (select lower(trim({{ identidade }})) as email),

filtrados as (
    select
        b.id_usuario_unidade,
        b.usuario,
        b.familias,
        b.id_unidade,
        b.nome_unidade,
        i.email,
        array(select as struct
            r.id_projeto_social,
            r.nome_projeto_social,
            r.id_unidade,
            r.nome_unidade,
            r.indicador_ativo,
            r.data_cadastro,
            r.data_cancelamento
        from unnest(b.projetos_sociais) as r with offset as pos
        where cast(r.id_unidade as string) = b.id_unidade
        order by pos) as projetos_sociais,
        array(select as struct
            r.id_atendimento_modulo,
            r.id_familia,
            r.origem_modulo,
            r.id_unidade,
            r.nome_unidade,
            r.tipo_unidade,
            r.cas,
            r.data_atendimento_informada,
            r.hora_atendimento,
            r.tipo_atendimento_descricao,
            r.tipo_servico,
            r.classificacao_atendimento,
            cast(null as string) as projetos_sociais_familia,
            r.elegivel_indicador,
            r.atendimento_compartilhado,
            array(
                select as struct
                    p.id_profissional,
                    p.nome_profissional,
                    p.id_familia_referencia,
                    p.id_atendimento_legado,
                    p.descricoes_cbo,
                    p.cbo_principal_descricao,
                    p.profissional_cbo,
                    p.classificacao_atendimento,
                    p.nome_atendimento
                from unnest(r.profissionais) as p with offset as posicao
                order by posicao
            ) as profissionais,
            r.cadastrante,
            r.data_atendimento_sistema,
            r.email_unidade,
            r.email_cas
        from unnest(b.atendimentos.registros) as r with offset as pos
        where cast(r.id_unidade as string) = b.id_unidade
        order by pos) as atendimentos_registros,
        array(select as struct
            r.id_evolucao_modulo,
            r.origem_modulo,
            r.id_familia,
            r.id_unidade,
            r.nome_unidade,
            r.data_evolucao,
            r.data_cancelamento,
            r.tipo_evolucao,
            r.tipo_servico,
            r.nome_atividade,
            r.descricao_evolucao,
            r.nome_profissional,
            r.formularios
        from unnest(b.evolucoes.registros) as r with offset as pos
        where cast(r.id_unidade as string) = b.id_unidade
        order by pos) as evolucoes_registros,
        array(select as struct
            r.id_unidade,
            r.nome_unidade,
            {{ campos_perfil_centro_pop('r') }}
        from unnest(b.centro_pop.registros) as r with offset as pos
        where cast(r.id_unidade as string) = b.id_unidade
        order by pos) as centro_pop_registros,
        array(select as struct
            {{ campos_historico_centro_pop('r') }}
        from unnest(b.centro_pop.historico_mensal) as r with offset as pos
        where cast(r.id_unidade as string) = b.id_unidade
        order by pos) as centro_pop_historico_mensal,
        array(select as struct
            r.id_unidade,
            r.nome_unidade,
            {{ campos_perfil_creas('r') }}
        from unnest(b.creas.registros) as r with offset as pos
        where cast(r.id_unidade as string) = b.id_unidade
        order by pos) as creas_registros,
        array(select as struct
            r.id_ciclo,
            r.id_unidade,
            r.nome_unidade,
            r.data_entrada,
            r.data_saida,
            r.motivo_saida,
            r.tempo_permanencia_dias
        from unnest(b.alta_complexidade.registros) as r with offset as pos
        where cast(r.id_unidade as string) = b.id_unidade
        order by pos) as alta_complexidade_registros
    from {{ relacao }} as b cross join identidade as i
    where
        nullif(i.email, '') is not null
        and nullif(trim(b.id_unidade), '') is not null
        and {{ prontuario_email_autorizado('b.email_filtro', 'i.email') }}
)

select
    id_usuario_unidade,
    usuario.id_usuario as id_usuario,
    id_unidade,
    nome_unidade,
    email as email_filtro,
    struct(
        usuario.id_usuario,
        usuario.nome,
        usuario.nome_social,
        usuario.filiacao_mae,

        usuario.data_nascimento,
        usuario.idade,
        usuario.idade_faixa,
        usuario.uf_nascimento,
        usuario.cidade_nascimento,

        usuario.nacionalidade,
        usuario.pais_origem,
        usuario.condicao_estrangeira,

        usuario.sexo,
        usuario.genero,
        usuario.orientacao_sexual,
        usuario.raca_cor,

        usuario.possui_cpf,
        usuario.cpf,
        usuario.possui_rg,
        usuario.numero_rg,
        usuario.flag_rg_ambiguo,
        usuario.possui_certidao_nascimento,
        usuario.numero_certidao_nascimento,
        usuario.flag_certidao_ambigua,

        usuario.flag_cadunico,
        usuario.data_cadunico_informada,
        usuario.nis,
        usuario.flag_nis_ambiguo,

        usuario.bairro,
        cast(null as string) as logradouro,
        cast(null as string) as numero,
        cast(null as string) as complemento,
        cast(null as string) as referencia_ou_comunidade,

        usuario.escolaridade_indice,
        usuario.flag_frequenta_escola,
        usuario.serie_escolar,

        usuario.flag_trabalha,
        usuario.profissao,
        usuario.vinculo_trabalhista,
        usuario.tipo_atividade_remunerada,
        usuario.atvd_remunerada,
        usuario.renda_ativa,

        usuario.flag_recebe_beneficio,
        usuario.beneficio,

        usuario.flag_decisao_apoiada,
        usuario.numero_processo_decisao_apoiada,
        usuario.nome_apoiador,
        usuario.flag_saude_mental_comprometida,
        usuario.flag_curatela,
        usuario.tipo_curatela,

        usuario.flag_deficiencia,
        usuario.deficiencia,
        usuario.grau_dependencia,

        usuario.flag_situacao_rua,
        usuario.origem_demanda,
        usuario.motivo_acolhimento,
        cast(null as string) as nome_unidade_referencia,
        cast(null as string) as flag_possui_violacao_direito,
        cast([] as array<struct<descricao string, origem string>>) as violacoes,

        usuario.data_atualizacao,
        usuario.data_limite_cadastro_atual,
        usuario.flag_familia_ambigua,
        usuario.pontuacao
    ) as usuario,
    array(select as struct
        f.id_familia,
        f.nome_responsavel,
        f.flag_responsavel_atual,
        f.flag_ativo,
        f.data_ultima_modificacao,
        f.membros,
        array(
            select as struct
                v.parentesco_responsavel,
                v.data_entrada,
                v.data_saida,
                v.motivo_saida
            from unnest(f.vinculos) as v with offset as posicao
            order by posicao
        ) as vinculos,
        cast([] as array<struct<nome_unidade string, nome_servico string, data_cadastro string, data_cancelamento string>>) as servicos
    from unnest(familias) as f with offset as pos
    order by pos) as familias,
    projetos_sociais,
    struct(
        array_length(atendimentos_registros) as quantidade,
        (select countif(r.tipo_servico = 'cras') from unnest(atendimentos_registros) as r) as quantidade_cras,
        (select countif(r.tipo_servico = 'creas') from unnest(atendimentos_registros) as r) as quantidade_creas,
        (select countif(r.tipo_servico = 'centro_pop') from unnest(atendimentos_registros) as r) as quantidade_centro_pop,
        (select countif(r.tipo_servico = 'outros') from unnest(atendimentos_registros) as r) as quantidade_outros,
        atendimentos_registros as registros
    ) as atendimentos,
    struct(
        array_length(evolucoes_registros) as quantidade,
        (select countif(r.tipo_servico = 'cras') from unnest(evolucoes_registros) as r) as quantidade_cras,
        (select countif(r.tipo_servico = 'creas') from unnest(evolucoes_registros) as r) as quantidade_creas,
        (select countif(r.tipo_servico = 'centro_pop') from unnest(evolucoes_registros) as r) as quantidade_centro_pop,
        (select countif(r.tipo_servico = 'outros') from unnest(evolucoes_registros) as r) as quantidade_outros,
        evolucoes_registros as registros
    ) as evolucoes,
    struct(
        cast(null as string) as unidades_atendimento,
        cast(null as string) as possui_registro_pai,
        cast(null as string) as data_ocorrencia_pai,
        cast(null as string) as origem_data_ocorrencia_pai,
        cast(null as string) as situacao_data_ocorrencia_pai,
        cast(null as string) as data_registro_pai,
        cast(null as string) as data_resposta_pai,
        cast(null as string) as data_inclusao_pai,
        cast(null as string) as data_inclusao_pai_informada,
        cast(null as string) as flag_incluido_acompanhamento_pai,
        cast(null as string) as flag_empregabilidade_imediata,
        cast(null as string) as capacidade_habilidades,
        cast(null as string) as flag_interesse_curso,
        cast(null as string) as demandas_pai,
        cast(null as string) as encaminhamentos_pai,
        cast(null as string) as observacoes_pai,
        cast(null as string) as data_ocorrencia_atendimento_social,
        cast(null as string) as origem_data_ocorrencia_atendimento_social,
        cast(null as string) as situacao_data_ocorrencia_atendimento_social,
        cast(null as string) as data_registro_atendimento_social,
        cast(null as string) as data_resposta_atendimento_social,
        cast(null as string) as demandas_atendimento_social,
        cast(null as string) as encaminhamentos_atendimento_social,
        cast(null as string) as observacoes_atendimento_social,
        cast(null as string) as encaminhamentos,
        cast(null as string) as procedimentos,
        cast(null as string) as data_ocorrencia_saude,
        cast(null as string) as origem_data_ocorrencia_saude,
        cast(null as string) as situacao_data_ocorrencia_saude,
        cast(null as string) as data_registro_saude,
        cast(null as string) as data_resposta_saude,
        cast(null as string) as uso_substancias_psicoativas,
        cast(null as string) as situacao_saude,
        cast(null as string) as local_tratamento,
        cast(null as string) as possui_problema_saude,
        cast(null as string) as realiza_acompanhamento_saude,
        cast(null as string) as observacoes_saude,
        cast(null as string) as data_ocorrencia_situacao_rua,
        cast(null as string) as origem_data_ocorrencia_situacao_rua,
        cast(null as string) as situacao_data_ocorrencia_situacao_rua,
        cast(null as string) as data_registro_situacao_rua,
        cast(null as string) as data_resposta_situacao_rua,
        cast(null as string) as motivo_principal_permanencia_rua,
        cast(null as string) as motivo_secundario_permanencia_rua,
        cast(null as string) as possui_referencias_familiares,
        cast(null as string) as territorio_referencia_familia,
        cast(null as string) as possibilidade_reinsercao_familiar,
        cast(null as date) as data_ocorrencia_desligamento_pai,
        cast(null as string) as origem_data_ocorrencia_desligamento_pai,
        cast(null as string) as situacao_data_ocorrencia_desligamento_pai,
        cast(null as datetime) as data_hora_registro_desligamento_pai,
        cast(null as datetime) as data_hora_resposta_desligamento_pai,
        cast(null as date) as data_desligamento_pai,
        cast(null as string) as data_desligamento_pai_informada,
        cast(null as string) as motivo_desligamento_pai,
        cast(null as string) as complemento_motivo_desligamento_pai,
        cast(null as string) as observacoes_desligamento_pai,
        cast(null as string) as ultima_participacao_atividade,
        cast(null as string) as ultima_participacao_unidade,
        cast(null as string) as ultima_participacao_data,
        cast(null as string) as ultima_participacao_hora,
        centro_pop_registros as registros,
        centro_pop_historico_mensal as historico_mensal
    ) as centro_pop,
    struct(
        cast(null as integer) as mse_id_unidade,
        cast(null as string) as mse_nome_unidade,
        cast(null as date) as mse_data_ocorrencia,
        cast(null as datetime) as mse_data_registro,
        cast(null as string) as mse_data,
        cast(null as string) as mse_data_de_inclusao_no_mse,
        cast(null as string) as mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia,
        cast(null as string) as mse_com_quem_moraria,
        cast(null as string) as mse_1o_bimestre_documentacao,
        cast(null as string) as mse_1o_bimestre_beneficios,
        cast(null as string) as mse_1o_bimestre_cadunico,
        cast(null as string) as mse_1o_bimestre_localizacao,
        cast(null as string) as mse_1o_bimestre_outras_acoes,
        cast(null as string) as mse_1o_bimestre_saude,
        cast(null as string) as mse_adolescente_em_reinfracao,
        cast(null as string) as mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade,
        cast(null as string) as mse_avaliacao_tecnica_bimestral_1o_bimestre,
        cast(null as string) as mse_cras_de_referencia,
        cast(null as string) as mse_creas_de_referencia,
        cast(null as string) as mse_carga_horaria,
        cast(null as string) as mse_com_quem_residia_anteriormente,
        cast(null as string) as mse_desdobramento,
        cast(null as string) as mse_em_caso_de_cumprimento_de_mse_qual_o_status,
        cast(null as string) as mse_em_caso_de_cumprimento_de_psc_qual_o_local,
        cast(null as string) as mse_endereco_cras,
        cast(null as string) as mse_endereco_creas,
        cast(null as string) as mse_especialidade,
        cast(null as string) as mse_gostaria_de_retornar_para_casa_ou_convivio_anterior,
        cast(null as string) as mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria,
        cast(null as string) as mse_local,
        cast(null as string) as mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia,
        cast(null as string) as mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia,
        cast(null as string) as mse_medida_aplicada,
        cast(null as string) as mse_nome_do_local_de_cumprimento_de_psc,
        cast(null as string) as mse_outras_atividades,
        cast(null as string) as mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia,
        cast(null as string) as mse_procedimento,
        cast(null as string) as mse_profissional_cras,
        cast(null as string) as mse_profissional_creas,
        cast(null as string) as mse_quais_atividades_gostaria_de_participar,
        cast(null as string) as mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido,
        cast(null as string) as mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc,
        cast(null as string) as mse_telefone_cras,
        cast(null as string) as mse_telefone_creas,
        cast(null as string) as mse_tempo_de_cumprimento,
        cast(null as string) as mse_tipo,
        cast(null as string) as mse_tipo_de_ato_infracional,
        cast(null as string) as mse_usuario_interditado,
        cast(null as string) as mse_observacoes,
        cast(null as integer) as mse_retorno_id_unidade,
        cast(null as string) as mse_retorno_nome_unidade,
        cast(null as date) as mse_retorno_data_ocorrencia,
        cast(null as datetime) as mse_retorno_data_registro,
        cast(null as string) as mse_retorno_data_do_desligamento,
        cast(null as string) as mse_retorno_adolescente_em_reinfracao,
        cast(null as string) as mse_retorno_desligamento,
        cast(null as string) as mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status,
        cast(null as string) as mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local,
        cast(null as string) as mse_retorno_motivo_do_desligamento,
        cast(null as string) as mse_retorno_nome_do_local_de_cumprimento_de_psc,
        cast(null as string) as mse_retorno_observacoes,
        cast(null as integer) as peti_id_unidade,
        cast(null as string) as peti_nome_unidade,
        cast(null as date) as peti_data_ocorrencia,
        cast(null as datetime) as peti_data_registro,
        cast(null as string) as peti_data_da_inclusao_no_peti,
        cast(null as string) as peti_demanda_inicial,
        cast(null as string) as peti_forma_de_acesso_informe_a_origem_de_acesso,
        cast(null as string) as peti_marcacao_nos_campo_cadunico,
        cast(null as string) as peti_orientacoes_e_encaminhamentos,
        cast(null as string) as peti_outras_demandas_vulnerabilidades,
        cast(null as string) as peti_outras_formas_de_acesso,
        cast(null as string) as peti_outros,
        cast(null as string) as peti_tipo_de_trabalho_infantil,
        cast(null as string) as peti_e_acompanhado_em_outro_creas,
        cast(null as string) as peti_e_necessario_realizar_a_gestao_de_beneficios,
        cast(null as string) as peti_observacoes,
        cast(null as integer) as peti_desligamento_id_unidade,
        cast(null as string) as peti_desligamento_nome_unidade,
        cast(null as date) as peti_desligamento_data_ocorrencia,
        cast(null as datetime) as peti_desligamento_data_registro,
        cast(null as string) as peti_desligamento_data_do_desligamento_do_peti,
        cast(null as string) as peti_desligamento_motivo_do_desligamento,
        cast(null as string) as peti_desligamento_outros,
        cast(null as string) as peti_desligamento_observacoes,
        cast(null as integer) as scfv_id_unidade,
        cast(null as string) as scfv_nome_unidade,
        cast(null as date) as scfv_data_ocorrencia,
        cast(null as datetime) as scfv_data_registro,
        cast(null as string) as scfv_data_da_solicitacao_do_acolhimento,
        cast(null as string) as scfv_demanda_de_acolhimento_institucional_por_decisao_judicial,
        cast(null as string) as scfv_nome_do_cras_responsavel,
        cast(null as string) as scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos,
        cast(null as string) as scfv_oriundo_de_instituicao_de_acolhimento_interditado,
        cast(null as string) as scfv_usuario_interditado,
        cast(null as string) as scfv_orgao_de_origem_do_acolhimento,
        cast(null as string) as scfv_observacoes,
        cast(null as string) as passo_a_passo_ultima_unidade,
        cast(null as string) as passo_a_passo_ultima_atividade,
        cast(null as date) as passo_a_passo_ultima_data,
        cast(null as string) as passo_a_passo_ultima_hora,
        creas_registros as registros
    ) as creas,
    struct(
        (
            select r.id_ciclo from unnest(alta_complexidade_registros) as r
            order by if(r.data_saida is null, 0, 1), r.data_entrada desc nulls last, r.id_ciclo desc, to_json_string(r) desc limit 1
        ) as id_ciclo,
        (
            select r.id_unidade from unnest(alta_complexidade_registros) as r
            order by if(r.data_saida is null, 0, 1), r.data_entrada desc nulls last, r.id_ciclo desc, to_json_string(r) desc limit 1
        ) as id_unidade,
        (
            select r.nome_unidade from unnest(alta_complexidade_registros) as r
            order by if(r.data_saida is null, 0, 1), r.data_entrada desc nulls last, r.id_ciclo desc, to_json_string(r) desc limit 1
        ) as nome_unidade,
        (
            select r.data_entrada from unnest(alta_complexidade_registros) as r
            order by if(r.data_saida is null, 0, 1), r.data_entrada desc nulls last, r.id_ciclo desc, to_json_string(r) desc limit 1
        ) as data_entrada,
        (
            select r.data_saida from unnest(alta_complexidade_registros) as r
            order by if(r.data_saida is null, 0, 1), r.data_entrada desc nulls last, r.id_ciclo desc, to_json_string(r) desc limit 1
        ) as data_saida,
        (
            select r.motivo_saida from unnest(alta_complexidade_registros) as r
            order by if(r.data_saida is null, 0, 1), r.data_entrada desc nulls last, r.id_ciclo desc, to_json_string(r) desc limit 1
        ) as motivo_saida,
        (
            select r.tempo_permanencia_dias from unnest(alta_complexidade_registros) as r
            order by if(r.data_saida is null, 0, 1), r.data_entrada desc nulls last, r.id_ciclo desc, to_json_string(r) desc limit 1
        ) as tempo_permanencia_dias,
        alta_complexidade_registros as registros
    ) as alta_complexidade
from filtrados
where array_length(projetos_sociais) + array_length(atendimentos_registros) + array_length(evolucoes_registros) + array_length(centro_pop_registros) + array_length(centro_pop_historico_mensal) + array_length(creas_registros) + array_length(alta_complexidade_registros) > 0
{% endmacro %}

{% macro prontuario_projecao_cartoes(relacao) -%}
-- Projeção de publicação para os cartões do Looker. Uma linha por cidadão, unidade e conjunto de famílias
-- do atendimento. Os nomes e tipos publicados são preservados.
select  -- noqa: AM04
    p.* replace (
        if(
            grupo is null,
            p.familias,
            array(
                select as struct f.* replace (id_referencia as id_familia)
                from unnest(grupo.ids_familia) as id_referencia
                left join unnest(p.familias) as f on f.id_familia = id_referencia
                order by id_referencia
            )
        ) as familias,
        (
            select as struct
                p.atendimentos.* replace (
                    coalesce(array_length(grupo.eventos), 0) as quantidade,
                    (select countif(e.tipo_servico = 'cras') from unnest(grupo.eventos) as e) as quantidade_cras,
                    (select countif(e.tipo_servico = 'creas') from unnest(grupo.eventos) as e) as quantidade_creas,
                    (select countif(e.tipo_servico = 'centro_pop') from unnest(grupo.eventos) as e) as quantidade_centro_pop,
                    (select countif(e.tipo_servico = 'outros') from unnest(grupo.eventos) as e) as quantidade_outros,
                    array(
                        select as struct
                            evento.* replace (
                                array(
                                    select as struct prof.* replace (evento.cadastrante as id_profissional)
                                    from unnest(evento.profissionais) as prof with offset as posicao_profissional
                                    order by posicao_profissional
                                ) as profissionais
                            )
                        from unnest(grupo.eventos) as evento with offset as posicao_evento
                        order by posicao_evento
                    ) as registros
                )
        ) as atendimentos
    )
from {{ relacao }} as p
left join unnest(array(
    select as struct
        ids_familia,
        array_agg(evento order by posicao) as eventos
    from (
        select
            evento,
            posicao,
            array(
                select distinct trim(prof.id_familia_referencia)
                from unnest(evento.profissionais) as prof
                where nullif(trim(prof.id_familia_referencia), '') is not null
                order by 1
            ) as ids_familia
        from unnest(p.atendimentos.registros) as evento with offset as posicao
    ) as eventos_com_familia
    group by ids_familia
)) as grupo
{%- endmacro %}
