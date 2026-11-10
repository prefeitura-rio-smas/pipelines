-- Executar no conector BigQuery do Looker Studio com o parâmetro nativo de e-mail habilitado.
-- Ajustar somente o projeto/dataset para o ambiente publicado.
select
    registro.id_usuario,
    registro.id_atendimento,
    registro.id_unidade,
    registro.nome_unidade,
    registro.id_tipo_atendimento,
    registro.tipo_atendimento_descricao,
    registro.data_atendimento,
    registro.hora_atendimento,
    registro.flag_cancelado,
    registro.cas,
    registro.tipo_unidade,
    registro.elegivel_indicador,
    registro.cadastrante,
    registro.data_cadastro_atendimento,
    registro.id_atendimento_modulo,
    registro.origem_modulo,
    registro.tipo_servico,
    registro.id_familia,
    registro.classificacao_atendimento,
    registro.quantidade_linhas_bi
from `rj-smas-dev.relatorio.prontuario_bi_atendimentos` as registro
where exists (
    select 1 from `rj-smas-dev.relatorio.prontuario_bi_permissoes` as permissao
    where
        permissao.id_unidade = registro.id_unidade
        and permissao.email = lower(trim(@DS_USER_EMAIL))
)
