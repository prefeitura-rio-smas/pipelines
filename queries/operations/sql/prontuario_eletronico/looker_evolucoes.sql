-- Executar no conector BigQuery do Looker Studio com o parâmetro nativo de e-mail habilitado.
-- Ajustar somente o projeto/dataset para o ambiente publicado.
select
    registro.id_usuario,
    registro.id_evolucao,
    registro.id_evolucao_modulo,
    registro.origem_modulo,
    registro.id_familia,
    registro.id_unidade,
    registro.nome_unidade,
    registro.tipo_servico,
    registro.id_profissional,
    registro.id_atividade,
    registro.data_evolucao,
    registro.tipo_evolucao,
    registro.descricao_evolucao,
    registro.nome_atividade,
    registro.nome_profissional,
    registro.formularios_texto
from `rj-smas-dev.relatorio.prontuario_bi_evolucoes` as registro
where exists (
    select 1 from `rj-smas-dev.relatorio.prontuario_bi_permissoes` as permissao
    where
        permissao.id_unidade = registro.id_unidade
        and permissao.email = lower(trim(@DS_USER_EMAIL))
)
