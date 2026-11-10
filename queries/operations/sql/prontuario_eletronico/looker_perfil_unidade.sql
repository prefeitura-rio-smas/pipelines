-- Executar no conector BigQuery do Looker Studio com o parâmetro nativo de e-mail habilitado.
-- Ajustar somente o projeto/dataset para o ambiente publicado.
select
    registro.id_usuario,
    registro.id_unidade,
    registro.nome_unidade,
    registro.centro_pop,
    registro.creas,
    registro.alta_complexidade,
    registro.usuario,
    registro.quantidade_atendimentos,
    registro.quantidade_linhas_bi,
    registro.quantidade_evolucoes
from `rj-smas-dev.relatorio.prontuario_bi_perfil_unidade` as registro
where exists (
    select 1 from `rj-smas-dev.relatorio.prontuario_bi_permissoes` as permissao
    where
        permissao.id_unidade = registro.id_unidade
        and permissao.email = lower(trim(@DS_USER_EMAIL))
)
