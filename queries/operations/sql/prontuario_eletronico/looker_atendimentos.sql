-- Executar no conector BigQuery do Looker Studio com o parâmetro nativo de e-mail habilitado.
-- Ajustar somente o projeto/dataset para o ambiente publicado.
select registro.*
from `rj-smas-dev.relatorio.prontuario_bi_atendimentos` registro
where exists (
    select 1 from `rj-smas-dev.relatorio.prontuario_bi_permissoes` permissao
    where permissao.id_unidade = registro.id_unidade
      and permissao.email = lower(trim(@DS_USER_EMAIL))
)
