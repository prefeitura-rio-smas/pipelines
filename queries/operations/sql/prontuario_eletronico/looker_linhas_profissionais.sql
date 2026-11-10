-- Executar no conector BigQuery do Looker Studio com o parâmetro nativo de e-mail habilitado.
-- Ajustar somente o projeto/dataset para o ambiente publicado.
select
    registro.modulo,
    registro.cas,
    registro.tipo_unidade,
    registro.unidade_atendimento,
    registro.seq_atendimento,
    registro.data_de_atendimento,
    registro.hora_de_atendimento,
    registro.id_usuario,
    registro.id_familia,
    registro.nome_usuario,
    registro.data_nascimento,
    registro.idade,
    registro.idade_faixa,
    registro.raca_cor,
    registro.sexo,
    registro.profissional_id,
    registro.profissional,
    registro.profissional_cbo_original,
    registro.profissional_cbo,
    registro.tipo_atendimento,
    registro.nome_atendimento,
    registro.nome_atendimento_original,
    registro.id_atendimento,
    registro.cadastrante,
    registro.data_cadastro_atendimento,
    registro.id_unidade
from `rj-smas-dev.relatorio.prontuario_bi_linhas_profissionais` as registro
where exists (
    select 1 from `rj-smas-dev.relatorio.prontuario_bi_permissoes` as permissao
    where
        permissao.id_unidade = registro.id_unidade
        and permissao.email = lower(trim(@DS_USER_EMAIL))
)
