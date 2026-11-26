# Ordem dos campos da `STRUCT usuario`

Cada linha da tabela continua a anterior. Os 63 campos pertencem à mesma `STRUCT usuario`.

Ordem implementada no modelo dbt e na projeção do Looker no [PR #163](https://github.com/prefeitura-rio-smas/pipelines/pull/163), commit `0ba5e98`. A rematerialização da tabela publicada está pendente.

| Escopo | Colunas |
|---|---|
| Identificação | `id_usuario` → `nome` → `nome_social` → `filiacao_mae` |
| Nascimento e idade | `data_nascimento` → `idade` → `idade_faixa` → `uf_nascimento` → `cidade_nascimento` |
| Nacionalidade e origem | `nacionalidade` → `pais_origem` → `condicao_estrangeira` |
| Perfil demográfico | `sexo` → `genero` → `orientacao_sexual` → `raca_cor` |
| Documentos | `possui_cpf` → `cpf` → `possui_rg` → `numero_rg` → `flag_rg_ambiguo` → `possui_certidao_nascimento` → `numero_certidao_nascimento` → `flag_certidao_ambigua` |
| Cadastro Único | `flag_cadunico` → `data_cadunico_informada` → `nis` → `flag_nis_ambiguo` |
| Endereço | `bairro` → `logradouro` → `numero` → `complemento` → `referencia_ou_comunidade` |
| Escolaridade | `escolaridade_indice` → `flag_frequenta_escola` → `serie_escolar` |
| Trabalho e renda | `flag_trabalha` → `profissao` → `vinculo_trabalhista` → `tipo_atividade_remunerada` → `atvd_remunerada` → `renda_ativa` |
| Benefícios | `flag_recebe_beneficio` → `beneficio` |
| Decisão apoiada, saúde mental e curatela | `flag_decisao_apoiada` → `numero_processo_decisao_apoiada` → `nome_apoiador` → `flag_saude_mental_comprometida` → `flag_curatela` → `tipo_curatela` |
| Deficiência e dependência | `flag_deficiencia` → `deficiencia` → `grau_dependencia` |
| Acompanhamento e proteção social | `flag_situacao_rua` → `origem_demanda` → `motivo_acolhimento` → `nome_unidade_referencia` → `flag_possui_violacao_direito` → `violacoes` |
| Outros dados do cadastro | `data_atualizacao` → `data_limite_cadastro_atual` → `flag_familia_ambigua` → `pontuacao` |
