# Tabelas, colunas e respostas do Prontuário Eletrônico

O Prontuário Eletrônico de `rj-smas-dev.abordagem` reúne cadastro, famílias, projetos, atendimentos, evoluções, Centro POP, CREAS e acolhimento. O modelo foi construído no grão de **usuário e unidade**. A mesma pessoa pode aparecer em várias unidades e, se estiver sem vínculo, a unidade é publicada vazia.

**Escopo verificado.** Os 705 caminhos do dicionário coincidem com os nomes dos campos e subcampos consultados nos metadados do BigQuery. A tabela registrava 692.858 linhas e a última modificação indicada era de 23/11/2026 às 16:37 (horário de Brasília). A linhagem descreve o código local no commit `94e0e9c`, cujo alias configurado é `planilha_unificada`. O esquema publicado foi conferido. A identidade do SQL da última carga, a unicidade da chave e o preenchimento efetivo das respostas não foram verificados.

O fluxo dbt tem **35 tabelas de origem distintas no BigQuery**, 69 modelos anteriores à saída, inclusive efêmeros, e 4 catálogos em CSV. As tabelas `gh_*` são as relações de origem do Acolherio no BigQuery. O inventário inclui tabelas usadas para obter valores, montar vínculos e aplicar filtros.

## Como rastrear uma resposta

| Campo publicado | Tabela e coluna da resposta | Regra para localizar a resposta |
| --- | --- | --- |
| `usuario.condicao_estrangeira` | `rj-smas.brutos_acolherio_staging.gh_cidadao_pac.condestr` | `seqpac` identifica a pessoa. `raw_usuarios` renomeia o campo, `int_usuarios` aplica o mapa de códigos, e `dim_usuarios` fornece a descrição à saída. Código ausente ou fora do mapa vira texto vazio. |
| `evolucoes.registros.formularios.campos` | `gh_evolupac.dscevopac`, `gh_evolufamil.dscevopac`, `gh_evoluadm.dscevopac` ou `gh_evolugrupo.dscevogrp` | O módulo determina a origem. O HTML em `descricao_evolucao` é interpretado para produzir título, observações, rótulo e valor. A mart publica `campos` como array de textos `rótulo: valor`. |
| `centro_pop.historico_mensal.evolucoes.formularios.campos.label` e `.valor` | Mesmas quatro colunas HTML de evolução | O histórico mensal preserva o par estruturado em campos distintos. |
| `creas.*` e `creas.registros.*` para respostas de formulário | Mesmas quatro colunas HTML de evolução, conforme o módulo | `creas_campos.csv` cruza título e rótulo normalizado para atribuir o valor à coluna. A inclusão também depende do universo filtrado pelo CREAS. O catálogo completo consta no apêndice. |
| `centro_pop.motivo_principal_permanencia_rua` e projeções derivadas do perfil e histórico | `gh_regeatv_evol_quest_list.dscresp` | Junta com `gh_regeatv_evol_quest` por `seqevol`, `seqtemplate` e `seqmodulo` com tratamento de nulos. O cabeçalho vincula `seqprontuario` ao usuário. Filtra `seqtemplate = 3`, `seqquest = 18` e resposta preenchida. |
| `centro_pop.encaminhamentos` | Colunas HTML de evolução, pelos rótulos que começam com `Encaminhamentos` | Usa Atendimento Social e PAI. Consolida os valores distintos por usuário, unidade, assunto e rótulo. |
| `centro_pop.procedimentos` | Colunas HTML de evolução, pelos rótulos `Demanda inicial` e `Identificação das demandas apresentadas pelo usuário` | Consolida demandas por usuário e unidade, com deduplicação dentro de cada assunto. |

No mapa de `condestr`, os códigos de 1 a 8 geram, em ordem, Apatrida, Asilado, Imigrante com autorização de residência, Imigrante com visto temporário, Refugiado (Não legalizado), Refugiado (Em solicitação), Refugiado Legalizado e Turista.

Quando a data do formulário está mapeada e é válida, ela fornece `data_ocorrencia`. Nos demais casos o fluxo usa a data da evolução e indica a situação em `situacao_data_ocorrencia`.

[SQL da mart](../models/marts/prontuario_eletronico/prontuario_eletronico.sql) · [Extração de formulários](../macros/dominios/acolherio/formularios.sql) · [Regras do Centro POP](../macros/dominios/centro_pop/escopo.sql) · [Regras do CREAS](../macros/dominios/creas/pu_perfil_creas_sql.sql)

### Rótulos de formulário do Centro POP

Os nomes abaixo são os campos de resposta de `int_registros_centro_pop`. As projeções por unidade e por mês estão no dicionário. O caminho exato muda conforme a estrutura publicada.

| Formulário | Rótulo de origem | Campo extraído |
| --- | --- | --- |
| Plano de Atendimento Individual (PAI) | Data do atendimento | `data_inclusao_pai`, `data_inclusao_informada` |
| Atendimento Social | Motivo secundário da ida às ruas | `motivo_secundario_permanencia_rua` |
| Atendimento Social | Possui referências familiares? | `possui_referencias_familiares` |
| Atendimento Social | Território da referência familiar | `territorio_referencia_familia` |
| Atendimento Social | Existe possibilidade de reinserção familiar? | `possibilidade_reinsercao_familiar` |
| Situação de Saúde | Faz uso de substâncias psicoativas? | `uso_substancias_psicoativas` |
| Situação de Saúde | Situação | `situacao_saude`, `possui_problema_saude` |
| Situação de Saúde | Local onde faz tratamento | `local_tratamento`, `realiza_acompanhamento_saude` |
| Desligamento PAI | Data do desligamento | `data_desligamento`, `data_desligamento_informada` |
| Desligamento PAI | Motivo do desligamento | `motivo_desligamento` |
| Desligamento PAI | Outros | `motivo_desligamento_outros` |
| Atendimento Social e PAI | Demanda inicial ou Identificação das demandas apresentadas pelo usuário | `demandas` |
| Atendimento Social e PAI | Rótulos iniciados por Encaminhamentos | `encaminhamentos` |

[Mapeamento e filtros do Centro POP](../macros/dominios/centro_pop/escopo.sql#L53)

## Inventário das tabelas de origem

Cada tabela abaixo aponta para a primeira leitura no dbt. O apêndice de projeções lista os nomes das colunas físicas e os aliases usados no SQL. As colunas selecionadas na leitura inicial podem atender várias saídas ou servir apenas para joins, filtros e controle de carga.

| Tabela física | Leitores diretos no dbt |
| --- | --- |
| `rj-smas-dev.dashboard_acolherio.dev_atendimentos` | [raw_atendimentos_programada](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_programada.sql) |
| `rj-smas-dev.dashboard_acolherio.filtro_email_dev` | [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql), [int_filtro_email_unidades](../models/intermediate/seguranca/int_filtro_email_unidades.sql), [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) |
| `rj-smas.brutos_acolherio_staging.gh_atend_familia` | [int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql), [raw_atendimentos_familias](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_familias.sql) |
| `rj-smas.brutos_acolherio_staging.gh_atendimentos` | [int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql), [raw_atendimentos_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_usuarios.sql) |
| `rj-smas.brutos_acolherio_staging.gh_ativ_paciente` | [int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql), [raw_presencas_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_presencas_usuarios.sql) |
| `rj-smas.brutos_acolherio_staging.gh_ativgrp` | [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) |
| `rj-smas.brutos_acolherio_staging.gh_cbo` | [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql) |
| `rj-smas.brutos_acolherio_staging.gh_cidadao_pac` | [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql), [raw_usuarios_enderecos](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_enderecos.sql) |
| `rj-smas.brutos_acolherio_staging.gh_contas` | [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql) |
| `rj-smas.brutos_acolherio_staging.gh_contas_modulos` | [raw_contas_modulos](../models/raw/prontuario_carioca_assistencia_social/raw_contas_modulos.sql) |
| `rj-smas.brutos_acolherio_staging.gh_contas_us` | [raw_operadores_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_operadores_unidades.sql) |
| `rj-smas.brutos_acolherio_staging.gh_evoluadm` | [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql) |
| `rj-smas.brutos_acolherio_staging.gh_evolufamil` | [int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql), [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql) |
| `rj-smas.brutos_acolherio_staging.gh_evolugrupo` | [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql) |
| `rj-smas.brutos_acolherio_staging.gh_evolupac` | [int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql), [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql) |
| `rj-smas.brutos_acolherio_staging.gh_famil_projsociais` | [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) |
| `rj-smas.brutos_acolherio_staging.gh_famil_servassist` | [raw_familias_servicos_assistenciais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_servicos_assistenciais.sql) |
| `rj-smas.brutos_acolherio_staging.gh_famil_vulnerab` | [raw_familias_vulnerabilidades](../models/raw/prontuario_carioca_assistencia_social/raw_familias_vulnerabilidades.sql) |
| `rj-smas.brutos_acolherio_staging.gh_familias` | [raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql) |
| `rj-smas.brutos_acolherio_staging.gh_familias_membros` | [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql) |
| `rj-smas.brutos_acolherio_staging.gh_familias_motiv_saida` | [raw_familias_motivos_saida](../models/raw/prontuario_carioca_assistencia_social/raw_familias_motivos_saida.sql) |
| `rj-smas.brutos_acolherio_staging.gh_origens` | [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql) |
| `rj-smas.brutos_acolherio_staging.gh_pac_ciclos` | [raw_usuarios_acolhimentos](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_acolhimentos.sql) |
| `rj-smas.brutos_acolherio_staging.gh_pac_dados` | [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) |
| `rj-smas.brutos_acolherio_staging.gh_pac_info_med` | [raw_usuarios_informacoes_medicas](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_informacoes_medicas.sql) |
| `rj-smas.brutos_acolherio_staging.gh_pacientes_sm` | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| `rj-smas.brutos_acolherio_staging.gh_perfil_grupos` | [raw_perfis_grupos](../models/raw/prontuario_carioca_assistencia_social/raw_perfis_grupos.sql) |
| `rj-smas.brutos_acolherio_staging.gh_prof` | [raw_profissionais](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais.sql) |
| `rj-smas.brutos_acolherio_staging.gh_profocup` | [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql) |
| `rj-smas.brutos_acolherio_staging.gh_regeatv_evol_quest` | [raw_evolucoes_questionario](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_questionario.sql) |
| `rj-smas.brutos_acolherio_staging.gh_regeatv_evol_quest_list` | [raw_evolucoes_questionario_lista](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_questionario_lista.sql) |
| `rj-smas.brutos_acolherio_staging.gh_us` | [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) |
| `rj-smas.brutos_acolherio_staging.gh_us_config` | [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql) |
| `rj-smas.brutos_acolherio_staging.gh_us_smas` | [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) |
| `rj-smas.brutos_acolherio_staging.gh_us_tipo` | [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql) |

## Regras de integração

- **População e chave.** A base é `dim_usuarios`. A mart forma os vínculos de unidade a partir de atendimentos, evoluções, projetos, registros e histórico mensal do Centro POP, histórico e participações CREAS, e ciclos de acolhimento. As origens dos vínculos são combinadas por `UNION DISTINCT`. Usuários sem vínculo continuam na base com unidade vazia.
- **Cadastro e famílias.** Atributos cadastrais e famílias acompanham o usuário nas suas unidades. A unidade de referência é um atributo cadastral. RG e NIS vêm de `raw_usuarios`, matrícula da certidão vem de `raw_usuarios_detalhes`. Para endereço, `int_usuarios_enderecos` usa a captura da programada quando há registro e usa o cadastro na ausência dessa captura.
- **Atendimentos.** A programada é `rj-smas-dev.dashboard_acolherio.dev_atendimentos`. O intermediate monta eventos, exclui cancelados e reúne profissionais. A mart expande os profissionais mantendo uma participação por registro, inclusive com ID profissional vazio. Totais e contadores por serviço nesse bloco contam participações. Para contar eventos por usuário e unidade, deduplicar pelo ID de evento preenchido na chave da linha.
- **Evoluções.** `int_evolucoes_por_usuario` organiza eventos do cidadão e exclui cancelados. A mart associa formulários por usuário e ID de evolução com módulo. O catálogo `formularios_datas_ocorrencia` mapeia as datas de ocorrência dos formulários.
- **Centro POP.** Os perfis por unidade partem de `int_registros_centro_pop` e do universo de atendimentos. A mart publica os indicadores médicos da raiz com dados cadastrais quando existe perfil. O histórico mensal recalcula o estado no limite de cada competência e preserva os eventos daquela competência. Os contadores históricos de atendimento partem do intermediate no grão de evento.
- **CREAS.** `int_perfil_creas_por_unidade` recorta histórico e participações pela unidade e monta os perfis com `pu_perfil_creas_sql`. O catálogo `creas_campos` liga títulos e rótulos a colunas. A chave de unidade dos registros vem do perfil por unidade.
- **Alta complexidade.** A mart usa o histórico de `alta_complexidade`, ordena ciclos da unidade por entrada e ID, e elege o ciclo da raiz priorizando ciclos abertos, depois entrada e ID mais recentes.
- **Acesso.** `int_filtro_email_unidades` combina os destinatários da CAS, da unidade e da planilha de permissões. A coluna `email_filtro` é a lista da unidade publicada. A consulta segura compara a identidade informada com endereços completos da lista. O acesso direto à tabela segue o IAM do BigQuery.

## Dicionário de campos

Cada caminho representa uma coluna ou um subcampo publicado. As tabelas registram o tipo do BigQuery, a origem no fluxo dbt e a expressão de saída. Para os campos derivados de formulários, o título e o rótulo determinam qual resposta alimenta cada projeção.


### id_usuario_unidade

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `id_usuario_unidade` | `STRING` | dim_usuarios.id_usuario + unidades_por_usuario.id_unidade | Concatena os IDs com `:`. Unidade ausente gera sufixo vazio. |

### id_usuario

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `id_usuario` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `id_usuario` | `coalesce(CAST(p.id_usuario AS STRING), '')` |

### id_unidade

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `id_unidade` | `STRING` | `unidades_por_usuario` · `id_unidade` | `coalesce(CAST(vinculo.id_unidade AS STRING), '')` |

### nome_unidade

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `nome_unidade` | `STRING` | [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) · `nome_unidade` | `coalesce(unidade.nome_unidade, '')` |

### email_filtro

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `email_filtro` | `STRING` | [int_filtro_email_unidades](../models/intermediate/seguranca/int_filtro_email_unidades.sql) · `email_filtro` | `coalesce(permissoes.email_filtro, '')` |

### usuario

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `usuario` | `STRUCT` | Projeção da mart | Struct com os campos detalhados abaixo. |
| `usuario.id_usuario` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `id_usuario` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `id_paciente` | `coalesce(CAST(p.id_usuario AS STRING), '')` |
| `usuario.nome` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `nome` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `nome` | `coalesce(CAST(p.nome AS STRING), '')` |
| `usuario.nome_social` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `nome_social` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `nome_social` | `coalesce(CAST(p.nome_social AS STRING), '')` |
| `usuario.filiacao_mae` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `filiacao_mae` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `nome_mae` | `coalesce(CAST(p.filiacao_mae AS STRING), '')` |
| `usuario.nacionalidade` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `nacionalidade` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `nacionalidade` | `coalesce(CAST(p.nacionalidade AS STRING), '')` |
| `usuario.condicao_estrangeira` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `condicao_estrangeira` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `condicao_estrangeira` ← `gh_cidadao_pac.condestr` | Mapeia os códigos com `map_coluna_cond_estrangeiro` e usa texto vazio se o resultado é nulo. |
| `usuario.pais_origem` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `pais_origem` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `pais_origem` | `coalesce(CAST(p.pais_origem AS STRING), '')` |
| `usuario.cpf` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `cpf` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `cpf` | `coalesce(CAST(p.cpf AS STRING), '')` |
| `usuario.data_nascimento` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `data_nascimento` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `data_nascimento` | `coalesce(CAST(p.data_nascimento AS STRING), '')` |
| `usuario.sexo` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `sexo` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `sexo` | `coalesce(CAST(p.sexo AS STRING), '')` |
| `usuario.raca_cor` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `raca_cor` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `raca_cor` | `coalesce(CAST(p.raca_cor AS STRING), 'NÃO INFORMADO')` |
| `usuario.genero` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `genero` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `genero` | `coalesce(CAST(p.genero AS STRING), '')` |
| `usuario.bairro` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `bairro` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `bairro` | `coalesce(CAST(p.bairro AS STRING), 'NÃO INFORMADO')` |
| `usuario.prontuario` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `prontuario` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `prontuario_papel` | `coalesce(CAST(p.prontuario AS STRING), '')` |
| `usuario.flag_cadunico` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_cadunico` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `flag_cadunico` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_cadunico AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_cadunico AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_cadunico AS STRING), '') AS STRING) END` |
| `usuario.pontuacao` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `pontuacao` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `pontuacao` | `coalesce(CAST(p.pontuacao AS STRING), '')` |
| `usuario.grau_dependencia` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `grau_dependencia` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `grau_dependencia` | `coalesce(CAST(p.grau_dependencia AS STRING), '')` |
| `usuario.nome_apoiador` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `nome_apoiador` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `nome_apoiador` | `coalesce(CAST(p.nome_apoiador AS STRING), '')` |
| `usuario.numero_processo_decisao_apoiada` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `numero_processo_decisao_apoiada` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `numero_processo_decisao_apoiada` | `coalesce(CAST(p.numero_processo_decisao_apoiada AS STRING), '')` |
| `usuario.renda_ativa` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `renda_ativa` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `renda_ativa` | `coalesce(CAST(p.renda_ativa AS STRING), '')` |
| `usuario.atvd_remunerada` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `atvd_remunerada` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.atvd_remunerada AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.atvd_remunerada AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.atvd_remunerada AS STRING), '') AS STRING) END` |
| `usuario.flag_decisao_apoiada` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_decisao_apoiada` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `numero_processo_decisao_apoiada` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_decisao_apoiada AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_decisao_apoiada AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_decisao_apoiada AS STRING), '') AS STRING) END` |
| `usuario.flag_saude_mental_comprometida` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_saude_mental_comprometida` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `flag_saude_mental_comprometida` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_saude_mental_comprometida AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_saude_mental_comprometida AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_saude_mental_comprometida AS STRING), '') AS STRING) END` |
| `usuario.motivo_acolhimento` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `motivo_acolhimento` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `id_motivo_acolhimento` | `coalesce(CAST(p.motivo_acolhimento AS STRING), '')` |
| `usuario.orientacao_sexual` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `orientacao_sexual` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `orientacao_sexual` | `coalesce(CAST(p.orientacao_sexual AS STRING), '')` |
| `usuario.vinculo_trabalhista` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `vinculo_trabalhista` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `vinculo_trabalhista` | `coalesce(CAST(p.vinculo_trabalhista AS STRING), '')` |
| `usuario.flag_trabalha` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_trabalha` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `flag_trabalha` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_trabalha AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_trabalha AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_trabalha AS STRING), '') AS STRING) END` |
| `usuario.profissao` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `profissao` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `profissao` | `coalesce(CAST(p.profissao AS STRING), '')` |
| `usuario.flag_frequenta_escola` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_frequenta_escola` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `flag_frequenta_escola` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_frequenta_escola AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_frequenta_escola AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_frequenta_escola AS STRING), '') AS STRING) END` |
| `usuario.escolaridade_indice` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `escolaridade_indice` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `escolaridade` | `coalesce(CAST(p.escolaridade_indice AS STRING), '')` |
| `usuario.flag_recebe_beneficio` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_recebe_beneficio` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `flag_recebe_beneficio` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_recebe_beneficio AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_recebe_beneficio AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_recebe_beneficio AS STRING), '') AS STRING) END` |
| `usuario.flag_curatela` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_curatela` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `flag_curatela` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_curatela AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_curatela AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_curatela AS STRING), '') AS STRING) END` |
| `usuario.tipo_curatela` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `tipo_curatela` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `tipo_curatela` | `coalesce(CAST(p.tipo_curatela AS STRING), '')` |
| `usuario.flag_deficiencia` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_deficiencia` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `flag_deficiencia` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_deficiencia AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_deficiencia AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_deficiencia AS STRING), '') AS STRING) END` |
| `usuario.flag_situacao_rua` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_situacao_rua` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `flag_situacao_rua` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_situacao_rua AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_situacao_rua AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_situacao_rua AS STRING), '') AS STRING) END` |
| `usuario.origem_demanda` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `origem_demanda` ← [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql) · `descricao_origem` | `coalesce(CAST(p.origem_demanda AS STRING), '')` |
| `usuario.flag_possui_violacao_direito` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `flag_possui_violacao_direito` ← [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql) · `id_usuario` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_possui_violacao_direito AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(p.flag_possui_violacao_direito AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(p.flag_possui_violacao_direito AS STRING), '') AS STRING) END` |
| `usuario.nome_unidade_referencia` | `STRING` | [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) · `nome_unidade` | `coalesce(unidade_referencia.nome_unidade, '')` |
| `usuario.uf_nascimento` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `uf_nascimento` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `uf_nascimento` | `coalesce(CAST(p.uf_nascimento AS STRING), '')` |
| `usuario.cidade_nascimento` | `STRING` | Cadastro de usuários · `muninasc` → `raw_usuarios.cidade_nascimento` → `dim_usuarios` | Converte para STRING, remove espaços nas extremidades e usa texto vazio em caso de ausência. |
| `usuario.serie_escolar` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `serie_escolar` ← [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) · `serie_escolar` | `coalesce(CAST(p.serie_escolar AS STRING), '')` |
| `usuario.data_cadunico_informada` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `data_cadunico_informada` ← [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `data_cadunico` | `coalesce(CAST(p.data_cadunico_informada AS STRING), '')` |
| `usuario.nis` | `STRING` | [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `nis_cadastro` | `coalesce(CAST(cadastro.nis_cadastro AS STRING), '')` |
| `usuario.numero_rg` | `STRING` | [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `numero_rg_cadastro` | `coalesce(CAST(cadastro.numero_rg_cadastro AS STRING), '')` |
| `usuario.possui_rg` | `STRING` | [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `numero_rg_cadastro` | `IF(NOT nullif(TRIM(CAST(cadastro.numero_rg_cadastro AS STRING)), '') IS NULL, 'Sim', 'Não')` |
| `usuario.possui_certidao_nascimento` | `STRING` | [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `matricula_certidao_nascimento` | `IF(NOT nullif(TRIM(CAST(detalhes_cadastro.matricula_certidao_nascimento AS STRING)), '') IS NULL, 'Sim', 'Não')` |
| `usuario.numero_certidao_nascimento` | `STRING` | [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) · `matricula_certidao_nascimento` | `coalesce(CAST(detalhes_cadastro.matricula_certidao_nascimento AS STRING), '')` |
| `usuario.data_atualizacao` | `STRING` | Constante da mart | `CAST('' AS STRING)` |
| `usuario.data_limite_cadastro_atual` | `STRING` | Constante da mart | `CAST('' AS STRING)` |
| `usuario.flag_familia_ambigua` | `STRING` | Constante da mart | `CAST('' AS STRING)` |
| `usuario.flag_nis_ambiguo` | `STRING` | Constante da mart | `CAST('' AS STRING)` |
| `usuario.flag_rg_ambiguo` | `STRING` | Constante da mart | `CAST('' AS STRING)` |
| `usuario.flag_certidao_ambigua` | `STRING` | Constante da mart | `CAST('' AS STRING)` |
| `usuario.beneficio` | `ARRAY<STRUCT>` | Projeção da mart ← [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql) · `beneficio` | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `usuario.beneficio.descricao` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `beneficio.descricao` | `coalesce(CAST(item1.descricao AS STRING), '')` |
| `usuario.deficiencia` | `ARRAY<STRUCT>` | Projeção da mart ← [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql) · `deficiencia` | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `usuario.deficiencia.descricao` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `deficiencia.descricao` | `coalesce(CAST(item1.descricao AS STRING), '')` |
| `usuario.violacoes` | `ARRAY<STRUCT>` | Projeção da mart ← [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql) · `violacoes` | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `usuario.violacoes.descricao` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `violacoes.descricao` | `coalesce(CAST(item1.descricao AS STRING), '')` |
| `usuario.violacoes.origem` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `violacoes.origem` | `coalesce(CAST(item1.origem AS STRING), '')` |
| `usuario.logradouro` | `STRING` | [int_usuarios_enderecos](../models/intermediate/core/int_usuarios_enderecos.sql) · `endereco.logradouro` | `coalesce(end_bi.endereco.logradouro, '')` |
| `usuario.numero` | `STRING` | [int_usuarios_enderecos](../models/intermediate/core/int_usuarios_enderecos.sql) · `endereco.numero` | `coalesce(end_bi.endereco.numero, '')` |
| `usuario.complemento` | `STRING` | [int_usuarios_enderecos](../models/intermediate/core/int_usuarios_enderecos.sql) · `endereco.complemento` | `coalesce(end_bi.endereco.complemento, '')` |
| `usuario.referencia_ou_comunidade` | `STRING` | [int_usuarios_enderecos](../models/intermediate/core/int_usuarios_enderecos.sql) · `endereco.referencia_ou_comunidade` | `coalesce(end_bi.endereco.referencia_ou_comunidade, '')` |
| `usuario.idade` | `INTEGER` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `data_nascimento` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `data_nascimento` | `CASE WHEN SAFE_CAST(p.data_nascimento AS DATE) <= CURRENT_DATE('America/Sao_Paulo') THEN DATE_DIFF(date(CURRENT_DATE('America/Sao_Paulo')), date(SAFE_CAST(p.data_nascimento AS DATE)), YEAR) - CASE WHEN EXTRACT(MONTH FROM date(CURRENT_DATE('America/Sao_Paulo'))) < EXTRACT(MONTH FROM date(SAFE_CAST(p.data_nascimento AS DATE))) THEN 1 WHEN EXTRACT(MONTH FROM date(CURRENT_DATE('America/Sao_Paulo'))) = EXTRACT(MONTH FROM date(SAFE_CAST(p.data_nascimento AS DATE))) AND EXTRACT(DAY FROM date(CURRENT_DATE('America/Sao_Paulo'))) < EXTRACT(DAY FROM date(SAFE_CAST(p.data_nascimento AS DATE))) THEN 1 ELSE 0 END END` |
| `usuario.tipo_atividade_remunerada` | `STRING` | int_usuarios · CTE `atividades_trabalho` | Derivada do cadastro de detalhes e do catálogo de atividades de trabalho. |
| `usuario.possui_cpf` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `cpf` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `cpf` | `IF(NOT nullif(TRIM(CAST(p.cpf AS STRING)), '') IS NULL, 'Sim', 'Não')` |
| `usuario.idade_faixa` | `STRING` | [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) · `data_nascimento` ← [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) · `data_nascimento` | `Aplica a macro idade_faixa à idade calculada na data corrente em America/Sao_Paulo. Nascimento ausente ou futuro segue a faixa de ausência.` |

### familias

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `familias` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `familias.id_familia` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.id_familia` | `coalesce(CAST(item1.id_familia AS STRING), '')` |
| `familias.nome_responsavel` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.nome_responsavel` | `coalesce(CAST(item1.nome_responsavel AS STRING), '')` |
| `familias.flag_responsavel_atual` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.flag_responsavel_atual` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(item1.flag_responsavel_atual AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(item1.flag_responsavel_atual AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(item1.flag_responsavel_atual AS STRING), '') AS STRING) END` |
| `familias.flag_ativo` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.flag_ativo` | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(item1.flag_ativo AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(item1.flag_ativo AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(item1.flag_ativo AS STRING), '') AS STRING) END` |
| `familias.data_ultima_modificacao` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.data_ultima_modificacao` | `coalesce(CAST(item1.data_ultima_modificacao AS STRING), '')` |
| `familias.membros` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `familias.membros.id_usuario` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.membros.id_usuario` | `coalesce(CAST(item2.id_usuario AS STRING), '')` |
| `familias.membros.nome` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.membros.nome` | `coalesce(CAST(item2.nome AS STRING), '')` |
| `familias.membros.parentesco_responsavel` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.membros.parentesco_responsavel` | `coalesce(CAST(item2.parentesco_responsavel AS STRING), '')` |
| `familias.vinculos` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `familias.vinculos.data_entrada` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.vinculos.data_entrada` | `coalesce(CAST(item2.data_entrada AS STRING), '')` |
| `familias.vinculos.data_saida` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.vinculos.data_saida` | `coalesce(CAST(item2.data_saida AS STRING), '')` |
| `familias.vinculos.parentesco_responsavel` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.vinculos.parentesco_responsavel` | `coalesce(CAST(item2.parentesco_responsavel AS STRING), '')` |
| `familias.vinculos.motivo_saida` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.vinculos.motivo_saida` | `coalesce(CAST(item2.motivo_saida AS STRING), '')` |
| `familias.servicos` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `familias.servicos.nome_unidade` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.servicos.nome_unidade` | `coalesce(item2.nome_unidade, '')` |
| `familias.servicos.data_cadastro` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.servicos.data_cadastro` | `coalesce(CAST(item2.data_cadastro AS STRING), '')` |
| `familias.servicos.data_cancelamento` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.servicos.data_cancelamento` | `coalesce(CAST(item2.data_cancelamento AS STRING), '')` |
| `familias.servicos.nome_servico` | `STRING` | [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) · `familias.servicos.nome_servico` | `coalesce(CAST(item2.nome_servico AS STRING), '')` |

### projetos_sociais

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `projetos_sociais` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `projetos_sociais.nome_projeto_social` | `STRING` | `projetos_por_usuario` · `projetos_sociais.nome_projeto_social` ← [int_projetos_sociais_por_unidade](../models/intermediate/social/int_projetos_sociais_por_unidade.sql) | `coalesce(CAST(item1.nome_projeto_social AS STRING), '')` |
| `projetos_sociais.id_projeto_social` | `INTEGER` | `projetos_por_usuario` · `projetos_sociais.id_projeto_social` ← [int_projetos_sociais_por_unidade](../models/intermediate/social/int_projetos_sociais_por_unidade.sql) | `item1.id_projeto_social` |
| `projetos_sociais.id_unidade` | `INTEGER` | `projetos_por_usuario` · `projetos_sociais.id_unidade` ← [int_projetos_sociais_por_unidade](../models/intermediate/social/int_projetos_sociais_por_unidade.sql) | `item1.id_unidade` |
| `projetos_sociais.nome_unidade` | `STRING` | `projetos_por_usuario` · `projetos_sociais.nome_unidade` ← [int_projetos_sociais_por_unidade](../models/intermediate/social/int_projetos_sociais_por_unidade.sql) | `coalesce(item1.nome_unidade, '')` |
| `projetos_sociais.indicador_ativo` | `STRING` | `projetos_por_usuario` · `projetos_sociais.indicador_ativo` ← [int_projetos_sociais_por_unidade](../models/intermediate/social/int_projetos_sociais_por_unidade.sql) | `CASE WHEN lower(TRIM(CAST(coalesce(CAST(item1.indicador_ativo AS STRING), '') AS STRING))) IN ('true', 's', 'sim') THEN 'Sim' WHEN lower(TRIM(CAST(coalesce(CAST(item1.indicador_ativo AS STRING), '') AS STRING))) IN ('false', 'n', 'não', 'nao') THEN 'Não' ELSE CAST(coalesce(CAST(item1.indicador_ativo AS STRING), '') AS STRING) END` |
| `projetos_sociais.data_cadastro` | `STRING` | `projetos_por_usuario` · `projetos_sociais.data_cadastro` ← [int_projetos_sociais_por_unidade](../models/intermediate/social/int_projetos_sociais_por_unidade.sql) | `coalesce(CAST(item1.data_cadastro AS STRING), '')` |
| `projetos_sociais.data_cancelamento` | `STRING` | `projetos_por_usuario` · `projetos_sociais.data_cancelamento` ← [int_projetos_sociais_por_unidade](../models/intermediate/social/int_projetos_sociais_por_unidade.sql) | `coalesce(CAST(item1.data_cancelamento AS STRING), '')` |

### atendimentos

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `atendimentos` | `STRUCT` | Projeção da mart | Struct com os campos detalhados abaixo. |
| `atendimentos.quantidade` | `INTEGER` | `atendimentos_por_usuario` · `atendimentos` | `ARRAY_LENGTH(coalesce(a.atendimentos, []))` |
| `atendimentos.quantidade_cras` | `INTEGER` | Array de entrada do contador · `tipo_servico`<br>`atendimentos_por_usuario` · `atendimentos` | `(SELECT count(*) FROM UNNEST(coalesce(a.atendimentos, [])) AS evento WHERE evento.tipo_servico = 'cras')` |
| `atendimentos.quantidade_creas` | `INTEGER` | Array de entrada do contador · `tipo_servico`<br>`atendimentos_por_usuario` · `atendimentos` | `(SELECT count(*) FROM UNNEST(coalesce(a.atendimentos, [])) AS evento WHERE evento.tipo_servico = 'creas')` |
| `atendimentos.quantidade_centro_pop` | `INTEGER` | Array de entrada do contador · `tipo_servico`<br>`atendimentos_por_usuario` · `atendimentos` | `(SELECT count(*) FROM UNNEST(coalesce(a.atendimentos, [])) AS evento WHERE evento.tipo_servico = 'centro_pop')` |
| `atendimentos.quantidade_outros` | `INTEGER` | Array de entrada do contador · `tipo_servico`<br>`atendimentos_por_usuario` · `atendimentos` | `(SELECT count(*) FROM UNNEST(coalesce(a.atendimentos, [])) AS evento WHERE evento.tipo_servico = 'outros')` |
| `atendimentos.registros` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `atendimentos.registros.id_atendimento_modulo` | `STRING` | Programada · `seq_atendimento` + `modulo` | Concatenação preservada nas participações do evento. |
| `atendimentos.registros.id_familia` | `STRING` | `atendimentos_por_usuario` · `atendimentos.id_familia` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.id_familia, '')` |
| `atendimentos.registros.origem_modulo` | `STRING` | `atendimentos_por_usuario` · `atendimentos.origem_modulo` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(CAST(item1.origem_modulo AS STRING), '')` |
| `atendimentos.registros.id_unidade` | `STRING` | `atendimentos_por_usuario` · `atendimentos.id_unidade` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(CAST(item1.id_unidade AS STRING), '')` |
| `atendimentos.registros.nome_unidade` | `STRING` | `atendimentos_por_usuario` · `atendimentos.nome_unidade` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.nome_unidade, '')` |
| `atendimentos.registros.tipo_atendimento_descricao` | `STRING` | `atendimentos_por_usuario` · `atendimentos.tipo_atendimento_descricao` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(CAST(item1.tipo_atendimento_descricao AS STRING), '')` |
| `atendimentos.registros.data_atendimento_informada` | `STRING` | int_datas_atendimento · `data_atendimento` | Data de realização associada por módulo, evento, usuário e unidade. Publicada como STRING. |
| `atendimentos.registros.hora_atendimento` | `STRING` | `atendimentos_por_usuario` · `atendimentos.hora_atendimento` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(FORMAT_TIME('%H:%M', CASE WHEN REGEXP_CONTAINS(TRIM(CAST(item1.hora_atendimento AS STRING)), '^\\d{4}$') THEN SAFE.PARSE_TIME('%H%M', TRIM(CAST(item1.hora_atendimento AS STRING))) WHEN REGEXP_CONTAINS(TRIM(CAST(item1.hora_atendimento AS STRING)), '^\\d{1,2}:\\d{2}$') THEN SAFE.PARSE_TIME('%H:%M', TRIM(CAST(item1.hora_atendimento AS STRING))) END), '')` |
| `atendimentos.registros.profissionais` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `atendimentos.registros.profissionais.nome_profissional` | `STRING` | `atendimentos_por_usuario` · `atendimentos.profissionais.nome_profissional` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(CAST(item2.nome_profissional AS STRING), '')` |
| `atendimentos.registros.profissionais.descricoes_cbo` | `ARRAY<STRING>` | `atendimentos_por_usuario` · `atendimentos.profissionais.descricoes_cbo` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `ARRAY(SELECT coalesce(item3, '') FROM UNNEST(item2.descricoes_cbo) AS item3 WITH OFFSET AS posicao ORDER BY posicao)` |
| `atendimentos.registros.profissionais.cbo_principal_descricao` | `STRING` | `atendimentos_por_usuario` · `atendimentos.profissionais.cbo_principal_descricao` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(CAST(item2.cbo_principal_descricao AS STRING), '')` |
| `atendimentos.registros.profissionais.id_profissional` | `STRING` | Programada · `profissional_id` | Converte para STRING e representa a ausência com texto vazio. A participação continua no total. |
| `atendimentos.registros.profissionais.profissional_cbo` | `STRING` | `atendimentos_por_usuario` · `atendimentos.profissionais.profissional_cbo` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item2.profissional_cbo, '')` |
| `atendimentos.registros.profissionais.classificacao_atendimento` | `STRING` | Programada · `tipo_atendimento` | Categoria original da linha profissional. |
| `atendimentos.registros.profissionais.nome_atendimento` | `STRING` | `atendimentos_por_usuario` · `atendimentos.profissionais.nome_atendimento` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item2.nome_atendimento, '')` |
| `atendimentos.registros.profissionais.id_familia_referencia` | `STRING` | Programada · `id_familia` | Família da linha profissional. Convertida para STRING. |
| `atendimentos.registros.profissionais.id_atendimento_legado` | `STRING` | Programada · `id_atendimento` | Identificador legado da linha profissional. O ID do evento com módulo é publicado separadamente. |
| `atendimentos.registros.tipo_servico` | `STRING` | `atendimentos_por_usuario` · `atendimentos.tipo_servico` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.tipo_servico, '')` |
| `atendimentos.registros.classificacao_atendimento` | `STRING` | Programada · `tipo_atendimento` da linha profissional | A mart copia a categoria do único profissional da participação. |
| `atendimentos.registros.projetos_sociais_familia` | `STRING` | `atendimentos_por_usuario` · `atendimentos.projetos_sociais_familia` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.projetos_sociais_familia, '')` |
| `atendimentos.registros.cas` | `STRING` | `atendimentos_por_usuario` · `atendimentos.cas` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.cas, '')` |
| `atendimentos.registros.email_unidade` | `STRING` | `atendimentos_por_usuario` · `atendimentos.email_unidade` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.email_unidade, '')` |
| `atendimentos.registros.email_cas` | `STRING` | `atendimentos_por_usuario` · `atendimentos.email_cas` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.email_cas, '')` |
| `atendimentos.registros.cadastrante` | `STRING` | `atendimentos_por_usuario` · `atendimentos.cadastrante` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.cadastrante, '')` |
| `atendimentos.registros.data_atendimento_sistema` | `STRING` | Programada · `data_cadastro_atendimento` | Data de cadastro no sistema, convertida para STRING. |
| `atendimentos.registros.tipo_unidade` | `STRING` | `atendimentos_por_usuario` · `atendimentos.tipo_unidade` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `coalesce(item1.tipo_unidade, '')` |
| `atendimentos.registros.elegivel_indicador` | `STRING` | `atendimentos_por_usuario` · `atendimentos.elegivel_indicador` ← [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `item1.elegivel_indicador` |
| `atendimentos.registros.atendimento_compartilhado` | `STRING` | int_atendimentos_por_usuario · `atendimento_compartilhado` | Sim quando o evento tem dois ou mais IDs profissionais distintos e preenchidos. A mesma flag é copiada para todas as participações. |

### evolucoes

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `evolucoes` | `STRUCT` | Projeção da mart | Struct com os campos detalhados abaixo. |
| `evolucoes.quantidade` | `INTEGER` | `registros_evolucoes` · `evolucoes` | `ARRAY_LENGTH(coalesce(e.evolucoes, []))` |
| `evolucoes.quantidade_cras` | `INTEGER` | Array de entrada do contador · `tipo_servico`<br>`registros_evolucoes` · `evolucoes` | `(SELECT count(*) FROM UNNEST(coalesce(e.evolucoes, [])) AS evento WHERE evento.tipo_servico = 'cras')` |
| `evolucoes.quantidade_creas` | `INTEGER` | Array de entrada do contador · `tipo_servico`<br>`registros_evolucoes` · `evolucoes` | `(SELECT count(*) FROM UNNEST(coalesce(e.evolucoes, [])) AS evento WHERE evento.tipo_servico = 'creas')` |
| `evolucoes.quantidade_centro_pop` | `INTEGER` | Array de entrada do contador · `tipo_servico`<br>`registros_evolucoes` · `evolucoes` | `(SELECT count(*) FROM UNNEST(coalesce(e.evolucoes, [])) AS evento WHERE evento.tipo_servico = 'centro_pop')` |
| `evolucoes.quantidade_outros` | `INTEGER` | Array de entrada do contador · `tipo_servico`<br>`registros_evolucoes` · `evolucoes` | `(SELECT count(*) FROM UNNEST(coalesce(e.evolucoes, [])) AS evento WHERE evento.tipo_servico = 'outros')` |
| `evolucoes.registros` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `evolucoes.registros.id_evolucao_modulo` | `STRING` | `registros_evolucoes` · `evolucoes.id_evolucao_modulo` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.id_evolucao_modulo AS STRING), '')` |
| `evolucoes.registros.origem_modulo` | `STRING` | `registros_evolucoes` · `evolucoes.origem_modulo` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.origem_modulo AS STRING), '')` |
| `evolucoes.registros.id_familia` | `STRING` | `registros_evolucoes` · `evolucoes.id_familia` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.id_familia AS STRING), '')` |
| `evolucoes.registros.id_unidade` | `STRING` | `registros_evolucoes` · `evolucoes.id_unidade` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.id_unidade AS STRING), '')` |
| `evolucoes.registros.nome_unidade` | `STRING` | `registros_evolucoes` · `evolucoes.nome_unidade` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(item1.nome_unidade, '')` |
| `evolucoes.registros.data_evolucao` | `STRING` | `registros_evolucoes` · `evolucoes.data_evolucao` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.data_evolucao AS STRING), '')` |
| `evolucoes.registros.data_cancelamento` | `STRING` | `registros_evolucoes` · `evolucoes.data_cancelamento` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.data_cancelamento AS STRING), '')` |
| `evolucoes.registros.tipo_evolucao` | `STRING` | `registros_evolucoes` · `evolucoes.tipo_evolucao` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.tipo_evolucao AS STRING), '')` |
| `evolucoes.registros.nome_profissional` | `STRING` | `registros_evolucoes` · `evolucoes.nome_profissional` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.nome_profissional AS STRING), '')` |
| `evolucoes.registros.nome_atividade` | `STRING` | `registros_evolucoes` · `evolucoes.nome_atividade` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.nome_atividade AS STRING), '')` |
| `evolucoes.registros.descricao_evolucao` | `STRING` | `registros_evolucoes` · `evolucoes.descricao_evolucao` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(CAST(item1.descricao_evolucao AS STRING), '')` |
| `evolucoes.registros.formularios` | `ARRAY<STRUCT>` | Projeção da mart | Array de structs. Os campos abaixo detalham a origem de cada elemento. |
| `evolucoes.registros.formularios.titulo_formulario` | `STRING` | `registros_evolucoes` · `evolucoes.formularios.titulo_formulario` ← [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | `coalesce(CAST(item2.titulo_formulario AS STRING), '')` |
| `evolucoes.registros.formularios.data_ocorrencia` | `STRING` | `registros_evolucoes` · `evolucoes.formularios.data_ocorrencia` ← [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | `coalesce(CAST(item2.data_ocorrencia AS STRING), '')` |
| `evolucoes.registros.formularios.origem_data_ocorrencia` | `STRING` | `registros_evolucoes` · `evolucoes.formularios.origem_data_ocorrencia` ← [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | `item2.origem_data_ocorrencia` |
| `evolucoes.registros.formularios.situacao_data_ocorrencia` | `STRING` | `registros_evolucoes` · `evolucoes.formularios.situacao_data_ocorrencia` ← [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | `item2.situacao_data_ocorrencia` |
| `evolucoes.registros.formularios.observacoes` | `STRING` | `registros_evolucoes` · `evolucoes.formularios.observacoes` ← [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | `coalesce(CAST(item2.observacoes AS STRING), '')` |
| `evolucoes.registros.formularios.campos` | `ARRAY<STRING>` | `registros_evolucoes` · `evolucoes.formularios.campos` ← [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql)<br>`registros_evolucoes` · `evolucoes.formularios.campos.label` ← [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql)<br>`registros_evolucoes` · `evolucoes.formularios.campos.valor` ← [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | `ARRAY(SELECT CONCAT(coalesce(CAST(item3.label AS STRING), ''), ': ', coalesce(CAST(item3.valor AS STRING), '')) FROM UNNEST(item2.campos) AS item3 WITH OFFSET AS posicao ORDER BY posicao)` |
| `evolucoes.registros.tipo_servico` | `STRING` | `registros_evolucoes` · `evolucoes.tipo_servico` ← [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | `coalesce(item1.tipo_servico, '')` |

### centro_pop

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `centro_pop` | `STRUCT` | Projeção da mart | Struct com os campos detalhados abaixo. |
| `centro_pop.unidades_atendimento` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.unidades_atendimento` | `array_to_string(coalesce(perfil_cp.perfil.unidades_atendimento, []), ', ')` |
| `centro_pop.possui_registro_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.possui_registro_pai` | `coalesce(perfil_cp.perfil.possui_registro_pai, '')` |
| `centro_pop.data_ocorrencia_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_ocorrencia_pai` | `coalesce(CAST(perfil_cp.perfil.data_ocorrencia_pai AS STRING), '')` |
| `centro_pop.origem_data_ocorrencia_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.origem_data_ocorrencia_pai` | `coalesce(perfil_cp.perfil.origem_data_ocorrencia_pai, '')` |
| `centro_pop.situacao_data_ocorrencia_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.situacao_data_ocorrencia_pai` | `coalesce(perfil_cp.perfil.situacao_data_ocorrencia_pai, '')` |
| `centro_pop.data_registro_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_registro_pai` | `coalesce(CAST(perfil_cp.perfil.data_registro_pai AS STRING), '')` |
| `centro_pop.data_resposta_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_resposta_pai` | `coalesce(CAST(perfil_cp.perfil.data_resposta_pai AS STRING), '')` |
| `centro_pop.data_inclusao_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_inclusao_pai` | `coalesce(CAST(perfil_cp.perfil.data_inclusao_pai AS STRING), '')` |
| `centro_pop.data_inclusao_pai_informada` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_inclusao_pai_informada` | `coalesce(CAST(perfil_cp.perfil.data_inclusao_pai_informada AS STRING), '')` |
| `centro_pop.demandas_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.demandas_pai` | `array_to_string(coalesce(perfil_cp.perfil.demandas_pai, []), ', ')` |
| `centro_pop.encaminhamentos_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.encaminhamentos_pai` | `array_to_string(coalesce(perfil_cp.perfil.encaminhamentos_pai, []), ', ')` |
| `centro_pop.observacoes_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.observacoes_pai` | `coalesce(CAST(perfil_cp.perfil.observacoes_pai AS STRING), '')` |
| `centro_pop.data_ocorrencia_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_ocorrencia_atendimento_social` | `coalesce(CAST(perfil_cp.perfil.data_ocorrencia_atendimento_social AS STRING), '')` |
| `centro_pop.origem_data_ocorrencia_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.origem_data_ocorrencia_atendimento_social` | `coalesce(perfil_cp.perfil.origem_data_ocorrencia_atendimento_social, '')` |
| `centro_pop.situacao_data_ocorrencia_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.situacao_data_ocorrencia_atendimento_social` | `coalesce(perfil_cp.perfil.situacao_data_ocorrencia_atendimento_social, '')` |
| `centro_pop.data_registro_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_registro_atendimento_social` | `coalesce(CAST(perfil_cp.perfil.data_registro_atendimento_social AS STRING), '')` |
| `centro_pop.data_resposta_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_resposta_atendimento_social` | `coalesce(CAST(perfil_cp.perfil.data_resposta_atendimento_social AS STRING), '')` |
| `centro_pop.motivo_secundario_permanencia_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.motivo_secundario_permanencia_rua` | `coalesce(CAST(perfil_cp.perfil.motivo_secundario_permanencia_rua AS STRING), '')` |
| `centro_pop.possui_referencias_familiares` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.possui_referencias_familiares` | `coalesce(CAST(perfil_cp.perfil.possui_referencias_familiares AS STRING), '')` |
| `centro_pop.territorio_referencia_familia` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.territorio_referencia_familia` | `coalesce(CAST(perfil_cp.perfil.territorio_referencia_familia AS STRING), '')` |
| `centro_pop.possibilidade_reinsercao_familiar` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.possibilidade_reinsercao_familiar` | `coalesce(CAST(perfil_cp.perfil.possibilidade_reinsercao_familiar AS STRING), '')` |
| `centro_pop.demandas_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.demandas_atendimento_social` | `array_to_string(coalesce(perfil_cp.perfil.demandas_atendimento_social, []), ', ')` |
| `centro_pop.encaminhamentos_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.encaminhamentos_atendimento_social` | `array_to_string(coalesce(perfil_cp.perfil.encaminhamentos_atendimento_social, []), ', ')` |
| `centro_pop.observacoes_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.observacoes_atendimento_social` | `coalesce(CAST(perfil_cp.perfil.observacoes_atendimento_social AS STRING), '')` |
| `centro_pop.data_ocorrencia_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_ocorrencia_saude` | `coalesce(CAST(perfil_cp.perfil.data_ocorrencia_saude AS STRING), '')` |
| `centro_pop.origem_data_ocorrencia_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.origem_data_ocorrencia_saude` | `coalesce(perfil_cp.perfil.origem_data_ocorrencia_saude, '')` |
| `centro_pop.situacao_data_ocorrencia_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.situacao_data_ocorrencia_saude` | `coalesce(perfil_cp.perfil.situacao_data_ocorrencia_saude, '')` |
| `centro_pop.data_registro_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_registro_saude` | `coalesce(CAST(perfil_cp.perfil.data_registro_saude AS STRING), '')` |
| `centro_pop.data_resposta_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_resposta_saude` | `coalesce(CAST(perfil_cp.perfil.data_resposta_saude AS STRING), '')` |
| `centro_pop.uso_substancias_psicoativas` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `id_usuario`<br>[raw_usuarios_informacoes_medicas](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_informacoes_medicas.sql) · `flag_uso_substancias_psicoativas` | `CASE WHEN perfil_cp.id_usuario IS NULL THEN '' ELSE CASE upper(TRIM(CAST(medico.flag_uso_substancias_psicoativas AS STRING))) WHEN 'S' THEN 'Sim' WHEN 'N' THEN 'Não' ELSE '' END END` |
| `centro_pop.situacao_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.situacao_saude` | `coalesce(CAST(perfil_cp.perfil.situacao_saude AS STRING), '')` |
| `centro_pop.local_tratamento` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.local_tratamento` | `coalesce(CAST(perfil_cp.perfil.local_tratamento AS STRING), '')` |
| `centro_pop.possui_problema_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `id_usuario`<br>[raw_usuarios_informacoes_medicas](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_informacoes_medicas.sql) · `flag_possui_comorbidade` | `CASE WHEN perfil_cp.id_usuario IS NULL THEN '' ELSE CASE upper(TRIM(CAST(medico.flag_possui_comorbidade AS STRING))) WHEN 'S' THEN 'Sim' WHEN 'N' THEN 'Não' ELSE '' END END` |
| `centro_pop.realiza_acompanhamento_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.realiza_acompanhamento_saude` | `coalesce(CAST(perfil_cp.perfil.realiza_acompanhamento_saude AS STRING), '')` |
| `centro_pop.observacoes_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.observacoes_saude` | `coalesce(CAST(perfil_cp.perfil.observacoes_saude AS STRING), '')` |
| `centro_pop.data_ocorrencia_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_ocorrencia_situacao_rua` | `coalesce(CAST(perfil_cp.perfil.data_ocorrencia_situacao_rua AS STRING), '')` |
| `centro_pop.origem_data_ocorrencia_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.origem_data_ocorrencia_situacao_rua` | `coalesce(perfil_cp.perfil.origem_data_ocorrencia_situacao_rua, '')` |
| `centro_pop.situacao_data_ocorrencia_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.situacao_data_ocorrencia_situacao_rua` | `coalesce(perfil_cp.perfil.situacao_data_ocorrencia_situacao_rua, '')` |
| `centro_pop.data_registro_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_registro_situacao_rua` | `coalesce(CAST(perfil_cp.perfil.data_registro_situacao_rua AS STRING), '')` |
| `centro_pop.data_resposta_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_resposta_situacao_rua` | `coalesce(CAST(perfil_cp.perfil.data_resposta_situacao_rua AS STRING), '')` |
| `centro_pop.motivo_principal_permanencia_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.motivo_principal_permanencia_rua` | `coalesce(CAST(perfil_cp.perfil.motivo_principal_permanencia_rua AS STRING), '')` |
| `centro_pop.data_ocorrencia_desligamento_pai` | `DATE` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_ocorrencia_desligamento` | `SAFE_CAST(perfil_cp.perfil.data_ocorrencia_desligamento AS DATE)` |
| `centro_pop.origem_data_ocorrencia_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.origem_data_ocorrencia_desligamento` | `coalesce(perfil_cp.perfil.origem_data_ocorrencia_desligamento, '')` |
| `centro_pop.situacao_data_ocorrencia_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.situacao_data_ocorrencia_desligamento` | `coalesce(perfil_cp.perfil.situacao_data_ocorrencia_desligamento, '')` |
| `centro_pop.data_hora_registro_desligamento_pai` | `DATETIME` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_registro_desligamento` | `SAFE_CAST(perfil_cp.perfil.data_registro_desligamento AS DATETIME)` |
| `centro_pop.data_hora_resposta_desligamento_pai` | `DATETIME` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_resposta_desligamento` | `SAFE_CAST(perfil_cp.perfil.data_resposta_desligamento AS DATETIME)` |
| `centro_pop.data_desligamento_pai` | `DATE` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_desligamento` | `SAFE_CAST(perfil_cp.perfil.data_desligamento AS DATE)` |
| `centro_pop.data_desligamento_pai_informada` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_desligamento_informada` | `coalesce(perfil_cp.perfil.data_desligamento_informada, '')` |
| `centro_pop.motivo_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.motivo_desligamento` | `coalesce(perfil_cp.perfil.motivo_desligamento, '')` |
| `centro_pop.complemento_motivo_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.motivo_desligamento_outros` | `coalesce(perfil_cp.perfil.motivo_desligamento_outros, '')` |
| `centro_pop.observacoes_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.observacoes_desligamento` | `coalesce(perfil_cp.perfil.observacoes_desligamento, '')` |
| `centro_pop.flag_empregabilidade_imediata` | `STRING` | Constante da mart | `'Não Informado'` |
| `centro_pop.capacidade_habilidades` | `STRING` | Constante da mart | `'Não Informado'` |
| `centro_pop.flag_interesse_curso` | `STRING` | Constante da mart | `'Não Informado'` |
| `centro_pop.flag_incluido_acompanhamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `perfil.data_inclusao_pai` | `IF(NOT nullif(TRIM(CAST(perfil_cp.perfil.data_inclusao_pai AS STRING)), '') IS NULL, 'Sim', 'Não')` |
| `centro_pop.registros` | `ARRAY<STRUCT>` | CTE da mart | `coalesce(cp_reg.registros, [])` |
| `centro_pop.registros.id_unidade` | `INTEGER` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.nome_unidade` | `STRING` | [int_filtro_email_unidades](../models/intermediate/seguranca/int_filtro_email_unidades.sql) · `nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.unidades_atendimento` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.unidades_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.possui_registro_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.possui_registro_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_ocorrencia_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_ocorrencia_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.origem_data_ocorrencia_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.origem_data_ocorrencia_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.situacao_data_ocorrencia_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.situacao_data_ocorrencia_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_registro_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_registro_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_resposta_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_resposta_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_inclusao_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_inclusao_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_inclusao_pai_informada` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_inclusao_pai_informada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.demandas_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.demandas_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.encaminhamentos_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.encaminhamentos_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.observacoes_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.observacoes_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_ocorrencia_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_ocorrencia_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.origem_data_ocorrencia_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.origem_data_ocorrencia_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.situacao_data_ocorrencia_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.situacao_data_ocorrencia_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_registro_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_registro_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_resposta_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_resposta_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.motivo_secundario_permanencia_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.motivo_secundario_permanencia_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.possui_referencias_familiares` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.possui_referencias_familiares` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.territorio_referencia_familia` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.territorio_referencia_familia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.possibilidade_reinsercao_familiar` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.possibilidade_reinsercao_familiar` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.demandas_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.demandas_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.encaminhamentos_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.encaminhamentos_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.observacoes_atendimento_social` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.observacoes_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_ocorrencia_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_ocorrencia_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.origem_data_ocorrencia_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.origem_data_ocorrencia_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.situacao_data_ocorrencia_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.situacao_data_ocorrencia_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_registro_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_registro_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_resposta_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_resposta_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.uso_substancias_psicoativas` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.uso_substancias_psicoativas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.situacao_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.situacao_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.local_tratamento` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.local_tratamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.possui_problema_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.possui_problema_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.realiza_acompanhamento_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.realiza_acompanhamento_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.observacoes_saude` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.observacoes_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_ocorrencia_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_ocorrencia_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.origem_data_ocorrencia_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.origem_data_ocorrencia_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.situacao_data_ocorrencia_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.situacao_data_ocorrencia_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_registro_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_registro_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_resposta_situacao_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_resposta_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.motivo_principal_permanencia_rua` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.motivo_principal_permanencia_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_ocorrencia_desligamento_pai` | `DATE` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_ocorrencia_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.origem_data_ocorrencia_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.origem_data_ocorrencia_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.situacao_data_ocorrencia_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.situacao_data_ocorrencia_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_hora_registro_desligamento_pai` | `DATETIME` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_hora_registro_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_hora_resposta_desligamento_pai` | `DATETIME` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_hora_resposta_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_desligamento_pai` | `DATE` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.data_desligamento_pai_informada` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.data_desligamento_pai_informada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.motivo_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.motivo_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.complemento_motivo_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.complemento_motivo_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.observacoes_desligamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.observacoes_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.flag_empregabilidade_imediata` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.flag_empregabilidade_imediata` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.capacidade_habilidades` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.capacidade_habilidades` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.flag_interesse_curso` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.flag_interesse_curso` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.flag_incluido_acompanhamento_pai` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.flag_incluido_acompanhamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.ultima_participacao_atividade` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.ultima_participacao_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.ultima_participacao_unidade` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.ultima_participacao_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.ultima_participacao_data` | `DATE` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.ultima_participacao_data` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.registros.ultima_participacao_hora` | `STRING` | [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) · `centro_pop.ultima_participacao_hora` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal` | `ARRAY<STRUCT>` | CTE da mart | `coalesce(hist.historico_mensal, [])` |
| `centro_pop.historico_mensal.id_unidade` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.nome_unidade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.mes_referencia` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `mes_referencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.situacao` | `STRING` | int_centro_pop_mensal | Constante Prévia na reconstrução mensal. |
| `centro_pop.historico_mensal.versao` | `INTEGER` | int_centro_pop_mensal | NULL tipado como INT64. |
| `centro_pop.historico_mensal.data_snapshot` | `TIMESTAMP` | int_centro_pop_mensal | NULL tipado como TIMESTAMP. |
| `centro_pop.historico_mensal.id_fechamento` | `STRING` | int_centro_pop_mensal | NULL tipado como STRING. |
| `centro_pop.historico_mensal.data_atualizacao` | `TIMESTAMP` | int_centro_pop_mensal | Timestamp da execução do modelo mensal. |
| `centro_pop.historico_mensal.data_carga_fontes` | `TIMESTAMP` | int_centro_pop_mensal | Menor data de carga entre as fontes consideradas no modelo mensal. |
| `centro_pop.historico_mensal.quantidade_atendimentos` | `INTEGER` | int_centro_pop_mensal | Tamanho do array de eventos de atendimento da competência. Conta eventos, conforme o intermediate. |
| `centro_pop.historico_mensal.quantidade_evolucoes` | `INTEGER` | int_centro_pop_mensal | Tamanho do array de evoluções da competência. |
| `centro_pop.historico_mensal.quantidade_participacoes` | `INTEGER` | int_centro_pop_mensal | Contagem das ocorrências de presença com tipo participação. Métrica do histórico Centro POP. |
| `centro_pop.historico_mensal.unidades_atendimento` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `unidades_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.possui_registro_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `possui_registro_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_ocorrencia_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_ocorrencia_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.origem_data_ocorrencia_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `origem_data_ocorrencia_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.situacao_data_ocorrencia_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `situacao_data_ocorrencia_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_registro_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_registro_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_resposta_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_resposta_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_inclusao_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_inclusao_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_inclusao_pai_informada` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_inclusao_pai_informada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.demandas_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `demandas_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.encaminhamentos_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `encaminhamentos_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.observacoes_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `observacoes_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_ocorrencia_atendimento_social` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_ocorrencia_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.origem_data_ocorrencia_atendimento_social` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `origem_data_ocorrencia_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.situacao_data_ocorrencia_atendimento_social` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `situacao_data_ocorrencia_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_registro_atendimento_social` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_registro_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_resposta_atendimento_social` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_resposta_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.motivo_secundario_permanencia_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `motivo_secundario_permanencia_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.possui_referencias_familiares` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `possui_referencias_familiares` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.territorio_referencia_familia` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `territorio_referencia_familia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.possibilidade_reinsercao_familiar` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `possibilidade_reinsercao_familiar` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.demandas_atendimento_social` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `demandas_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.encaminhamentos_atendimento_social` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `encaminhamentos_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.observacoes_atendimento_social` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `observacoes_atendimento_social` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_ocorrencia_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_ocorrencia_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.origem_data_ocorrencia_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `origem_data_ocorrencia_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.situacao_data_ocorrencia_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `situacao_data_ocorrencia_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_registro_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_registro_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_resposta_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_resposta_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.uso_substancias_psicoativas` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `uso_substancias_psicoativas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.situacao_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `situacao_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.local_tratamento` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `local_tratamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.possui_problema_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `possui_problema_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.realiza_acompanhamento_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `realiza_acompanhamento_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.observacoes_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `observacoes_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_ocorrencia_situacao_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_ocorrencia_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.origem_data_ocorrencia_situacao_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `origem_data_ocorrencia_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.situacao_data_ocorrencia_situacao_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `situacao_data_ocorrencia_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_registro_situacao_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_registro_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_resposta_situacao_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_resposta_situacao_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.motivo_principal_permanencia_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `motivo_principal_permanencia_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_ocorrencia_desligamento_pai` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_ocorrencia_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.origem_data_ocorrencia_desligamento_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `origem_data_ocorrencia_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.situacao_data_ocorrencia_desligamento_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `situacao_data_ocorrencia_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_hora_registro_desligamento_pai` | `DATETIME` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_hora_registro_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_hora_resposta_desligamento_pai` | `DATETIME` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_hora_resposta_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_desligamento_pai` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.data_desligamento_pai_informada` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_desligamento_pai_informada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.motivo_desligamento_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `motivo_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.complemento_motivo_desligamento_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `complemento_motivo_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.observacoes_desligamento_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `observacoes_desligamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.flag_empregabilidade_imediata` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `flag_empregabilidade_imediata` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.capacidade_habilidades` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `capacidade_habilidades` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.flag_interesse_curso` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `flag_interesse_curso` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.flag_incluido_acompanhamento_pai` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `flag_incluido_acompanhamento_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ultima_participacao_atividade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ultima_participacao_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ultima_participacao_unidade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ultima_participacao_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ultima_participacao_data` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ultima_participacao_data` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ultima_participacao_hora` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ultima_participacao_hora` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos` | `ARRAY<STRUCT>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.id_atendimento` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.id_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.id_unidade` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.nome_unidade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.id_tipo_atendimento` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.id_tipo_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.tipo_atendimento_descricao` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.tipo_atendimento_descricao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.data_atendimento` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.data_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.hora_atendimento` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.hora_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.cas` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.cas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.email_unidade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.email_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.email_cas` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.email_cas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.tipo_unidade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.tipo_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.elegivel_indicador` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.elegivel_indicador` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.cadastrante` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.cadastrante` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.data_cadastro_atendimento` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.data_cadastro_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.id_atendimento_modulo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.id_atendimento_modulo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.origem_modulo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.origem_modulo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.tipo_servico` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.tipo_servico` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.id_familia` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.id_familia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.classificacao_atendimento` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.classificacao_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais` | `ARRAY<STRUCT>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.id_profissional` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.id_profissional` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.nome_profissional` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.nome_profissional` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.codigos_cbo` | `ARRAY<STRING>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.codigos_cbo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.descricoes_cbo` | `ARRAY<STRING>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.descricoes_cbo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.cbo_principal_codigo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.cbo_principal_codigo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.cbo_principal_descricao` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.cbo_principal_descricao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.profissional_cbo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.profissional_cbo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.classificacao_atendimento` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.classificacao_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.nome_atendimento` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.nome_atendimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.id_familia_referencia` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.id_familia_referencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.atendimentos.profissionais.id_atendimento_legado` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `atendimentos.profissionais.id_atendimento_legado` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes` | `ARRAY<STRUCT>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.id_evolucao` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.id_evolucao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.origem_modulo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.origem_modulo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.id_familia` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.id_familia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.id_unidade` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.nome_unidade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.tipo_servico` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.tipo_servico` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.id_profissional` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.id_profissional` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.codigo_abrangencia` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.codigo_abrangencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.id_atividade` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.id_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.data_evolucao` | `DATETIME` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.data_evolucao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.data_cancelamento` | `DATETIME` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.data_cancelamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.tipo_evolucao` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.tipo_evolucao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.descricao_evolucao` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.descricao_evolucao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.nome_atividade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.nome_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.nome_profissional` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.nome_profissional` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.id_evolucao_modulo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.id_evolucao_modulo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.data_evento` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.data_evento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios` | `ARRAY<STRUCT>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.id_evolucao_modulo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.id_evolucao_modulo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.id_evolucao` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.id_evolucao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.origem_modulo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.origem_modulo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.id_familia` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.id_familia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.id_unidade` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.id_profissional` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.id_profissional` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.codigo_abrangencia` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.codigo_abrangencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.id_atividade` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.id_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.data_evolucao` | `DATETIME` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.data_evolucao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.data_cancelamento` | `DATETIME` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.data_cancelamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.tipo_evolucao` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.tipo_evolucao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.nome_profissional` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.nome_profissional` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.nome_atividade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.nome_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.titulo_formulario` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.titulo_formulario` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.observacoes_html` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.observacoes_html` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.observacoes` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.observacoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.campos` | `ARRAY<STRUCT>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.campos` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.campos.label` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.campos.label` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.campos.valor` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.campos.valor` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.data_ocorrencia` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.origem_data_ocorrencia` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.origem_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.evolucoes.formularios.situacao_data_ocorrencia` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `evolucoes.formularios.situacao_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias` | `ARRAY<STRUCT>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.id_registro` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.id_registro` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.id_unidade` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.origem` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.origem` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.tipo_registro` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.tipo_registro` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.abrangencia` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.abrangencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.id_evolucao_modulo` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.id_evolucao_modulo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.titulo_formulario` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.titulo_formulario` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_ocorrencia` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.origem_data_ocorrencia` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.origem_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.situacao_data_ocorrencia` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.situacao_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_registro` | `DATETIME` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_registro` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_resposta` | `DATETIME` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_resposta` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_inclusao_pai` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_inclusao_pai` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_inclusao_informada` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_inclusao_informada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.motivo_principal_permanencia_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.motivo_principal_permanencia_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.motivo_secundario_permanencia_rua` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.motivo_secundario_permanencia_rua` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.possui_referencias_familiares` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.possui_referencias_familiares` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.territorio_referencia_familia` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.territorio_referencia_familia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.possibilidade_reinsercao_familiar` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.possibilidade_reinsercao_familiar` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.uso_substancias_psicoativas` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.uso_substancias_psicoativas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.situacao_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.situacao_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.local_tratamento` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.local_tratamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.possui_problema_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.possui_problema_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.realiza_acompanhamento_saude` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.realiza_acompanhamento_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_desligamento` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_desligamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_desligamento_informada` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_desligamento_informada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.motivo_desligamento` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.motivo_desligamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.motivo_desligamento_outros` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.motivo_desligamento_outros` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.demandas` | `ARRAY<STRING>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.demandas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.encaminhamentos` | `ARRAY<STRUCT>` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.encaminhamentos` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.encaminhamentos.label` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.encaminhamentos.label` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.encaminhamentos.valor` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.encaminhamentos.valor` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.observacoes` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.observacoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.empregabilidade_imediata` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.empregabilidade_imediata` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.capacidade_habilidades` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.capacidade_habilidades` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.interesse_curso` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.interesse_curso` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.resultado_acesso` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.resultado_acesso` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.resultado_descricao` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.resultado_descricao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.id_atividade` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.id_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.nome_atividade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.nome_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_presenca` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_presenca` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.hora_presenca` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.hora_presenca` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.conflitos` | `INTEGER` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.conflitos` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.nome_unidade` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.ocorrencias.data_evento` | `DATE` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `ocorrencias.data_evento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.historico_mensal.procedimentos_mes` | `STRING` | int_centro_pop_mensal | Demandas distintas de PAI e atendimento social no mês, concatenadas em ordem alfabética. |
| `centro_pop.historico_mensal.encaminhamentos_mes` | `STRING` | int_centro_pop_mensal | Pares rótulo e valor preenchidos das ocorrências do mês, distintos e concatenados em ordem alfabética. |
| `centro_pop.historico_mensal.data_ultimo_atend_tecnico_informada` | `STRING` | [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) · `data_ultimo_atend_tecnico_informada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `centro_pop.ultima_participacao_atividade` | `STRING` | `escopo_centro_pop` · `participacoes` ← [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql) | `esc.participacoes[SAFE_OFFSET(ARRAY_LENGTH(esc.participacoes) - 1)].nome_atividade` |
| `centro_pop.ultima_participacao_unidade` | `STRING` | `escopo_centro_pop` · `participacoes` ← [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql) | `esc.participacoes[SAFE_OFFSET(ARRAY_LENGTH(esc.participacoes) - 1)].nome_unidade` |
| `centro_pop.ultima_participacao_data` | `STRING` | `escopo_centro_pop` · `participacoes` ← [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql) | `esc.participacoes[SAFE_OFFSET(ARRAY_LENGTH(esc.participacoes) - 1)].data_presenca` |
| `centro_pop.ultima_participacao_hora` | `STRING` | `escopo_centro_pop` · `participacoes` ← [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql) | `esc.participacoes[SAFE_OFFSET(ARRAY_LENGTH(esc.participacoes) - 1)].hora_presenca` |
| `centro_pop.encaminhamentos` | `STRING` | `encaminhamentos_centro_pop` · `encaminhamentos` ← [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql) | `coalesce(cp_enc.encaminhamentos, '')` |
| `centro_pop.procedimentos` | `STRING` | `procedimentos_centro_pop` · `procedimentos` ← [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql) | `coalesce(cp_proc.procedimentos, '')` |
| `centro_pop.data_ultimo_atend_tecnico_informada` | `STRING` | `ultimo_atendimento_tecnico_centro_pop` · `data_ultimo_atend_tecnico_informada` | `coalesce(CAST(ultimo_cp.data_ultimo_atend_tecnico_informada AS STRING), '')` |

### creas

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `creas` | `STRUCT` | Projeção da mart | Struct com os campos detalhados abaixo. |
| `creas.registros` | `ARRAY<STRUCT>` | CTE da mart | `coalesce(cr_reg.registros, [])` |
| `creas.registros.id_unidade` | `INTEGER` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.nome_unidade` | `STRING` | [int_filtro_email_unidades](../models/intermediate/seguranca/int_filtro_email_unidades.sql) · `nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_data_ocorrencia` | `DATE` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_data_registro` | `DATETIME` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_data_registro` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_id_unidade` | `INTEGER` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_nome_unidade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_observacoes` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_observacoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · - Com que frequência vai à casa da família ou pessoa de referência?. |
| `creas.registros.mse_com_quem_moraria` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_com_quem_moraria` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · - Com quem moraria?. |
| `creas.registros.mse_1o_bimestre_documentacao` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_1o_bimestre_documentacao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre  [Documentação]. |
| `creas.registros.mse_1o_bimestre_beneficios` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_1o_bimestre_beneficios` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [Benefícios]. |
| `creas.registros.mse_1o_bimestre_cadunico` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_1o_bimestre_cadunico` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [CadÚnico]. |
| `creas.registros.mse_1o_bimestre_localizacao` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_1o_bimestre_localizacao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [Localização]. |
| `creas.registros.mse_1o_bimestre_outras_acoes` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_1o_bimestre_outras_acoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [Outras Ações]. |
| `creas.registros.mse_1o_bimestre_saude` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_1o_bimestre_saude` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [Saúde]. |
| `creas.registros.mse_adolescente_em_reinfracao` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_adolescente_em_reinfracao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Adolescente em reinfração?. |
| `creas.registros.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Adolescente tem histórico de cumprimento de MSE em meio fechado ou semiliberdade?. |
| `creas.registros.mse_avaliacao_tecnica_bimestral_1o_bimestre` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_avaliacao_tecnica_bimestral_1o_bimestre` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Avaliação Técnica Bimestral  &#124; 1º Bimestre. |
| `creas.registros.mse_cras_de_referencia` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_cras_de_referencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · CRAS de Referência / CRAS de referência. |
| `creas.registros.mse_creas_de_referencia` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_creas_de_referencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · CREAS de Referência / CREAS de referência. |
| `creas.registros.mse_carga_horaria` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_carga_horaria` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Carga horária. |
| `creas.registros.mse_com_quem_residia_anteriormente` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_com_quem_residia_anteriormente` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Com quem residia anteriormente?. |
| `creas.registros.mse_data` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_data` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Data. |
| `creas.registros.mse_data_de_inclusao_no_mse` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_data_de_inclusao_no_mse` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Data de inclusão no MSE. |
| `creas.registros.mse_desdobramento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_desdobramento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Desdobramento. |
| `creas.registros.mse_em_caso_de_cumprimento_de_mse_qual_o_status` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_em_caso_de_cumprimento_de_mse_qual_o_status` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Em caso de cumprimento de MSE, qual o status?. |
| `creas.registros.mse_em_caso_de_cumprimento_de_psc_qual_o_local` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_em_caso_de_cumprimento_de_psc_qual_o_local` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Em caso de cumprimento de PSC, qual o local?. |
| `creas.registros.mse_endereco_cras` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_endereco_cras` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Endereço (CRAS). |
| `creas.registros.mse_endereco_creas` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_endereco_creas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Endereço (CREAS). |
| `creas.registros.mse_especialidade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_especialidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Especialidade. |
| `creas.registros.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Gostaria de retornar para casa ou convívio anterior?. |
| `creas.registros.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Gostaria de retornar para casa ou convívio anterior?  - Com quem moraria?. |
| `creas.registros.mse_local` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_local` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Local. |
| `creas.registros.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Mantém contato com a Família ou pessoa de referência?. |
| `creas.registros.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Mantém contato com a Família ou pessoa de referência?  - Com que frequência vai à casa da família ou pessoa de referência?. |
| `creas.registros.mse_medida_aplicada` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_medida_aplicada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Medida aplicada. |
| `creas.registros.mse_nome_do_local_de_cumprimento_de_psc` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_nome_do_local_de_cumprimento_de_psc` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Nome do local de cumprimento de PSC. |
| `creas.registros.mse_outras_atividades` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_outras_atividades` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Outras atividades. |
| `creas.registros.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Por que se afastou da família ou pessoa de referência?. |
| `creas.registros.mse_procedimento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_procedimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Procedimento. |
| `creas.registros.mse_profissional_cras` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_profissional_cras` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Profissional (CRAS). |
| `creas.registros.mse_profissional_creas` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_profissional_creas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Profissional (CREAS). |
| `creas.registros.mse_quais_atividades_gostaria_de_participar` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_quais_atividades_gostaria_de_participar` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Quais atividades gostaria de participar?. |
| `creas.registros.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Registrar o que conseguiu cumprir e justificar o que não conseguiu cumprir no prazo estabelecido. |
| `creas.registros.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Serviço de proteção social ao adolescente em cumprimento de LA ou PSC. |
| `creas.registros.mse_telefone_cras` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_telefone_cras` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Telefone (CRAS). |
| `creas.registros.mse_telefone_creas` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_telefone_creas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Telefone (CREAS). |
| `creas.registros.mse_tempo_de_cumprimento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_tempo_de_cumprimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Tempo de cumprimento. |
| `creas.registros.mse_tipo` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_tipo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Tipo. |
| `creas.registros.mse_tipo_de_ato_infracional` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_tipo_de_ato_infracional` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Tipo de ato infracional. |
| `creas.registros.mse_usuario_interditado` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_usuario_interditado` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Usuário Interditado?. |
| `creas.registros.mse_retorno_data_ocorrencia` | `DATE` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_retorno_data_registro` | `DATETIME` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_data_registro` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_retorno_id_unidade` | `INTEGER` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_retorno_nome_unidade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_retorno_observacoes` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_observacoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.mse_retorno_adolescente_em_reinfracao` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_adolescente_em_reinfracao` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - RETORNO** · Adolescente em reinfração?. |
| `creas.registros.mse_retorno_data_do_desligamento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_data_do_desligamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - RETORNO** · Data do desligamento. |
| `creas.registros.mse_retorno_desligamento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_desligamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - RETORNO** · Desligamento. |
| `creas.registros.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - RETORNO** · Em caso de cumprimento de MSE, qual o status?. |
| `creas.registros.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - RETORNO** · Em caso de cumprimento de PSC, qual o local?. |
| `creas.registros.mse_retorno_motivo_do_desligamento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_motivo_do_desligamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - RETORNO** · Motivo do desligamento. |
| `creas.registros.mse_retorno_nome_do_local_de_cumprimento_de_psc` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.mse_retorno_nome_do_local_de_cumprimento_de_psc` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - MSE - RETORNO** · Nome do local de cumprimento de PSC. |
| `creas.registros.peti_data_ocorrencia` | `DATE` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_data_registro` | `DATETIME` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_data_registro` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_id_unidade` | `INTEGER` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_nome_unidade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_observacoes` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_observacoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_data_da_inclusao_no_peti` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_data_da_inclusao_no_peti` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Data da inclusão no PETI. |
| `creas.registros.peti_demanda_inicial` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_demanda_inicial` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Demanda Inicial. |
| `creas.registros.peti_forma_de_acesso_informe_a_origem_de_acesso` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_forma_de_acesso_informe_a_origem_de_acesso` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Forma de acesso (informe a origem de acesso). |
| `creas.registros.peti_marcacao_nos_campo_cadunico` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_marcacao_nos_campo_cadunico` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Marcação nos campo CadÚnico. |
| `creas.registros.peti_orientacoes_e_encaminhamentos` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_orientacoes_e_encaminhamentos` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Orientações e Encaminhamentos. |
| `creas.registros.peti_outras_demandas_vulnerabilidades` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_outras_demandas_vulnerabilidades` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Outras demandas / vulnerabilidades. |
| `creas.registros.peti_outras_formas_de_acesso` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_outras_formas_de_acesso` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Outras formas de acesso. |
| `creas.registros.peti_outros` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_outros` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Outros. |
| `creas.registros.peti_tipo_de_trabalho_infantil` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_tipo_de_trabalho_infantil` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Tipo de trabalho infantil. |
| `creas.registros.peti_e_acompanhado_em_outro_creas` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_e_acompanhado_em_outro_creas` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · É acompanhado em outro CREAS?. |
| `creas.registros.peti_e_necessario_realizar_a_gestao_de_beneficios` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_e_necessario_realizar_a_gestao_de_beneficios` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · É necessário realizar a Gestão de Benefícios?. |
| `creas.registros.scfv_data_ocorrencia` | `DATE` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.scfv_data_registro` | `DATETIME` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_data_registro` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.scfv_id_unidade` | `INTEGER` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.scfv_nome_unidade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.scfv_observacoes` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_observacoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.scfv_data_da_solicitacao_do_acolhimento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_data_da_solicitacao_do_acolhimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Data da solicitação do acolhimento. |
| `creas.registros.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Demanda de acolhimento institucional por decisão judicial?. |
| `creas.registros.scfv_nome_do_cras_responsavel` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_nome_do_cras_responsavel` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Nome do CRAS responsável. |
| `creas.registros.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · O usuário participa do SCFV (Serviço de Convivência e Fortalecimento de Vínculos)?. |
| `creas.registros.scfv_oriundo_de_instituicao_de_acolhimento_interditado` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_oriundo_de_instituicao_de_acolhimento_interditado` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Oriundo de instituição de acolhimento interditado?. |
| `creas.registros.scfv_usuario_interditado` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_usuario_interditado` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Usuário Interditado?. |
| `creas.registros.scfv_orgao_de_origem_do_acolhimento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.scfv_orgao_de_origem_do_acolhimento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Órgão de origem do acolhimento. |
| `creas.registros.peti_desligamento_data_ocorrencia` | `DATE` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_desligamento_data_ocorrencia` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_desligamento_data_registro` | `DATETIME` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_desligamento_data_registro` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_desligamento_id_unidade` | `INTEGER` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_desligamento_id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_desligamento_nome_unidade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_desligamento_nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_desligamento_observacoes` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_desligamento_observacoes` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.peti_desligamento_data_do_desligamento_do_peti` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_desligamento_data_do_desligamento_do_peti` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil** · Data do desligamento do PETI. |
| `creas.registros.peti_desligamento_motivo_do_desligamento` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_desligamento_motivo_do_desligamento` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil** · Motivo do desligamento. |
| `creas.registros.peti_desligamento_outros` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.peti_desligamento_outros` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. Catálogo CREAS · **CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil** · Outros. |
| `creas.registros.passo_a_passo_ultima_unidade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.passo_a_passo_ultima_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.passo_a_passo_ultima_atividade` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.passo_a_passo_ultima_atividade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.passo_a_passo_ultima_data` | `DATE` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.passo_a_passo_ultima_data` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.registros.passo_a_passo_ultima_hora` | `STRING` | [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) · `creas.passo_a_passo_ultima_hora` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `creas.mse_data_ocorrencia` | `DATE` | `creas_por_unidade` · `creas.mse_data_ocorrencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_data_ocorrencia` |
| `creas.mse_data_registro` | `DATETIME` | `creas_por_unidade` · `creas.mse_data_registro` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_data_registro` |
| `creas.mse_id_unidade` | `INTEGER` | `creas_por_unidade` · `creas.mse_id_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_id_unidade` |
| `creas.mse_nome_unidade` | `STRING` | `creas_por_unidade` · `creas.mse_nome_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_nome_unidade` |
| `creas.mse_observacoes` | `STRING` | `creas_por_unidade` · `creas.mse_observacoes` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_observacoes` |
| `creas.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` | `STRING` | `creas_por_unidade` · `creas.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · - Com que frequência vai à casa da família ou pessoa de referência?. |
| `creas.mse_com_quem_moraria` | `STRING` | `creas_por_unidade` · `creas.mse_com_quem_moraria` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_com_quem_moraria` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · - Com quem moraria?. |
| `creas.mse_1o_bimestre_documentacao` | `STRING` | `creas_por_unidade` · `creas.mse_1o_bimestre_documentacao` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_1o_bimestre_documentacao` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre  [Documentação]. |
| `creas.mse_1o_bimestre_beneficios` | `STRING` | `creas_por_unidade` · `creas.mse_1o_bimestre_beneficios` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_1o_bimestre_beneficios` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [Benefícios]. |
| `creas.mse_1o_bimestre_cadunico` | `STRING` | `creas_por_unidade` · `creas.mse_1o_bimestre_cadunico` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_1o_bimestre_cadunico` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [CadÚnico]. |
| `creas.mse_1o_bimestre_localizacao` | `STRING` | `creas_por_unidade` · `creas.mse_1o_bimestre_localizacao` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_1o_bimestre_localizacao` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [Localização]. |
| `creas.mse_1o_bimestre_outras_acoes` | `STRING` | `creas_por_unidade` · `creas.mse_1o_bimestre_outras_acoes` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_1o_bimestre_outras_acoes` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [Outras Ações]. |
| `creas.mse_1o_bimestre_saude` | `STRING` | `creas_por_unidade` · `creas.mse_1o_bimestre_saude` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_1o_bimestre_saude` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · 1º Bimestre [Saúde]. |
| `creas.mse_adolescente_em_reinfracao` | `STRING` | `creas_por_unidade` · `creas.mse_adolescente_em_reinfracao` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_adolescente_em_reinfracao` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Adolescente em reinfração?. |
| `creas.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` | `STRING` | `creas_por_unidade` · `creas.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Adolescente tem histórico de cumprimento de MSE em meio fechado ou semiliberdade?. |
| `creas.mse_avaliacao_tecnica_bimestral_1o_bimestre` | `STRING` | `creas_por_unidade` · `creas.mse_avaliacao_tecnica_bimestral_1o_bimestre` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_avaliacao_tecnica_bimestral_1o_bimestre` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Avaliação Técnica Bimestral  &#124; 1º Bimestre. |
| `creas.mse_cras_de_referencia` | `STRING` | `creas_por_unidade` · `creas.mse_cras_de_referencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_cras_de_referencia` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · CRAS de Referência / CRAS de referência. |
| `creas.mse_creas_de_referencia` | `STRING` | `creas_por_unidade` · `creas.mse_creas_de_referencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_creas_de_referencia` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · CREAS de Referência / CREAS de referência. |
| `creas.mse_carga_horaria` | `STRING` | `creas_por_unidade` · `creas.mse_carga_horaria` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_carga_horaria` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Carga horária. |
| `creas.mse_com_quem_residia_anteriormente` | `STRING` | `creas_por_unidade` · `creas.mse_com_quem_residia_anteriormente` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_com_quem_residia_anteriormente` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Com quem residia anteriormente?. |
| `creas.mse_data` | `STRING` | `creas_por_unidade` · `creas.mse_data` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_data` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Data. |
| `creas.mse_data_de_inclusao_no_mse` | `STRING` | `creas_por_unidade` · `creas.mse_data_de_inclusao_no_mse` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_data_de_inclusao_no_mse` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Data de inclusão no MSE. |
| `creas.mse_desdobramento` | `STRING` | `creas_por_unidade` · `creas.mse_desdobramento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_desdobramento` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Desdobramento. |
| `creas.mse_em_caso_de_cumprimento_de_mse_qual_o_status` | `STRING` | `creas_por_unidade` · `creas.mse_em_caso_de_cumprimento_de_mse_qual_o_status` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_em_caso_de_cumprimento_de_mse_qual_o_status` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Em caso de cumprimento de MSE, qual o status?. |
| `creas.mse_em_caso_de_cumprimento_de_psc_qual_o_local` | `STRING` | `creas_por_unidade` · `creas.mse_em_caso_de_cumprimento_de_psc_qual_o_local` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_em_caso_de_cumprimento_de_psc_qual_o_local` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Em caso de cumprimento de PSC, qual o local?. |
| `creas.mse_endereco_cras` | `STRING` | `creas_por_unidade` · `creas.mse_endereco_cras` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_endereco_cras` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Endereço (CRAS). |
| `creas.mse_endereco_creas` | `STRING` | `creas_por_unidade` · `creas.mse_endereco_creas` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_endereco_creas` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Endereço (CREAS). |
| `creas.mse_especialidade` | `STRING` | `creas_por_unidade` · `creas.mse_especialidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_especialidade` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Especialidade. |
| `creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` | `STRING` | `creas_por_unidade` · `creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Gostaria de retornar para casa ou convívio anterior?. |
| `creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` | `STRING` | `creas_por_unidade` · `creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Gostaria de retornar para casa ou convívio anterior?  - Com quem moraria?. |
| `creas.mse_local` | `STRING` | `creas_por_unidade` · `creas.mse_local` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_local` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Local. |
| `creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` | `STRING` | `creas_por_unidade` · `creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Mantém contato com a Família ou pessoa de referência?. |
| `creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` | `STRING` | `creas_por_unidade` · `creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Mantém contato com a Família ou pessoa de referência?  - Com que frequência vai à casa da família ou pessoa de referência?. |
| `creas.mse_medida_aplicada` | `STRING` | `creas_por_unidade` · `creas.mse_medida_aplicada` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_medida_aplicada` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Medida aplicada. |
| `creas.mse_nome_do_local_de_cumprimento_de_psc` | `STRING` | `creas_por_unidade` · `creas.mse_nome_do_local_de_cumprimento_de_psc` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_nome_do_local_de_cumprimento_de_psc` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Nome do local de cumprimento de PSC. |
| `creas.mse_outras_atividades` | `STRING` | `creas_por_unidade` · `creas.mse_outras_atividades` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_outras_atividades` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Outras atividades. |
| `creas.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` | `STRING` | `creas_por_unidade` · `creas.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Por que se afastou da família ou pessoa de referência?. |
| `creas.mse_procedimento` | `STRING` | `creas_por_unidade` · `creas.mse_procedimento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_procedimento` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Procedimento. |
| `creas.mse_profissional_cras` | `STRING` | `creas_por_unidade` · `creas.mse_profissional_cras` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_profissional_cras` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Profissional (CRAS). |
| `creas.mse_profissional_creas` | `STRING` | `creas_por_unidade` · `creas.mse_profissional_creas` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_profissional_creas` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Profissional (CREAS). |
| `creas.mse_quais_atividades_gostaria_de_participar` | `STRING` | `creas_por_unidade` · `creas.mse_quais_atividades_gostaria_de_participar` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_quais_atividades_gostaria_de_participar` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Quais atividades gostaria de participar?. |
| `creas.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` | `STRING` | `creas_por_unidade` · `creas.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Registrar o que conseguiu cumprir e justificar o que não conseguiu cumprir no prazo estabelecido. |
| `creas.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` | `STRING` | `creas_por_unidade` · `creas.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Serviço de proteção social ao adolescente em cumprimento de LA ou PSC. |
| `creas.mse_telefone_cras` | `STRING` | `creas_por_unidade` · `creas.mse_telefone_cras` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_telefone_cras` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Telefone (CRAS). |
| `creas.mse_telefone_creas` | `STRING` | `creas_por_unidade` · `creas.mse_telefone_creas` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_telefone_creas` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Telefone (CREAS). |
| `creas.mse_tempo_de_cumprimento` | `STRING` | `creas_por_unidade` · `creas.mse_tempo_de_cumprimento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_tempo_de_cumprimento` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Tempo de cumprimento. |
| `creas.mse_tipo` | `STRING` | `creas_por_unidade` · `creas.mse_tipo` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_tipo` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Tipo. |
| `creas.mse_tipo_de_ato_infracional` | `STRING` | `creas_por_unidade` · `creas.mse_tipo_de_ato_infracional` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_tipo_de_ato_infracional` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Tipo de ato infracional. |
| `creas.mse_usuario_interditado` | `STRING` | `creas_por_unidade` · `creas.mse_usuario_interditado` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_usuario_interditado` Catálogo CREAS · **CREAS - MSE - Medidas Socioeducativas** · Usuário Interditado?. |
| `creas.mse_retorno_data_ocorrencia` | `DATE` | `creas_por_unidade` · `creas.mse_retorno_data_ocorrencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_data_ocorrencia` |
| `creas.mse_retorno_data_registro` | `DATETIME` | `creas_por_unidade` · `creas.mse_retorno_data_registro` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_data_registro` |
| `creas.mse_retorno_id_unidade` | `INTEGER` | `creas_por_unidade` · `creas.mse_retorno_id_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_id_unidade` |
| `creas.mse_retorno_nome_unidade` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_nome_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_nome_unidade` |
| `creas.mse_retorno_observacoes` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_observacoes` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_observacoes` |
| `creas.mse_retorno_adolescente_em_reinfracao` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_adolescente_em_reinfracao` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_adolescente_em_reinfracao` Catálogo CREAS · **CREAS - MSE - RETORNO** · Adolescente em reinfração?. |
| `creas.mse_retorno_data_do_desligamento` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_data_do_desligamento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_data_do_desligamento` Catálogo CREAS · **CREAS - MSE - RETORNO** · Data do desligamento. |
| `creas.mse_retorno_desligamento` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_desligamento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_desligamento` Catálogo CREAS · **CREAS - MSE - RETORNO** · Desligamento. |
| `creas.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` Catálogo CREAS · **CREAS - MSE - RETORNO** · Em caso de cumprimento de MSE, qual o status?. |
| `creas.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` Catálogo CREAS · **CREAS - MSE - RETORNO** · Em caso de cumprimento de PSC, qual o local?. |
| `creas.mse_retorno_motivo_do_desligamento` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_motivo_do_desligamento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_motivo_do_desligamento` Catálogo CREAS · **CREAS - MSE - RETORNO** · Motivo do desligamento. |
| `creas.mse_retorno_nome_do_local_de_cumprimento_de_psc` | `STRING` | `creas_por_unidade` · `creas.mse_retorno_nome_do_local_de_cumprimento_de_psc` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.mse_retorno_nome_do_local_de_cumprimento_de_psc` Catálogo CREAS · **CREAS - MSE - RETORNO** · Nome do local de cumprimento de PSC. |
| `creas.peti_data_ocorrencia` | `DATE` | `creas_por_unidade` · `creas.peti_data_ocorrencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_data_ocorrencia` |
| `creas.peti_data_registro` | `DATETIME` | `creas_por_unidade` · `creas.peti_data_registro` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_data_registro` |
| `creas.peti_id_unidade` | `INTEGER` | `creas_por_unidade` · `creas.peti_id_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_id_unidade` |
| `creas.peti_nome_unidade` | `STRING` | `creas_por_unidade` · `creas.peti_nome_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_nome_unidade` |
| `creas.peti_observacoes` | `STRING` | `creas_por_unidade` · `creas.peti_observacoes` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_observacoes` |
| `creas.peti_data_da_inclusao_no_peti` | `STRING` | `creas_por_unidade` · `creas.peti_data_da_inclusao_no_peti` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_data_da_inclusao_no_peti` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Data da inclusão no PETI. |
| `creas.peti_demanda_inicial` | `STRING` | `creas_por_unidade` · `creas.peti_demanda_inicial` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_demanda_inicial` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Demanda Inicial. |
| `creas.peti_forma_de_acesso_informe_a_origem_de_acesso` | `STRING` | `creas_por_unidade` · `creas.peti_forma_de_acesso_informe_a_origem_de_acesso` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_forma_de_acesso_informe_a_origem_de_acesso` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Forma de acesso (informe a origem de acesso). |
| `creas.peti_marcacao_nos_campo_cadunico` | `STRING` | `creas_por_unidade` · `creas.peti_marcacao_nos_campo_cadunico` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_marcacao_nos_campo_cadunico` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Marcação nos campo CadÚnico. |
| `creas.peti_orientacoes_e_encaminhamentos` | `STRING` | `creas_por_unidade` · `creas.peti_orientacoes_e_encaminhamentos` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_orientacoes_e_encaminhamentos` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Orientações e Encaminhamentos. |
| `creas.peti_outras_demandas_vulnerabilidades` | `STRING` | `creas_por_unidade` · `creas.peti_outras_demandas_vulnerabilidades` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_outras_demandas_vulnerabilidades` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Outras demandas / vulnerabilidades. |
| `creas.peti_outras_formas_de_acesso` | `STRING` | `creas_por_unidade` · `creas.peti_outras_formas_de_acesso` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_outras_formas_de_acesso` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Outras formas de acesso. |
| `creas.peti_outros` | `STRING` | `creas_por_unidade` · `creas.peti_outros` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_outros` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Outros. |
| `creas.peti_tipo_de_trabalho_infantil` | `STRING` | `creas_por_unidade` · `creas.peti_tipo_de_trabalho_infantil` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_tipo_de_trabalho_infantil` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · Tipo de trabalho infantil. |
| `creas.peti_e_acompanhado_em_outro_creas` | `STRING` | `creas_por_unidade` · `creas.peti_e_acompanhado_em_outro_creas` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_e_acompanhado_em_outro_creas` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · É acompanhado em outro CREAS?. |
| `creas.peti_e_necessario_realizar_a_gestao_de_beneficios` | `STRING` | `creas_por_unidade` · `creas.peti_e_necessario_realizar_a_gestao_de_beneficios` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_e_necessario_realizar_a_gestao_de_beneficios` Catálogo CREAS · **CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil** · É necessário realizar a Gestão de Benefícios?. |
| `creas.scfv_data_ocorrencia` | `DATE` | `creas_por_unidade` · `creas.scfv_data_ocorrencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_data_ocorrencia` |
| `creas.scfv_data_registro` | `DATETIME` | `creas_por_unidade` · `creas.scfv_data_registro` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_data_registro` |
| `creas.scfv_id_unidade` | `INTEGER` | `creas_por_unidade` · `creas.scfv_id_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_id_unidade` |
| `creas.scfv_nome_unidade` | `STRING` | `creas_por_unidade` · `creas.scfv_nome_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_nome_unidade` |
| `creas.scfv_observacoes` | `STRING` | `creas_por_unidade` · `creas.scfv_observacoes` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_observacoes` |
| `creas.scfv_data_da_solicitacao_do_acolhimento` | `STRING` | `creas_por_unidade` · `creas.scfv_data_da_solicitacao_do_acolhimento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_data_da_solicitacao_do_acolhimento` Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Data da solicitação do acolhimento. |
| `creas.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` | `STRING` | `creas_por_unidade` · `creas.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Demanda de acolhimento institucional por decisão judicial?. |
| `creas.scfv_nome_do_cras_responsavel` | `STRING` | `creas_por_unidade` · `creas.scfv_nome_do_cras_responsavel` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_nome_do_cras_responsavel` Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Nome do CRAS responsável. |
| `creas.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` | `STRING` | `creas_por_unidade` · `creas.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · O usuário participa do SCFV (Serviço de Convivência e Fortalecimento de Vínculos)?. |
| `creas.scfv_oriundo_de_instituicao_de_acolhimento_interditado` | `STRING` | `creas_por_unidade` · `creas.scfv_oriundo_de_instituicao_de_acolhimento_interditado` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_oriundo_de_instituicao_de_acolhimento_interditado` Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Oriundo de instituição de acolhimento interditado?. |
| `creas.scfv_usuario_interditado` | `STRING` | `creas_por_unidade` · `creas.scfv_usuario_interditado` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_usuario_interditado` Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Usuário Interditado?. |
| `creas.scfv_orgao_de_origem_do_acolhimento` | `STRING` | `creas_por_unidade` · `creas.scfv_orgao_de_origem_do_acolhimento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.scfv_orgao_de_origem_do_acolhimento` Catálogo CREAS · **CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil** · Órgão de origem do acolhimento. |
| `creas.peti_desligamento_data_ocorrencia` | `DATE` | `creas_por_unidade` · `creas.peti_desligamento_data_ocorrencia` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_desligamento_data_ocorrencia` |
| `creas.peti_desligamento_data_registro` | `DATETIME` | `creas_por_unidade` · `creas.peti_desligamento_data_registro` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_desligamento_data_registro` |
| `creas.peti_desligamento_id_unidade` | `INTEGER` | `creas_por_unidade` · `creas.peti_desligamento_id_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_desligamento_id_unidade` |
| `creas.peti_desligamento_nome_unidade` | `STRING` | `creas_por_unidade` · `creas.peti_desligamento_nome_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_desligamento_nome_unidade` |
| `creas.peti_desligamento_observacoes` | `STRING` | `creas_por_unidade` · `creas.peti_desligamento_observacoes` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_desligamento_observacoes` |
| `creas.peti_desligamento_data_do_desligamento_do_peti` | `STRING` | `creas_por_unidade` · `creas.peti_desligamento_data_do_desligamento_do_peti` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_desligamento_data_do_desligamento_do_peti` Catálogo CREAS · **CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil** · Data do desligamento do PETI. |
| `creas.peti_desligamento_motivo_do_desligamento` | `STRING` | `creas_por_unidade` · `creas.peti_desligamento_motivo_do_desligamento` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_desligamento_motivo_do_desligamento` Catálogo CREAS · **CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil** · Motivo do desligamento. |
| `creas.peti_desligamento_outros` | `STRING` | `creas_por_unidade` · `creas.peti_desligamento_outros` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.peti_desligamento_outros` Catálogo CREAS · **CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil** · Outros. |
| `creas.passo_a_passo_ultima_atividade` | `STRING` | `creas_por_unidade` · `creas.passo_a_passo_ultima_atividade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.passo_a_passo_ultima_atividade` |
| `creas.passo_a_passo_ultima_data` | `DATE` | `creas_por_unidade` · `creas.passo_a_passo_ultima_data` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.passo_a_passo_ultima_data` |
| `creas.passo_a_passo_ultima_hora` | `STRING` | `creas_por_unidade` · `creas.passo_a_passo_ultima_hora` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.passo_a_passo_ultima_hora` |
| `creas.passo_a_passo_ultima_unidade` | `STRING` | `creas_por_unidade` · `creas.passo_a_passo_ultima_unidade` ← [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | `cr_reg.creas.passo_a_passo_ultima_unidade` |

### alta_complexidade

| Campo | Tipo | Origem na cadeia dbt | Transformação ou projeção |
| --- | --- | --- | --- |
| `alta_complexidade` | `STRUCT` | Projeção da mart | Struct com os campos detalhados abaixo. |
| `alta_complexidade.registros` | `ARRAY<STRUCT>` | CTE da mart | `coalesce(perfil_alta.registros, [])` |
| `alta_complexidade.registros.id_ciclo` | `INTEGER` | [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) · `historico.id_ciclo` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `alta_complexidade.registros.id_unidade` | `INTEGER` | [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) · `historico.id_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `alta_complexidade.registros.nome_unidade` | `STRING` | [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) · `historico.nome_unidade` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `alta_complexidade.registros.data_entrada` | `DATE` | [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) · `historico.data_entrada` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `alta_complexidade.registros.data_saida` | `DATE` | [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) · `historico.data_saida` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `alta_complexidade.registros.motivo_saida` | `STRING` | [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) · `historico.motivo_saida` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `alta_complexidade.registros.tempo_permanencia_dias` | `INTEGER` | [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) · `historico.tempo_permanencia_dias` | Campo repassado na estrutura de origem, conforme o fluxo do domínio. |
| `alta_complexidade.id_ciclo` | `INTEGER` | `acolhimentos_por_unidade` · `ultimo_ciclo.id_ciclo` ← [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) | `perfil_alta.ultimo_ciclo.id_ciclo` |
| `alta_complexidade.id_unidade` | `INTEGER` | `acolhimentos_por_unidade` · `ultimo_ciclo.id_unidade` ← [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) | `perfil_alta.ultimo_ciclo.id_unidade` |
| `alta_complexidade.nome_unidade` | `STRING` | `acolhimentos_por_unidade` · `ultimo_ciclo.nome_unidade` ← [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) | `perfil_alta.ultimo_ciclo.nome_unidade` |
| `alta_complexidade.data_entrada` | `DATE` | `acolhimentos_por_unidade` · `ultimo_ciclo.data_entrada` ← [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) | `perfil_alta.ultimo_ciclo.data_entrada` |
| `alta_complexidade.data_saida` | `DATE` | `acolhimentos_por_unidade` · `ultimo_ciclo.data_saida` ← [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) | `perfil_alta.ultimo_ciclo.data_saida` |
| `alta_complexidade.motivo_saida` | `STRING` | `acolhimentos_por_unidade` · `ultimo_ciclo.motivo_saida` ← [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) | `perfil_alta.ultimo_ciclo.motivo_saida` |
| `alta_complexidade.tempo_permanencia_dias` | `INTEGER` | `acolhimentos_por_unidade` · `ultimo_ciclo.tempo_permanencia_dias` ← [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) | `perfil_alta.ultimo_ciclo.tempo_permanencia_dias` |

## Modelos e fontes por domínio

As tabelas a seguir listam os caminhos físicos de cada fluxo. As dependências incluem tabelas usadas para filtros, chaves e enriquecimentos e também os modelos que fornecem as colunas. Os links abrem o código da transformação.

### Cadastro e chave

| Modelo, catálogo ou fonte | Dependência direta ou tabela física |
| --- | --- |
| [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) | [int_usuarios](../models/intermediate/core/int_usuarios.sql), [int_familias](../models/intermediate/core/int_familias.sql) |
| [int_usuarios_enderecos](../models/intermediate/core/int_usuarios_enderecos.sql) | [raw_atendimentos_programada](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_programada.sql), [raw_usuarios_enderecos](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_enderecos.sql), [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) |
| [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) | [int_unidades](../models/intermediate/core/int_unidades.sql) |
| [int_filtro_email_unidades](../models/intermediate/seguranca/int_filtro_email_unidades.sql) | `dashboard_acolherio.filtro_email_dev`, [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) |
| [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql) | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql) | [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql), [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) |
| [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql) | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) | [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql), [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql), [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql), [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) |
| [int_familias](../models/intermediate/core/int_familias.sql) | [raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql), [int_usuarios](../models/intermediate/core/int_usuarios.sql), [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [int_vulnerabilidades_agregadas](../models/intermediate/social/int_vulnerabilidades_agregadas.sql), [int_servicos_agregados](../models/intermediate/social/int_servicos_agregados.sql), [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) |
| [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) | [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) |
| [int_servicos_agregados](../models/intermediate/social/int_servicos_agregados.sql) | [raw_familias_servicos_assistenciais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_servicos_assistenciais.sql), [acolherio_dominios](../seeds/acolherio/acolherio_dominios.csv) |
| [int_unidades](../models/intermediate/core/int_unidades.sql) | [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql), [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql), [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql), [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) |
| [int_usuarios](../models/intermediate/core/int_usuarios.sql) | [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql), [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql), [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql), [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql), [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql), [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql), [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql), [int_usuarios_projetos_sociais](../models/intermediate/social/int_usuarios_projetos_sociais.sql), [acolherio_atividades_trabalho](../seeds/acolherio/acolherio_atividades_trabalho.csv) |
| [int_usuarios_projetos_sociais](../models/intermediate/social/int_usuarios_projetos_sociais.sql) | [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) |
| [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql) | [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql), [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) |
| [int_vulnerabilidades_agregadas](../models/intermediate/social/int_vulnerabilidades_agregadas.sql) | [raw_familias_vulnerabilidades](../models/raw/prontuario_carioca_assistencia_social/raw_familias_vulnerabilidades.sql) |
| [raw_atendimentos_programada](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_programada.sql) | `acolherio_referencia.atendimentos_programada` |
| [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) | `brutos_acolherio_staging.gh_ativgrp` |
| [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) | `brutos_acolherio_staging.gh_us_smas` |
| [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql) | `brutos_acolherio_staging.gh_us_config` |
| [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql) | `brutos_acolherio_staging.gh_evoluadm` |
| [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql) | `brutos_acolherio_staging.gh_evolufamil` |
| [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql) | `brutos_acolherio_staging.gh_evolugrupo` |
| [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql) | `brutos_acolherio_staging.gh_evolupac` |
| [raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql) | `brutos_acolherio_staging.gh_familias` |
| [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) | `brutos_acolherio_staging.gh_famil_projsociais` |
| [raw_familias_servicos_assistenciais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_servicos_assistenciais.sql) | `brutos_acolherio_staging.gh_famil_servassist` |
| [raw_familias_vulnerabilidades](../models/raw/prontuario_carioca_assistencia_social/raw_familias_vulnerabilidades.sql) | `brutos_acolherio_staging.gh_famil_vulnerab` |
| [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql) | `brutos_acolherio_staging.gh_familias_membros` |
| [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql) | `brutos_acolherio_staging.gh_origens` |
| [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) | `dashboard_acolherio.filtro_email_dev` |
| [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql) | `brutos_acolherio_staging.gh_us_tipo` |
| [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) | `brutos_acolherio_staging.gh_us` |
| [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) | `brutos_acolherio_staging.gh_cidadao_pac` |
| [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) | `brutos_acolherio_staging.gh_pac_dados` |
| [raw_usuarios_enderecos](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_enderecos.sql) | `brutos_acolherio_staging.gh_cidadao_pac` |
| [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) | `brutos_acolherio_staging.gh_pacientes_sm` |
| [acolherio_atividades_trabalho](../seeds/acolherio/acolherio_atividades_trabalho.csv) | Definição local do catálogo. |
| [acolherio_dominios](../seeds/acolherio/acolherio_dominios.csv) | Definição local do catálogo. |
| `acolherio_referencia.atendimentos_programada` | `rj-smas-dev.dashboard_acolherio.dev_atendimentos` |
| `brutos_acolherio_staging.gh_ativgrp` | `rj-smas.brutos_acolherio_staging.gh_ativgrp` |
| `brutos_acolherio_staging.gh_cidadao_pac` | `rj-smas.brutos_acolherio_staging.gh_cidadao_pac` |
| `brutos_acolherio_staging.gh_evoluadm` | `rj-smas.brutos_acolherio_staging.gh_evoluadm` |
| `brutos_acolherio_staging.gh_evolufamil` | `rj-smas.brutos_acolherio_staging.gh_evolufamil` |
| `brutos_acolherio_staging.gh_evolugrupo` | `rj-smas.brutos_acolherio_staging.gh_evolugrupo` |
| `brutos_acolherio_staging.gh_evolupac` | `rj-smas.brutos_acolherio_staging.gh_evolupac` |
| `brutos_acolherio_staging.gh_famil_projsociais` | `rj-smas.brutos_acolherio_staging.gh_famil_projsociais` |
| `brutos_acolherio_staging.gh_famil_servassist` | `rj-smas.brutos_acolherio_staging.gh_famil_servassist` |
| `brutos_acolherio_staging.gh_famil_vulnerab` | `rj-smas.brutos_acolherio_staging.gh_famil_vulnerab` |
| `brutos_acolherio_staging.gh_familias` | `rj-smas.brutos_acolherio_staging.gh_familias` |
| `brutos_acolherio_staging.gh_familias_membros` | `rj-smas.brutos_acolherio_staging.gh_familias_membros` |
| `brutos_acolherio_staging.gh_origens` | `rj-smas.brutos_acolherio_staging.gh_origens` |
| `brutos_acolherio_staging.gh_pac_dados` | `rj-smas.brutos_acolherio_staging.gh_pac_dados` |
| `brutos_acolherio_staging.gh_pacientes_sm` | `rj-smas.brutos_acolherio_staging.gh_pacientes_sm` |
| `brutos_acolherio_staging.gh_us` | `rj-smas.brutos_acolherio_staging.gh_us` |
| `brutos_acolherio_staging.gh_us_config` | `rj-smas.brutos_acolherio_staging.gh_us_config` |
| `brutos_acolherio_staging.gh_us_smas` | `rj-smas.brutos_acolherio_staging.gh_us_smas` |
| `brutos_acolherio_staging.gh_us_tipo` | `rj-smas.brutos_acolherio_staging.gh_us_tipo` |
| `dashboard_acolherio.filtro_email_dev` | `rj-smas-dev.dashboard_acolherio.filtro_email_dev` |

### Famílias e projetos

| Modelo, catálogo ou fonte | Dependência direta ou tabela física |
| --- | --- |
| [int_usuarios_familias](../models/intermediate/social/int_usuarios_familias.sql) | [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [raw_familias_motivos_saida](../models/raw/prontuario_carioca_assistencia_social/raw_familias_motivos_saida.sql), [raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql), [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql), [int_servicos_agregados](../models/intermediate/social/int_servicos_agregados.sql), [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) |
| [int_projetos_sociais_por_unidade](../models/intermediate/social/int_projetos_sociais_por_unidade.sql) | [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) |
| [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) | [int_unidades](../models/intermediate/core/int_unidades.sql) |
| [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) | [int_usuarios](../models/intermediate/core/int_usuarios.sql), [int_familias](../models/intermediate/core/int_familias.sql) |
| [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql) | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql) | [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql), [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) |
| [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql) | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) | [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql), [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql), [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql), [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) |
| [int_familias](../models/intermediate/core/int_familias.sql) | [raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql), [int_usuarios](../models/intermediate/core/int_usuarios.sql), [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [int_vulnerabilidades_agregadas](../models/intermediate/social/int_vulnerabilidades_agregadas.sql), [int_servicos_agregados](../models/intermediate/social/int_servicos_agregados.sql), [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) |
| [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) | [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) |
| [int_servicos_agregados](../models/intermediate/social/int_servicos_agregados.sql) | [raw_familias_servicos_assistenciais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_servicos_assistenciais.sql), [acolherio_dominios](../seeds/acolherio/acolherio_dominios.csv) |
| [int_unidades](../models/intermediate/core/int_unidades.sql) | [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql), [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql), [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql), [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) |
| [int_usuarios](../models/intermediate/core/int_usuarios.sql) | [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql), [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql), [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql), [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql), [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql), [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql), [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql), [int_usuarios_projetos_sociais](../models/intermediate/social/int_usuarios_projetos_sociais.sql), [acolherio_atividades_trabalho](../seeds/acolherio/acolherio_atividades_trabalho.csv) |
| [int_usuarios_projetos_sociais](../models/intermediate/social/int_usuarios_projetos_sociais.sql) | [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) |
| [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql) | [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql), [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) |
| [int_vulnerabilidades_agregadas](../models/intermediate/social/int_vulnerabilidades_agregadas.sql) | [raw_familias_vulnerabilidades](../models/raw/prontuario_carioca_assistencia_social/raw_familias_vulnerabilidades.sql) |
| [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) | `brutos_acolherio_staging.gh_ativgrp` |
| [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) | `brutos_acolherio_staging.gh_us_smas` |
| [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql) | `brutos_acolherio_staging.gh_us_config` |
| [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql) | `brutos_acolherio_staging.gh_evoluadm` |
| [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql) | `brutos_acolherio_staging.gh_evolufamil` |
| [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql) | `brutos_acolherio_staging.gh_evolugrupo` |
| [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql) | `brutos_acolherio_staging.gh_evolupac` |
| [raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql) | `brutos_acolherio_staging.gh_familias` |
| [raw_familias_motivos_saida](../models/raw/prontuario_carioca_assistencia_social/raw_familias_motivos_saida.sql) | `brutos_acolherio_staging.gh_familias_motiv_saida` |
| [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) | `brutos_acolherio_staging.gh_famil_projsociais` |
| [raw_familias_servicos_assistenciais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_servicos_assistenciais.sql) | `brutos_acolherio_staging.gh_famil_servassist` |
| [raw_familias_vulnerabilidades](../models/raw/prontuario_carioca_assistencia_social/raw_familias_vulnerabilidades.sql) | `brutos_acolherio_staging.gh_famil_vulnerab` |
| [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql) | `brutos_acolherio_staging.gh_familias_membros` |
| [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql) | `brutos_acolherio_staging.gh_origens` |
| [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) | `dashboard_acolherio.filtro_email_dev` |
| [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql) | `brutos_acolherio_staging.gh_us_tipo` |
| [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) | `brutos_acolherio_staging.gh_us` |
| [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) | `brutos_acolherio_staging.gh_cidadao_pac` |
| [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) | `brutos_acolherio_staging.gh_pac_dados` |
| [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) | `brutos_acolherio_staging.gh_pacientes_sm` |
| [acolherio_atividades_trabalho](../seeds/acolherio/acolherio_atividades_trabalho.csv) | Definição local do catálogo. |
| [acolherio_dominios](../seeds/acolherio/acolherio_dominios.csv) | Definição local do catálogo. |
| `brutos_acolherio_staging.gh_ativgrp` | `rj-smas.brutos_acolherio_staging.gh_ativgrp` |
| `brutos_acolherio_staging.gh_cidadao_pac` | `rj-smas.brutos_acolherio_staging.gh_cidadao_pac` |
| `brutos_acolherio_staging.gh_evoluadm` | `rj-smas.brutos_acolherio_staging.gh_evoluadm` |
| `brutos_acolherio_staging.gh_evolufamil` | `rj-smas.brutos_acolherio_staging.gh_evolufamil` |
| `brutos_acolherio_staging.gh_evolugrupo` | `rj-smas.brutos_acolherio_staging.gh_evolugrupo` |
| `brutos_acolherio_staging.gh_evolupac` | `rj-smas.brutos_acolherio_staging.gh_evolupac` |
| `brutos_acolherio_staging.gh_famil_projsociais` | `rj-smas.brutos_acolherio_staging.gh_famil_projsociais` |
| `brutos_acolherio_staging.gh_famil_servassist` | `rj-smas.brutos_acolherio_staging.gh_famil_servassist` |
| `brutos_acolherio_staging.gh_famil_vulnerab` | `rj-smas.brutos_acolherio_staging.gh_famil_vulnerab` |
| `brutos_acolherio_staging.gh_familias` | `rj-smas.brutos_acolherio_staging.gh_familias` |
| `brutos_acolherio_staging.gh_familias_membros` | `rj-smas.brutos_acolherio_staging.gh_familias_membros` |
| `brutos_acolherio_staging.gh_familias_motiv_saida` | `rj-smas.brutos_acolherio_staging.gh_familias_motiv_saida` |
| `brutos_acolherio_staging.gh_origens` | `rj-smas.brutos_acolherio_staging.gh_origens` |
| `brutos_acolherio_staging.gh_pac_dados` | `rj-smas.brutos_acolherio_staging.gh_pac_dados` |
| `brutos_acolherio_staging.gh_pacientes_sm` | `rj-smas.brutos_acolherio_staging.gh_pacientes_sm` |
| `brutos_acolherio_staging.gh_us` | `rj-smas.brutos_acolherio_staging.gh_us` |
| `brutos_acolherio_staging.gh_us_config` | `rj-smas.brutos_acolherio_staging.gh_us_config` |
| `brutos_acolherio_staging.gh_us_smas` | `rj-smas.brutos_acolherio_staging.gh_us_smas` |
| `brutos_acolherio_staging.gh_us_tipo` | `rj-smas.brutos_acolherio_staging.gh_us_tipo` |
| `dashboard_acolherio.filtro_email_dev` | `rj-smas-dev.dashboard_acolherio.filtro_email_dev` |

### Atendimentos

| Modelo, catálogo ou fonte | Dependência direta ou tabela física |
| --- | --- |
| [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `dashboard_acolherio.filtro_email_dev`, [raw_atendimentos_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_usuarios.sql), [raw_atendimentos_familias](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_familias.sql), [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql), [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql), [raw_atendimentos_programada](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_programada.sql), [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql), [int_datas_atendimento](../models/intermediate/atendimentos/int_datas_atendimento.sql) |
| [int_datas_atendimento](../models/intermediate/atendimentos/int_datas_atendimento.sql) | [raw_atendimentos_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_usuarios.sql), [raw_atendimentos_familias](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_familias.sql) |
| [raw_atendimentos_familias](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_familias.sql) | `brutos_acolherio_staging.gh_atend_familia` |
| [raw_atendimentos_programada](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_programada.sql) | `acolherio_referencia.atendimentos_programada` |
| [raw_atendimentos_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_usuarios.sql) | `brutos_acolherio_staging.gh_atendimentos` |
| [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql) | `brutos_acolherio_staging.gh_cbo` |
| [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql) | `brutos_acolherio_staging.gh_profocup` |
| [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) | `brutos_acolherio_staging.gh_us` |
| `acolherio_referencia.atendimentos_programada` | `rj-smas-dev.dashboard_acolherio.dev_atendimentos` |
| `brutos_acolherio_staging.gh_atend_familia` | `rj-smas.brutos_acolherio_staging.gh_atend_familia` |
| `brutos_acolherio_staging.gh_atendimentos` | `rj-smas.brutos_acolherio_staging.gh_atendimentos` |
| `brutos_acolherio_staging.gh_cbo` | `rj-smas.brutos_acolherio_staging.gh_cbo` |
| `brutos_acolherio_staging.gh_profocup` | `rj-smas.brutos_acolherio_staging.gh_profocup` |
| `brutos_acolherio_staging.gh_us` | `rj-smas.brutos_acolherio_staging.gh_us` |
| `dashboard_acolherio.filtro_email_dev` | `rj-smas-dev.dashboard_acolherio.filtro_email_dev` |

### Evoluções e formulários

| Modelo, catálogo ou fonte | Dependência direta ou tabela física |
| --- | --- |
| [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | [int_evolucoes](../models/intermediate/core/int_evolucoes.sql), [dim_profissionais](../models/marts/core/dimensions/dim_profissionais.sql), [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql), [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) |
| [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql), [formularios_datas_ocorrencia](../seeds/acolherio/formularios_datas_ocorrencia.csv) |
| [dim_profissionais](../models/marts/core/dimensions/dim_profissionais.sql) | [int_profissionais](../models/intermediate/core/int_profissionais.sql) |
| [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) | [int_unidades](../models/intermediate/core/int_unidades.sql) |
| [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql) | [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql), [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) |
| [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) | [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql), [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql), [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql), [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) |
| [int_profissionais](../models/intermediate/core/int_profissionais.sql) | [raw_profissionais](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais.sql), [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql), [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql), [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql), [raw_operadores_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_operadores_unidades.sql), [int_unidades](../models/intermediate/core/int_unidades.sql), [raw_contas_modulos](../models/raw/prontuario_carioca_assistencia_social/raw_contas_modulos.sql), [raw_perfis_grupos](../models/raw/prontuario_carioca_assistencia_social/raw_perfis_grupos.sql) |
| [int_unidades](../models/intermediate/core/int_unidades.sql) | [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql), [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql), [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql), [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) |
| [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) | `brutos_acolherio_staging.gh_ativgrp` |
| [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) | `brutos_acolherio_staging.gh_us_smas` |
| [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql) | `brutos_acolherio_staging.gh_cbo` |
| [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql) | `brutos_acolherio_staging.gh_us_config` |
| [raw_contas_modulos](../models/raw/prontuario_carioca_assistencia_social/raw_contas_modulos.sql) | `brutos_acolherio_staging.gh_contas_modulos` |
| [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql) | `brutos_acolherio_staging.gh_evoluadm` |
| [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql) | `brutos_acolherio_staging.gh_evolufamil` |
| [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql) | `brutos_acolherio_staging.gh_evolugrupo` |
| [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql) | `brutos_acolherio_staging.gh_evolupac` |
| [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql) | `brutos_acolherio_staging.gh_contas` |
| [raw_operadores_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_operadores_unidades.sql) | `brutos_acolherio_staging.gh_contas_us` |
| [raw_perfis_grupos](../models/raw/prontuario_carioca_assistencia_social/raw_perfis_grupos.sql) | `brutos_acolherio_staging.gh_perfil_grupos` |
| [raw_profissionais](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais.sql) | `brutos_acolherio_staging.gh_prof` |
| [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql) | `brutos_acolherio_staging.gh_profocup` |
| [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) | `dashboard_acolherio.filtro_email_dev` |
| [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql) | `brutos_acolherio_staging.gh_us_tipo` |
| [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) | `brutos_acolherio_staging.gh_us` |
| [formularios_datas_ocorrencia](../seeds/acolherio/formularios_datas_ocorrencia.csv) | Definição local do catálogo. |
| `brutos_acolherio_staging.gh_ativgrp` | `rj-smas.brutos_acolherio_staging.gh_ativgrp` |
| `brutos_acolherio_staging.gh_cbo` | `rj-smas.brutos_acolherio_staging.gh_cbo` |
| `brutos_acolherio_staging.gh_contas` | `rj-smas.brutos_acolherio_staging.gh_contas` |
| `brutos_acolherio_staging.gh_contas_modulos` | `rj-smas.brutos_acolherio_staging.gh_contas_modulos` |
| `brutos_acolherio_staging.gh_contas_us` | `rj-smas.brutos_acolherio_staging.gh_contas_us` |
| `brutos_acolherio_staging.gh_evoluadm` | `rj-smas.brutos_acolherio_staging.gh_evoluadm` |
| `brutos_acolherio_staging.gh_evolufamil` | `rj-smas.brutos_acolherio_staging.gh_evolufamil` |
| `brutos_acolherio_staging.gh_evolugrupo` | `rj-smas.brutos_acolherio_staging.gh_evolugrupo` |
| `brutos_acolherio_staging.gh_evolupac` | `rj-smas.brutos_acolherio_staging.gh_evolupac` |
| `brutos_acolherio_staging.gh_perfil_grupos` | `rj-smas.brutos_acolherio_staging.gh_perfil_grupos` |
| `brutos_acolherio_staging.gh_prof` | `rj-smas.brutos_acolherio_staging.gh_prof` |
| `brutos_acolherio_staging.gh_profocup` | `rj-smas.brutos_acolherio_staging.gh_profocup` |
| `brutos_acolherio_staging.gh_us` | `rj-smas.brutos_acolherio_staging.gh_us` |
| `brutos_acolherio_staging.gh_us_config` | `rj-smas.brutos_acolherio_staging.gh_us_config` |
| `brutos_acolherio_staging.gh_us_smas` | `rj-smas.brutos_acolherio_staging.gh_us_smas` |
| `brutos_acolherio_staging.gh_us_tipo` | `rj-smas.brutos_acolherio_staging.gh_us_tipo` |
| `dashboard_acolherio.filtro_email_dev` | `rj-smas-dev.dashboard_acolherio.filtro_email_dev` |

### Centro POP

| Modelo, catálogo ou fonte | Dependência direta ou tabela física |
| --- | --- |
| [int_perfil_centro_pop_por_unidade](../models/intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql) | [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql), [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) |
| [int_centro_pop_historico_mensal](../models/intermediate/centro_pop/int_centro_pop_historico_mensal.sql) | [int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql) |
| [dim_profissionais](../models/marts/core/dimensions/dim_profissionais.sql) | [int_profissionais](../models/intermediate/core/int_profissionais.sql) |
| [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) | [int_unidades](../models/intermediate/core/int_unidades.sql) |
| [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql) | [int_usuarios](../models/intermediate/core/int_usuarios.sql), [int_familias](../models/intermediate/core/int_familias.sql) |
| [fct_presencas_usuarios](../models/marts/core/facts/fct_presencas_usuarios.sql) | [int_presencas_usuarios](../models/intermediate/core/int_presencas_usuarios.sql) |
| [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) | `dashboard_acolherio.filtro_email_dev`, [raw_atendimentos_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_usuarios.sql), [raw_atendimentos_familias](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_familias.sql), [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql), [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql), [raw_atendimentos_programada](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_programada.sql), [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql), [int_datas_atendimento](../models/intermediate/atendimentos/int_datas_atendimento.sql) |
| [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql) | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql) | [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql), [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) |
| [int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql) | `brutos_acolherio_staging.gh_atendimentos`, `brutos_acolherio_staging.gh_atend_familia`, `brutos_acolherio_staging.gh_evolupac`, `brutos_acolherio_staging.gh_evolufamil`, `brutos_acolherio_staging.gh_ativ_paciente`, [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql), [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql), [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql), [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql), [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql), [dim_profissionais](../models/marts/core/dimensions/dim_profissionais.sql), [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql) |
| [int_datas_atendimento](../models/intermediate/atendimentos/int_datas_atendimento.sql) | [raw_atendimentos_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_usuarios.sql), [raw_atendimentos_familias](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_familias.sql) |
| [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql) | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) | [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql), [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql), [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql), [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) |
| [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | [int_evolucoes](../models/intermediate/core/int_evolucoes.sql), [dim_profissionais](../models/marts/core/dimensions/dim_profissionais.sql), [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql), [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) |
| [int_familias](../models/intermediate/core/int_familias.sql) | [raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql), [int_usuarios](../models/intermediate/core/int_usuarios.sql), [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [int_vulnerabilidades_agregadas](../models/intermediate/social/int_vulnerabilidades_agregadas.sql), [int_servicos_agregados](../models/intermediate/social/int_servicos_agregados.sql), [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) |
| [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) | [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) |
| [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql), [formularios_datas_ocorrencia](../seeds/acolherio/formularios_datas_ocorrencia.csv) |
| [int_presencas_usuarios](../models/intermediate/core/int_presencas_usuarios.sql) | [raw_presencas_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_presencas_usuarios.sql), [int_usuarios](../models/intermediate/core/int_usuarios.sql) |
| [int_profissionais](../models/intermediate/core/int_profissionais.sql) | [raw_profissionais](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais.sql), [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql), [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql), [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql), [raw_operadores_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_operadores_unidades.sql), [int_unidades](../models/intermediate/core/int_unidades.sql), [raw_contas_modulos](../models/raw/prontuario_carioca_assistencia_social/raw_contas_modulos.sql), [raw_perfis_grupos](../models/raw/prontuario_carioca_assistencia_social/raw_perfis_grupos.sql) |
| [int_registros_centro_pop](../models/intermediate/centro_pop/int_registros_centro_pop.sql) | [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql), [fct_presencas_usuarios](../models/marts/core/facts/fct_presencas_usuarios.sql), [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql), [dim_usuarios](../models/marts/core/dimensions/dim_usuarios.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql), [int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql), [raw_evolucoes_questionario](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_questionario.sql), [raw_evolucoes_questionario_lista](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_questionario_lista.sql) |
| [int_servicos_agregados](../models/intermediate/social/int_servicos_agregados.sql) | [raw_familias_servicos_assistenciais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_servicos_assistenciais.sql), [acolherio_dominios](../seeds/acolherio/acolherio_dominios.csv) |
| [int_unidades](../models/intermediate/core/int_unidades.sql) | [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql), [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql), [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql), [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) |
| [int_usuarios](../models/intermediate/core/int_usuarios.sql) | [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql), [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql), [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql), [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql), [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql), [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql), [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql), [int_usuarios_projetos_sociais](../models/intermediate/social/int_usuarios_projetos_sociais.sql), [acolherio_atividades_trabalho](../seeds/acolherio/acolherio_atividades_trabalho.csv) |
| [int_usuarios_projetos_sociais](../models/intermediate/social/int_usuarios_projetos_sociais.sql) | [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) |
| [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql) | [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql), [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) |
| [int_vulnerabilidades_agregadas](../models/intermediate/social/int_vulnerabilidades_agregadas.sql) | [raw_familias_vulnerabilidades](../models/raw/prontuario_carioca_assistencia_social/raw_familias_vulnerabilidades.sql) |
| [raw_atendimentos_familias](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_familias.sql) | `brutos_acolherio_staging.gh_atend_familia` |
| [raw_atendimentos_programada](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_programada.sql) | `acolherio_referencia.atendimentos_programada` |
| [raw_atendimentos_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_usuarios.sql) | `brutos_acolherio_staging.gh_atendimentos` |
| [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) | `brutos_acolherio_staging.gh_ativgrp` |
| [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) | `brutos_acolherio_staging.gh_us_smas` |
| [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql) | `brutos_acolherio_staging.gh_cbo` |
| [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql) | `brutos_acolherio_staging.gh_us_config` |
| [raw_contas_modulos](../models/raw/prontuario_carioca_assistencia_social/raw_contas_modulos.sql) | `brutos_acolherio_staging.gh_contas_modulos` |
| [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql) | `brutos_acolherio_staging.gh_evoluadm` |
| [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql) | `brutos_acolherio_staging.gh_evolufamil` |
| [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql) | `brutos_acolherio_staging.gh_evolugrupo` |
| [raw_evolucoes_questionario](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_questionario.sql) | `brutos_acolherio_staging.gh_regeatv_evol_quest` |
| [raw_evolucoes_questionario_lista](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_questionario_lista.sql) | `brutos_acolherio_staging.gh_regeatv_evol_quest_list` |
| [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql) | `brutos_acolherio_staging.gh_evolupac` |
| [raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql) | `brutos_acolherio_staging.gh_familias` |
| [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) | `brutos_acolherio_staging.gh_famil_projsociais` |
| [raw_familias_servicos_assistenciais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_servicos_assistenciais.sql) | `brutos_acolherio_staging.gh_famil_servassist` |
| [raw_familias_vulnerabilidades](../models/raw/prontuario_carioca_assistencia_social/raw_familias_vulnerabilidades.sql) | `brutos_acolherio_staging.gh_famil_vulnerab` |
| [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql) | `brutos_acolherio_staging.gh_familias_membros` |
| [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql) | `brutos_acolherio_staging.gh_contas` |
| [raw_operadores_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_operadores_unidades.sql) | `brutos_acolherio_staging.gh_contas_us` |
| [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql) | `brutos_acolherio_staging.gh_origens` |
| [raw_perfis_grupos](../models/raw/prontuario_carioca_assistencia_social/raw_perfis_grupos.sql) | `brutos_acolherio_staging.gh_perfil_grupos` |
| [raw_presencas_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_presencas_usuarios.sql) | `brutos_acolherio_staging.gh_ativ_paciente` |
| [raw_profissionais](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais.sql) | `brutos_acolherio_staging.gh_prof` |
| [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql) | `brutos_acolherio_staging.gh_profocup` |
| [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) | `dashboard_acolherio.filtro_email_dev` |
| [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql) | `brutos_acolherio_staging.gh_us_tipo` |
| [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) | `brutos_acolherio_staging.gh_us` |
| [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) | `brutos_acolherio_staging.gh_cidadao_pac` |
| [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) | `brutos_acolherio_staging.gh_pac_dados` |
| [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) | `brutos_acolherio_staging.gh_pacientes_sm` |
| [acolherio_atividades_trabalho](../seeds/acolherio/acolherio_atividades_trabalho.csv) | Definição local do catálogo. |
| [acolherio_dominios](../seeds/acolherio/acolherio_dominios.csv) | Definição local do catálogo. |
| [formularios_datas_ocorrencia](../seeds/acolherio/formularios_datas_ocorrencia.csv) | Definição local do catálogo. |
| `acolherio_referencia.atendimentos_programada` | `rj-smas-dev.dashboard_acolherio.dev_atendimentos` |
| `brutos_acolherio_staging.gh_atend_familia` | `rj-smas.brutos_acolherio_staging.gh_atend_familia` |
| `brutos_acolherio_staging.gh_atendimentos` | `rj-smas.brutos_acolherio_staging.gh_atendimentos` |
| `brutos_acolherio_staging.gh_ativ_paciente` | `rj-smas.brutos_acolherio_staging.gh_ativ_paciente` |
| `brutos_acolherio_staging.gh_ativgrp` | `rj-smas.brutos_acolherio_staging.gh_ativgrp` |
| `brutos_acolherio_staging.gh_cbo` | `rj-smas.brutos_acolherio_staging.gh_cbo` |
| `brutos_acolherio_staging.gh_cidadao_pac` | `rj-smas.brutos_acolherio_staging.gh_cidadao_pac` |
| `brutos_acolherio_staging.gh_contas` | `rj-smas.brutos_acolherio_staging.gh_contas` |
| `brutos_acolherio_staging.gh_contas_modulos` | `rj-smas.brutos_acolherio_staging.gh_contas_modulos` |
| `brutos_acolherio_staging.gh_contas_us` | `rj-smas.brutos_acolherio_staging.gh_contas_us` |
| `brutos_acolherio_staging.gh_evoluadm` | `rj-smas.brutos_acolherio_staging.gh_evoluadm` |
| `brutos_acolherio_staging.gh_evolufamil` | `rj-smas.brutos_acolherio_staging.gh_evolufamil` |
| `brutos_acolherio_staging.gh_evolugrupo` | `rj-smas.brutos_acolherio_staging.gh_evolugrupo` |
| `brutos_acolherio_staging.gh_evolupac` | `rj-smas.brutos_acolherio_staging.gh_evolupac` |
| `brutos_acolherio_staging.gh_famil_projsociais` | `rj-smas.brutos_acolherio_staging.gh_famil_projsociais` |
| `brutos_acolherio_staging.gh_famil_servassist` | `rj-smas.brutos_acolherio_staging.gh_famil_servassist` |
| `brutos_acolherio_staging.gh_famil_vulnerab` | `rj-smas.brutos_acolherio_staging.gh_famil_vulnerab` |
| `brutos_acolherio_staging.gh_familias` | `rj-smas.brutos_acolherio_staging.gh_familias` |
| `brutos_acolherio_staging.gh_familias_membros` | `rj-smas.brutos_acolherio_staging.gh_familias_membros` |
| `brutos_acolherio_staging.gh_origens` | `rj-smas.brutos_acolherio_staging.gh_origens` |
| `brutos_acolherio_staging.gh_pac_dados` | `rj-smas.brutos_acolherio_staging.gh_pac_dados` |
| `brutos_acolherio_staging.gh_pacientes_sm` | `rj-smas.brutos_acolherio_staging.gh_pacientes_sm` |
| `brutos_acolherio_staging.gh_perfil_grupos` | `rj-smas.brutos_acolherio_staging.gh_perfil_grupos` |
| `brutos_acolherio_staging.gh_prof` | `rj-smas.brutos_acolherio_staging.gh_prof` |
| `brutos_acolherio_staging.gh_profocup` | `rj-smas.brutos_acolherio_staging.gh_profocup` |
| `brutos_acolherio_staging.gh_regeatv_evol_quest` | `rj-smas.brutos_acolherio_staging.gh_regeatv_evol_quest` |
| `brutos_acolherio_staging.gh_regeatv_evol_quest_list` | `rj-smas.brutos_acolherio_staging.gh_regeatv_evol_quest_list` |
| `brutos_acolherio_staging.gh_us` | `rj-smas.brutos_acolherio_staging.gh_us` |
| `brutos_acolherio_staging.gh_us_config` | `rj-smas.brutos_acolherio_staging.gh_us_config` |
| `brutos_acolherio_staging.gh_us_smas` | `rj-smas.brutos_acolherio_staging.gh_us_smas` |
| `brutos_acolherio_staging.gh_us_tipo` | `rj-smas.brutos_acolherio_staging.gh_us_tipo` |
| `dashboard_acolherio.filtro_email_dev` | `rj-smas-dev.dashboard_acolherio.filtro_email_dev` |

### CREAS

| Modelo, catálogo ou fonte | Dependência direta ou tabela física |
| --- | --- |
| [int_perfil_creas_por_unidade](../models/intermediate/creas/int_perfil_creas_por_unidade.sql) | [int_perfil_creas](../models/intermediate/creas/int_perfil_creas.sql), [creas_campos](../seeds/acolherio/creas_campos.csv), [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) |
| [dim_profissionais](../models/marts/core/dimensions/dim_profissionais.sql) | [int_profissionais](../models/intermediate/core/int_profissionais.sql) |
| [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) | [int_unidades](../models/intermediate/core/int_unidades.sql) |
| [fct_presencas_usuarios](../models/marts/core/facts/fct_presencas_usuarios.sql) | [int_presencas_usuarios](../models/intermediate/core/int_presencas_usuarios.sql) |
| [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql) | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql) | [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql), [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) |
| [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql) | [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) |
| [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) | [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql), [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql), [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql), [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) |
| [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql) | [int_evolucoes](../models/intermediate/core/int_evolucoes.sql), [dim_profissionais](../models/marts/core/dimensions/dim_profissionais.sql), [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql), [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) |
| [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) | [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) |
| [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql) | [int_evolucoes_por_usuario](../models/intermediate/core/int_evolucoes_por_usuario.sql), [formularios_datas_ocorrencia](../seeds/acolherio/formularios_datas_ocorrencia.csv) |
| [int_perfil_creas](../models/intermediate/creas/int_perfil_creas.sql) | [int_formularios_por_usuario](../models/intermediate/formularios/int_formularios_por_usuario.sql), [fct_presencas_usuarios](../models/marts/core/facts/fct_presencas_usuarios.sql), [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) |
| [int_presencas_usuarios](../models/intermediate/core/int_presencas_usuarios.sql) | [raw_presencas_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_presencas_usuarios.sql), [int_usuarios](../models/intermediate/core/int_usuarios.sql) |
| [int_profissionais](../models/intermediate/core/int_profissionais.sql) | [raw_profissionais](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais.sql), [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql), [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql), [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql), [raw_operadores_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_operadores_unidades.sql), [int_unidades](../models/intermediate/core/int_unidades.sql), [raw_contas_modulos](../models/raw/prontuario_carioca_assistencia_social/raw_contas_modulos.sql), [raw_perfis_grupos](../models/raw/prontuario_carioca_assistencia_social/raw_perfis_grupos.sql) |
| [int_unidades](../models/intermediate/core/int_unidades.sql) | [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql), [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql), [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql), [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) |
| [int_usuarios](../models/intermediate/core/int_usuarios.sql) | [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql), [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql), [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql), [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql), [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql), [int_deficiencias_agregadas](../models/intermediate/social/int_deficiencias_agregadas.sql), [int_beneficios_agregados](../models/intermediate/social/int_beneficios_agregados.sql), [int_usuarios_projetos_sociais](../models/intermediate/social/int_usuarios_projetos_sociais.sql), [acolherio_atividades_trabalho](../seeds/acolherio/acolherio_atividades_trabalho.csv) |
| [int_usuarios_projetos_sociais](../models/intermediate/social/int_usuarios_projetos_sociais.sql) | [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql), [int_familias_projetos_sociais](../models/intermediate/social/int_familias_projetos_sociais.sql) |
| [int_usuarios_violacoes](../models/intermediate/social/int_usuarios_violacoes.sql) | [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql), [int_evolucoes](../models/intermediate/core/int_evolucoes.sql) |
| [raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql) | `brutos_acolherio_staging.gh_ativgrp` |
| [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) | `brutos_acolherio_staging.gh_us_smas` |
| [raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql) | `brutos_acolherio_staging.gh_cbo` |
| [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql) | `brutos_acolherio_staging.gh_us_config` |
| [raw_contas_modulos](../models/raw/prontuario_carioca_assistencia_social/raw_contas_modulos.sql) | `brutos_acolherio_staging.gh_contas_modulos` |
| [raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql) | `brutos_acolherio_staging.gh_evoluadm` |
| [raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql) | `brutos_acolherio_staging.gh_evolufamil` |
| [raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql) | `brutos_acolherio_staging.gh_evolugrupo` |
| [raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql) | `brutos_acolherio_staging.gh_evolupac` |
| [raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql) | `brutos_acolherio_staging.gh_famil_projsociais` |
| [raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql) | `brutos_acolherio_staging.gh_familias_membros` |
| [raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql) | `brutos_acolherio_staging.gh_contas` |
| [raw_operadores_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_operadores_unidades.sql) | `brutos_acolherio_staging.gh_contas_us` |
| [raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql) | `brutos_acolherio_staging.gh_origens` |
| [raw_perfis_grupos](../models/raw/prontuario_carioca_assistencia_social/raw_perfis_grupos.sql) | `brutos_acolherio_staging.gh_perfil_grupos` |
| [raw_presencas_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_presencas_usuarios.sql) | `brutos_acolherio_staging.gh_ativ_paciente` |
| [raw_profissionais](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais.sql) | `brutos_acolherio_staging.gh_prof` |
| [raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql) | `brutos_acolherio_staging.gh_profocup` |
| [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) | `dashboard_acolherio.filtro_email_dev` |
| [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql) | `brutos_acolherio_staging.gh_us_tipo` |
| [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) | `brutos_acolherio_staging.gh_us` |
| [raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql) | `brutos_acolherio_staging.gh_cidadao_pac` |
| [raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql) | `brutos_acolherio_staging.gh_pac_dados` |
| [raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql) | `brutos_acolherio_staging.gh_pacientes_sm` |
| [acolherio_atividades_trabalho](../seeds/acolherio/acolherio_atividades_trabalho.csv) | Definição local do catálogo. |
| [creas_campos](../seeds/acolherio/creas_campos.csv) | Definição local do catálogo. |
| [formularios_datas_ocorrencia](../seeds/acolherio/formularios_datas_ocorrencia.csv) | Definição local do catálogo. |
| `brutos_acolherio_staging.gh_ativ_paciente` | `rj-smas.brutos_acolherio_staging.gh_ativ_paciente` |
| `brutos_acolherio_staging.gh_ativgrp` | `rj-smas.brutos_acolherio_staging.gh_ativgrp` |
| `brutos_acolherio_staging.gh_cbo` | `rj-smas.brutos_acolherio_staging.gh_cbo` |
| `brutos_acolherio_staging.gh_cidadao_pac` | `rj-smas.brutos_acolherio_staging.gh_cidadao_pac` |
| `brutos_acolherio_staging.gh_contas` | `rj-smas.brutos_acolherio_staging.gh_contas` |
| `brutos_acolherio_staging.gh_contas_modulos` | `rj-smas.brutos_acolherio_staging.gh_contas_modulos` |
| `brutos_acolherio_staging.gh_contas_us` | `rj-smas.brutos_acolherio_staging.gh_contas_us` |
| `brutos_acolherio_staging.gh_evoluadm` | `rj-smas.brutos_acolherio_staging.gh_evoluadm` |
| `brutos_acolherio_staging.gh_evolufamil` | `rj-smas.brutos_acolherio_staging.gh_evolufamil` |
| `brutos_acolherio_staging.gh_evolugrupo` | `rj-smas.brutos_acolherio_staging.gh_evolugrupo` |
| `brutos_acolherio_staging.gh_evolupac` | `rj-smas.brutos_acolherio_staging.gh_evolupac` |
| `brutos_acolherio_staging.gh_famil_projsociais` | `rj-smas.brutos_acolherio_staging.gh_famil_projsociais` |
| `brutos_acolherio_staging.gh_familias_membros` | `rj-smas.brutos_acolherio_staging.gh_familias_membros` |
| `brutos_acolherio_staging.gh_origens` | `rj-smas.brutos_acolherio_staging.gh_origens` |
| `brutos_acolherio_staging.gh_pac_dados` | `rj-smas.brutos_acolherio_staging.gh_pac_dados` |
| `brutos_acolherio_staging.gh_pacientes_sm` | `rj-smas.brutos_acolherio_staging.gh_pacientes_sm` |
| `brutos_acolherio_staging.gh_perfil_grupos` | `rj-smas.brutos_acolherio_staging.gh_perfil_grupos` |
| `brutos_acolherio_staging.gh_prof` | `rj-smas.brutos_acolherio_staging.gh_prof` |
| `brutos_acolherio_staging.gh_profocup` | `rj-smas.brutos_acolherio_staging.gh_profocup` |
| `brutos_acolherio_staging.gh_us` | `rj-smas.brutos_acolherio_staging.gh_us` |
| `brutos_acolherio_staging.gh_us_config` | `rj-smas.brutos_acolherio_staging.gh_us_config` |
| `brutos_acolherio_staging.gh_us_smas` | `rj-smas.brutos_acolherio_staging.gh_us_smas` |
| `brutos_acolherio_staging.gh_us_tipo` | `rj-smas.brutos_acolherio_staging.gh_us_tipo` |
| `dashboard_acolherio.filtro_email_dev` | `rj-smas-dev.dashboard_acolherio.filtro_email_dev` |

### Alta complexidade

| Modelo, catálogo ou fonte | Dependência direta ou tabela física |
| --- | --- |
| [alta_complexidade](../models/marts/prontuario_eletronico/alta_complexidade/alta_complexidade.sql) | [raw_usuarios_acolhimentos](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_acolhimentos.sql), [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql), [int_filtro_email_unidades](../models/intermediate/seguranca/int_filtro_email_unidades.sql) |
| [dim_unidades](../models/marts/core/dimensions/dim_unidades.sql) | [int_unidades](../models/intermediate/core/int_unidades.sql) |
| [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql) | [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql), [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) |
| [int_filtro_email_unidades](../models/intermediate/seguranca/int_filtro_email_unidades.sql) | `dashboard_acolherio.filtro_email_dev`, [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) |
| [int_unidades](../models/intermediate/core/int_unidades.sql) | [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql), [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql), [int_capacidade_unidades](../models/intermediate/social/int_capacidade_unidades.sql), [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) |
| [raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql) | `brutos_acolherio_staging.gh_us_smas` |
| [raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql) | `brutos_acolherio_staging.gh_us_config` |
| [raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql) | `dashboard_acolherio.filtro_email_dev` |
| [raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql) | `brutos_acolherio_staging.gh_us_tipo` |
| [raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql) | `brutos_acolherio_staging.gh_us` |
| [raw_usuarios_acolhimentos](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_acolhimentos.sql) | `brutos_acolherio_staging.gh_pac_ciclos` |
| `brutos_acolherio_staging.gh_pac_ciclos` | `rj-smas.brutos_acolherio_staging.gh_pac_ciclos` |
| `brutos_acolherio_staging.gh_us` | `rj-smas.brutos_acolherio_staging.gh_us` |
| `brutos_acolherio_staging.gh_us_config` | `rj-smas.brutos_acolherio_staging.gh_us_config` |
| `brutos_acolherio_staging.gh_us_smas` | `rj-smas.brutos_acolherio_staging.gh_us_smas` |
| `brutos_acolherio_staging.gh_us_tipo` | `rj-smas.brutos_acolherio_staging.gh_us_tipo` |
| `dashboard_acolherio.filtro_email_dev` | `rj-smas-dev.dashboard_acolherio.filtro_email_dev` |

## Apêndice de colunas físicas e aliases

### `rj-smas-dev.dashboard_acolherio.dev_atendimentos`

[raw_atendimentos_programada](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_programada.sql)

```sql
{{ config(materialized='table', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Captura atômica da seleção publicada; não recalcula nem deduplica vencedores.
select
    modulo,
    cas,
    tipo_unidade,
    unidade_atendimento,
    email_unidade,
    email_cas,
    seq_atendimento,
    data_de_atendimento,
    hora_de_atendimento_original,
    hora_de_atendimento,
    id_usuario,
    id_familia,
    nome_usuario,
    documentacao,
    numero_documento,
    endereco,
    endereco_numero,
    endereco_complemento,
    bairro,
    referencia_ou_comunidade,
    nome_atendimento_original,
    data_nascimento,
    idade,
    raca_cor,
    sexo,
    profissional_id,
    profissional,
    profissional_cbo_original,
    profissional_cbo,
    cadastrante,
    data_cadastro_atendimento,
    tipo_atendimento,
    nome_atendimento,
    idade_faixa,
    id_atendimento,
    email,
    cbo_unico_rank,
    atendimento_unico_rank,
    projetos_sociais
from {{ source('acolherio_referencia', 'atendimentos_programada') }}
```

### `rj-smas-dev.dashboard_acolherio.filtro_email_dev`

[int_atendimentos_por_usuario](../models/intermediate/atendimentos/int_atendimentos_por_usuario.sql) usa `unidade_atendimento` e `email` na macro `aplicar_filtro_email`.

[int_filtro_email_unidades](../models/intermediate/seguranca/int_filtro_email_unidades.sql) lê `unidade_atendimento` e `email` para compor permissões por unidade.

[raw_sheets_filtro_email_prontuario](../models/raw/sheets/raw_sheets_filtro_email_prontuario.sql)

```sql
-- Camada Raw: Filtro de e-mails por unidade (planilha de apoio do dashboard)
with source as (
    select
        UNIDADE_ATENDIMENTO as unidade_atendimento,
        EMAIL as email
    from {{ source('dashboard_acolherio', 'filtro_email_dev') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_atend_familia`

[int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql) lê `MAX(_airbyte_extracted_at)` como marcador de carga no fluxo mensal.

[raw_atendimentos_familias](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_familias.sql)

```sql
-- Camada Raw: Atendimentos do Módulo Família
with source as (
    select
        sequs as id_unidade,
        seqatend as id_atendimento,
        seqpac as id_paciente,
        seqfamil as id_familia,
        seqprof as id_profissional,
        seqlogincad as id_login_cadastro,
        seqtpatend as id_tipo_atendimento,
        'f' as modulo,
        indatendcanc as flag_cancelado,
        dsclstprof as id_profissional_compartilhado,
        indlocalatend as local_atendimento,
        dtentrada as data_atendimento,
        datcadast as data_cadastro_atendimento,
        horaent as hora_atendimento,
        dtsaida as data_saida,
        horasai as hora_saida,
    from {{ source('brutos_acolherio_staging', 'gh_atend_familia') }}
)
select
    *,
    concat(id_atendimento, modulo) as id_atendimento_modulo
from source
```

### `rj-smas.brutos_acolherio_staging.gh_atendimentos`

[int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql) lê `MAX(_airbyte_extracted_at)` como marcador de carga no fluxo mensal.

[raw_atendimentos_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_atendimentos_usuarios.sql)

```sql
-- Camada Raw: Atendimentos do Módulo Usuário
with source as (
    select
        sequs as id_unidade,
        seqatend as id_atendimento,
        seqpac as id_paciente,
        seqprof as id_profissional,
        seqlogincad as id_login_cadastro,
        seqtpatend as id_tipo_atendimento,
        seqativgrp as id_atividade_grupo,
        codclin as codigo_clinica,
        'u' as modulo,
        indatendcanc as flag_cancelado,
        dsclstprof as id_profissional_compartilhado,
        indlocalatend as local_atendimento,
        dtentrada as data_atendimento,
        datcadast as data_cadastro_atendimento,
        horaent as hora_atendimento,
        dtsaida as data_saida,
        horasai as hora_saida,
    from {{ source('brutos_acolherio_staging', 'gh_atendimentos') }}
)
select
    *,
    concat(id_atendimento, modulo) as id_atendimento_modulo
from source
```

### `rj-smas.brutos_acolherio_staging.gh_ativ_paciente`

[int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql) lê `MAX(_airbyte_extracted_at)` como marcador de carga no fluxo mensal.

[raw_presencas_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_presencas_usuarios.sql)

```sql
-- Camada Raw: Presenças de usuários em atividades de grupo

with source as (
    select
        seqativgrp as id_atividade,
        seqpac as id_usuario,
        dtpresdia as data_presenca,
        horpresdia as hora_presenca,
        sequs as id_unidade,
        seqsetor as id_setor
    from {{ source('brutos_acolherio_staging', 'gh_ativ_paciente') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_ativgrp`

[raw_atividades_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_atividades_grupo.sql)

```sql
-- Camada Raw: Atividades de grupo

with source as (
    select
        seqativgrp as id_atividade,
        nmativgrp as nome_atividade,
        dtativ as data_inicio,
        horaativ as hora_inicio,
        dtativfim as data_fim,
        horaativfim as hora_fim,
        seqtpativ as id_tipo_atividade,
        sequs as id_unidade,
        seqlocal as id_local,
        dscperiodativ as recorrencia,
        indmatri as indicador_matricula,
        obsmatri as observacao_matricula
    from {{ source('brutos_acolherio_staging', 'gh_ativgrp') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_cbo`

[raw_cbo](../models/raw/prontuario_carioca_assistencia_social/raw_cbo.sql)

```sql
-- Camada Raw: Cadastro de CBO (Classificação Brasileira de Ocupações)
with source as (
    select
        codcbo as codigo_cbo,
        dsccbo as descricao,
    from {{ source('brutos_acolherio_staging', 'gh_cbo') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_cidadao_pac`

[raw_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios.sql)

```sql
-- Camada Raw: Dados cadastrais dos pacientes
with source as (
    select
        seqpac as id_paciente,
        sequsref as id_unidade_referencia,
        seqlogin as id_login_cadastro,
        sigufnasc as uf_nascimento,
        muninasc as cidade_nascimento,
        dscnomepac as nome,
        dscnomsoci as nome_social,
        dscnmmae as nome_mae,
        {{ map_coluna_estado_civil('estcivil') }} as estado_civil,
        datnascim as data_nascimento,
        nacional as nacionalidade,
        condestr as condicao_estrangeira,
        paisorigem as pais_origem,
        dscbairroender as bairro,
        dslogradouro as logradouro,
        numend as numero_endereco,
        complend as complemento_endereco,
        pontorefe as ponto_referencia,
        racacor as raca_cor,
        numcpfpac as cpf,
        indsexo as sexo,
        indgenero as genero,
        nuprontpapel as prontuario_papel,
        datcadast as data_cadastro,
        numrg as numero_rg_cadastro,
        dsccns as nis_cadastro
    from {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }}
)

select * from source
```

[raw_usuarios_enderecos](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_enderecos.sql)

```sql
-- Fonte adicional congelada para usuários sem endereço na foto do BI.
select distinct
    seqpac as id_paciente,
    trim(regexp_replace(dslogradouro, r'[,\s;]+', ' ')) as logradouro,
    trim(regexp_replace(numend, r'[,\s;]+', ' ')) as numero,
    trim(regexp_replace(complend, r'[,\s;]+', ' ')) as complemento,
    trim(regexp_replace(pontorefe, r'[,\s;]+', ' ')) as referencia_ou_comunidade
from {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }}
```

### `rj-smas.brutos_acolherio_staging.gh_contas`

[raw_operadores](../models/raw/prontuario_carioca_assistencia_social/raw_operadores.sql)

```sql
-- Camada Raw: Operadores (contas de login) do sistema
with source as (
    select
        seqlogin as id_login,
        nompess as nome_operador,
        dscemail as email,
        dsclogin as login,
        indnivel as nivel_conta,
        indtypeacess as tipo_acesso_codigo,
        indstatuser as status_conta_codigo,
        datcadastro as data_cadastro,
        datultacess as data_ultimo_acesso
    from {{ source('brutos_acolherio_staging', 'gh_contas') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_contas_modulos`

[raw_contas_modulos](../models/raw/prontuario_carioca_assistencia_social/raw_contas_modulos.sql)

```sql
-- Camada Raw: Vínculo contas de operadores × perfis de acesso customizados
-- Fonte: gh_contas_modulos (banco Prontuário Carioca)
-- Grão: 1 linha por conta (seqlogin); seqgrupo = id do perfil customizado
with source as (
    select
        seqlogin as id_login,
        seqgrupo as id_perfil_acesso,
        seqprof as id_profissional,
        indadmin as flag_admin
    from {{ source('brutos_acolherio_staging', 'gh_contas_modulos') }}
    where seqgrupo is not null
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_contas_us`

[raw_operadores_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_operadores_unidades.sql)

```sql
-- Camada Raw: Vínculo entre contas de operadores e unidades do sistema
-- Tabela: gh_contas_us (seqlogin → sequs)
with source as (
    select
        seqlogin as id_login,
        sequs as id_unidade
    from {{ source('brutos_acolherio_staging', 'gh_contas_us') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_evoluadm`

[raw_evolucoes_administrativas](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_administrativas.sql)

```sql
-- Camada Raw: Evoluções administrativas dos pacientes
with source as (
    select
        seqpac as id_paciente,
        seqevopac as id_evolucao,
        sequs as id_unidade,
        seqlogin as id_login,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        dtcancpac as data_cancelamento
    from {{ source('brutos_acolherio_staging', 'gh_evoluadm') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_evolufamil`

[int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql) lê `MAX(_airbyte_extracted_at)` como marcador de carga no fluxo mensal.

[raw_evolucoes_familias](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_familias.sql)

```sql
-- Camada Raw: Evoluções do módulo Família
with source as (
    select
        seqfamil as id_familia,
        seqevopac as id_evolucao,
        seqatend as id_atendimento,
        sequs as id_unidade,
        seqprof as id_profissional,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        dtcancpac as data_cancelamento,
        codabapac as modulo_prontuario,
        seqpac as id_paciente
    from {{ source('brutos_acolherio_staging', 'gh_evolufamil') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_evolugrupo`

[raw_evolucoes_grupo](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_grupo.sql)

```sql
-- Camada Raw: Evoluções de atividades de grupo
with source as (
    select
        seqevogrp as id_evolucao_grupo,
        seqevogrp as id_evolucao,
        seqatigrp as id_atividade,
        seqprof as id_profissional,
        dtevogrp as data_evolucao,
        dscevogrp as descricao_evolucao,
        codabagrp as codigo_abrangencia,
        indtpevogrp as tipo_evolucao,
        dtcancgrp as data_cancelamento,
        dsclistpac as lista_pacientes,
        codcboprof as cbo_profissional,
        dscrefpres as referencia_presenca
    from {{ source('brutos_acolherio_staging', 'gh_evolugrupo') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_evolupac`

[int_centro_pop_mensal](../models/intermediate/centro_pop/int_centro_pop_mensal.sql) lê `MAX(_airbyte_extracted_at)` como marcador de carga no fluxo mensal.

[raw_evolucoes_usuarios](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_usuarios.sql)

```sql
-- Camada Raw: Evoluções do módulo Usuário
with source as (
    select
        seqpac as id_usuario,
        seqevopac as id_evolucao,
        seqatend as id_atendimento,
        sequs as id_unidade,
        seqprof as id_profissional,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        dtcancpac as data_cancelamento,
        codabapac as codigo_abrangencia
    from {{ source('brutos_acolherio_staging', 'gh_evolupac') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_famil_projsociais`

[raw_familias_projetos_sociais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_projetos_sociais.sql)

```sql
-- Camada Raw: Projetos sociais das famílias
with source as (
    select
        seqfamil as id_familia,
        seqprojsoc as id_projeto_social,
        sequs as id_unidade,
        seqlogincad as id_login_cadastro,
        datcadastr as data_cadastro,
        datcancel as data_cancelamento,
        indativo as indicador_ativo
    from {{ source('brutos_acolherio_staging', 'gh_famil_projsociais') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_famil_servassist`

[raw_familias_servicos_assistenciais](../models/raw/prontuario_carioca_assistencia_social/raw_familias_servicos_assistenciais.sql)

```sql
-- Camada Raw: Serviços assistenciais das famílias
with source as (
    select
        seqfamil as id_familia,
        seqservassist as id_servico_assistencial,
        seqlogincad as id_login_cadastro,
        sequs as id_unidade,
        datcadastr as data_cadastro,
        datcancel as data_cancelamento
    from {{ source('brutos_acolherio_staging', 'gh_famil_servassist') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_famil_vulnerab`

[raw_familias_vulnerabilidades](../models/raw/prontuario_carioca_assistencia_social/raw_familias_vulnerabilidades.sql)

```sql
-- Camada Raw: Vulnerabilidades das famílias
with source as (
    select
        seqfamil as id_familia,
        seqlogincad as id_login_cadastro,
        seqvulnerab as id_vulnerabilidade,
        datcadastr as data_cadastro,
        datcancel as data_cancelamento
    from {{ source('brutos_acolherio_staging', 'gh_famil_vulnerab') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_familias`

[raw_familias](../models/raw/prontuario_carioca_assistencia_social/raw_familias.sql)

```sql
-- Camada Raw: Cadastro base de famílias
with source as (
    select
        seqfamil as id_familia,
        seqpac_responsavel as id_usuario_responsavel,
        datultalteracao as data_ultima_modificacao,
        indativo as flag_ativo
    from {{ source('brutos_acolherio_staging', 'gh_familias') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_familias_membros`

[raw_membros_familia](../models/raw/prontuario_carioca_assistencia_social/raw_membros_familia.sql)

```sql
-- Camada Raw: Vínculos de membros com famílias
with source as (
    select
        seqpac as id_paciente,
        seqfamil as id_familia,
        seqmembro as id_membro,
        seqfamilnova as id_familia_nova,
        seqmotivsaida as id_motivo_saida,
        datentrada as data_entrada,
        datsaida as data_saida,
        parentesco_responsavel_familia as parentesco_responsavel
    from  {{ source('brutos_acolherio_staging', 'gh_familias_membros') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_familias_motiv_saida`

[raw_familias_motivos_saida](../models/raw/prontuario_carioca_assistencia_social/raw_familias_motivos_saida.sql)

```sql
select
    seqmotivsaid as id_motivo_saida,
    dscmotivsaid as descricao_motivo_saida,
    indativo as indicador_ativo
from {{ source('brutos_acolherio_staging', 'gh_familias_motiv_saida') }}
```

### `rj-smas.brutos_acolherio_staging.gh_origens`

[raw_origens](../models/raw/prontuario_carioca_assistencia_social/raw_origens.sql)

```sql
-- Camada Raw: Origens de cadastro do sistema
with source as (
    select
        codorigem as id_origem,
        dscoripcsm as descricao_origem,
        codoriraas as codigo_raas,
        dscoriraas as descricao_raas,
        indativo as flag_ativo
    from {{ source('brutos_acolherio_staging', 'gh_origens') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_pac_ciclos`

[raw_usuarios_acolhimentos](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_acolhimentos.sql)

```sql
-- Camada Raw: Ciclos de acolhimento dos usuários
with source as (
    select
        seqciclo as id_ciclo,
        seqpac as id_usuario,
        sequs as id_unidade,
        dtentrada as data_entrada,
        horentrada as hora_entrada,
        dtsaida as data_saida,
        horsaida as hora_saida,
        sequssaida as id_unidade_saida,
        seqpacorig as id_usuario_origem,
        seqloginentr as id_login_entrada,
        seqloginsaid as id_login_saida,
        indciclo as indicador_ciclo,
        indmotivsaida as motivo_saida
    from {{ source('brutos_acolherio_staging', 'gh_pac_ciclos') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_pac_dados`

[raw_usuarios_detalhes](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_detalhes.sql)

```sql
-- Camada Raw: Detalhes complementares dos pacientes
with source as (
    select
        seqpac as id_paciente,
        {{ map_flag_cadunico('indcadunico') }} as flag_cadunico,
        datcadunico as data_cadunico,
        indgestante as flag_gestante,
        dsctomdecproces as numero_processo_decisao_apoiada,
        dsctomdecnome as nome_apoiador,
        indsmentcompr as flag_saude_mental_comprometida,
        indmotivacol as id_motivo_acolhimento,
        indvioldir as violacao_direito,
        valpontos as pontuacao,
        indgraudepend as grau_dependencia,
        indorientsex as orientacao_sexual,
        valrendaativ as renda_ativa,
        indqualativ as tipos_atividade_trabalho,
        valrendabenef as renda_beneficio,
        indtipovinc as vinculo_trabalhista,
        dscmatrcert as matricula_certidao_nascimento
    from {{ source('brutos_acolherio_staging', 'gh_pac_dados') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_pac_info_med`

[raw_usuarios_informacoes_medicas](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_informacoes_medicas.sql)

```sql
select
    seqpac as id_usuario,
    indcomorb as flag_possui_comorbidade,
    dsccomorb as descricao_comorbidades,
    datcomorbinfo as data_informacao_comorbidades,
    dsclocaltratcomorb as local_tratamento_comorbidades,
    datcadastro as data_cadastro,
    sequscadastro as id_unidade_cadastro,
    inddrogas as flag_uso_substancias_psicoativas
from {{ source('brutos_acolherio_staging', 'gh_pac_info_med') }}
```

### `rj-smas.brutos_acolherio_staging.gh_pacientes_sm`

[raw_usuarios_saude_mental](../models/raw/prontuario_carioca_assistencia_social/raw_usuarios_saude_mental.sql)

```sql
-- Camada Raw: Dados complementares de pacientes (módulo Saúde Mental)
with source as (
    select
        seqpacsm as id_paciente,
        codorigem as codigo_origem,
        indtrab as flag_trabalha,
        nmprofi as profissao,
        indfreqescol as flag_frequenta_escola,
        indserie as serie_escolar,
        indescolari as escolaridade,
        indrecbenef as flag_recebe_beneficio,
        indqualbenef as tipo_beneficio,
        indcuratela as flag_curatela,
        indtipcuratela as tipo_curatela,
        {{ map_flag_situacao_rua('indmoradi') }} as flag_situacao_rua,
        indpresdefi as flag_deficiencia,
        indtpdefi as tipo_deficiencia,
        indmoradi as tipo_moradia
    from {{ source('brutos_acolherio_staging', 'gh_pacientes_sm') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_perfil_grupos`

[raw_perfis_grupos](../models/raw/prontuario_carioca_assistencia_social/raw_perfis_grupos.sql)

```sql
-- Camada Raw: Perfis de acesso customizados (catálogo de grupos de perfil)
-- Fonte: gh_perfil_grupos (banco Prontuário Carioca)
-- Grão: 1 linha por perfil (seqgrupo)
with source as (
    select
        seqgrupo as id_perfil_acesso,
        dsctitmod as nome_perfil,
        indnivel as nivel_perfil,
        indus as flag_perfil_em_uso,
        indassocprof as flag_associado_profissional,
        dsccnfaccess as config_acesso_modulos,
        dsccnfaccessrel as config_acesso_relatorios
    from {{ source('brutos_acolherio_staging', 'gh_perfil_grupos') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_prof`

[raw_profissionais](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais.sql)

```sql
-- Camada Raw: Profissionais do Prontuário Carioca
with source as (
    select
        seqprof as id_profissional,
        sequs as id_unidade,
        seqlogin as id_login,
        nomgue as nome_guerra,
        crmbloq as flag_bloqueio,
        matrprof as matricula,
        sigufconselho as uf_conselho,
        nomeprof as nome,
        nomsocprof as nome_social,
        indusonomsoc as flag_uso_nome_social,
        codcns as cns,
        cpfprof as cpf,
        indsexo as sexo,
        indgenero as genero,
        indativo as flag_ativo,
        dtcadast as data_cadastro,
        datultmodif as data_ultima_modificacao,
        emailprof as email,
        dsctel as telefone,
    from {{ source('brutos_acolherio_staging', 'gh_prof') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_profocup`

[raw_profissionais_ocupacoes](../models/raw/prontuario_carioca_assistencia_social/raw_profissionais_ocupacoes.sql)

```sql
-- Camada Raw: Vínculo entre profissionais e CBOs
with source as (
    select
        seqprof as id_profissional,
        codcbo as codigo_cbo
    from {{ source('brutos_acolherio_staging', 'gh_profocup') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_regeatv_evol_quest`

[raw_evolucoes_questionario](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_questionario.sql)

```sql
-- Camada Raw: Evoluções de questionários estruturados (regeatv_evol_quest)
-- Vincula o prontuário (seqprontuario) às evoluções de questionário e às
-- respostas registradas em gh_regeatv_evol_quest_list / _quest_text.
with source as (
    select
        seqprontuario as id_prontuario,
        seqevol as id_evolucao,
        seqmodulo as id_modulo,
        seqtemplate as id_template,
        datevol as data_evolucao,
        dsclstevol as descricao_evolucao,
        dscconteudo as conteudo_evolucao
    from {{ source('brutos_acolherio_staging', 'gh_regeatv_evol_quest') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_regeatv_evol_quest_list`

[raw_evolucoes_questionario_lista](../models/raw/prontuario_carioca_assistencia_social/raw_evolucoes_questionario_lista.sql)

```sql
-- Camada Raw: Respostas de lista dos questionários estruturados (regeatv_evol_quest_list)
-- Vinculadas a gh_regeatv_evol_quest via id_evolucao + id_template.
with source as (
    select
        seqevol as id_evolucao,
        seqtemplate as id_template,
        seqmodulo as id_modulo,
        seqquest as id_questao,
        datresp as data_resposta,
        dscresp as resposta,
        indresp as indicador_resposta,
        numresp as numero_resposta,
        dscresp2 as resposta_complementar
    from {{ source('brutos_acolherio_staging', 'gh_regeatv_evol_quest_list') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_us`

[raw_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_unidades.sql)

```sql
-- Camada Raw: Limpeza e renomeação inicial da fonte de unidades
-- Sistema original: Prontuário Carioca de Assistência Social (ex-Acolherio)

with source as (
    select
        sequs as id_unidade,
        dscus as nome_unidade,
        apus as cas,
        siguf as uf,
        esfera,
        seqtipous as id_tipo_unidade,
        emailprof as email_unidade,
        (indinativo <> 'S') as flag_unidade_ativa
    from {{ source('brutos_acolherio_staging', 'gh_us') }}
)

select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_us_config`

[raw_configuracao_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_configuracao_unidades.sql)

```sql
-- Camada Raw: Configuração de leitos e bloqueios das unidades
with source as (
    select
        sequs as id_unidade,
        numleitos as total_leitos,
        numleitosbloq as leitos_bloqueados,
        numbloqinfra as leitos_bloqueados_infra,
        numbloqjud as leitos_bloqueados_judiciais,
        indadminleitos as flag_administra_leitos
    from {{ source('brutos_acolherio_staging', 'gh_us_config') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_us_smas`

[raw_capacidade_unidades](../models/raw/prontuario_carioca_assistencia_social/raw_capacidade_unidades.sql)

```sql
-- Camada Raw: Capacidade operacional das unidades (SMAS)
with source as (
    select
        sequs as id_unidade,
        indeixo as tipo_publico,
        indacessib as flag_acessibilidade,
        vagas_homens,
        vagas_mulheres,
        vagas_neutras,
        indgraudepend as grau_dependencia,
        indabrangatend as abrangencia
    from {{ source('brutos_acolherio_staging', 'gh_us_smas') }}
)
select * from source
```

### `rj-smas.brutos_acolherio_staging.gh_us_tipo`

[raw_tipos_unidade](../models/raw/prontuario_carioca_assistencia_social/raw_tipos_unidade.sql)

```sql
-- Camada Raw: Tipos de unidade do sistema
with source as (
    select
        seqtipous as id_tipo_unidade,
        dsctipous as nome_tipo,
        indclasstu as classe,
        dscclassdetal as descricao_classe,
        dscclassresum as descricao_classe_resumida
    from {{ source('brutos_acolherio_staging', 'gh_us_tipo') }}
)
select * from source
```

## Catálogos de interpretação

- [acolherio_atividades_trabalho.csv](../seeds/acolherio/acolherio_atividades_trabalho.csv) contém `codigo`, `descricao`, `fonte`, `confianca`.
- [acolherio_dominios.csv](../seeds/acolherio/acolherio_dominios.csv) contém `dominio`, `codigo`, `rotulo`, `ordem`, `vigente_desde`, `vigente_ate`, `fonte`, `observacao`.
- [creas_campos.csv](../seeds/acolherio/creas_campos.csv) contém `titulo`, `rotulo`, `coluna`, `assunto`.
- [formularios_datas_ocorrencia.csv](../seeds/acolherio/formularios_datas_ocorrencia.csv) contém `titulo_formulario`, `label_data_ocorrencia`.

### Mapeamento de perguntas e respostas CREAS

O catálogo liga título e rótulo do formulário à coluna de resposta. A última coluna indica os caminhos presentes no esquema. A publicação de cada valor depende da seleção da evolução e do rótulo preenchido.

| Título do formulário | Rótulo | Coluna no catálogo | Caminhos no BigQuery |
| --- | --- | --- | --- |
| CREAS - MSE - Medidas Socioeducativas | - Com que frequência vai à casa da família ou pessoa de referência? | `mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` | `creas.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia`, `creas.registros.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` |
| CREAS - MSE - Medidas Socioeducativas | - Com quem moraria? | `mse_com_quem_moraria` | `creas.mse_com_quem_moraria`, `creas.registros.mse_com_quem_moraria` |
| CREAS - MSE - Medidas Socioeducativas | 1º Bimestre  [Documentação] | `mse_1o_bimestre_documentacao` | `creas.mse_1o_bimestre_documentacao`, `creas.registros.mse_1o_bimestre_documentacao` |
| CREAS - MSE - Medidas Socioeducativas | 1º Bimestre [Benefícios] | `mse_1o_bimestre_beneficios` | `creas.mse_1o_bimestre_beneficios`, `creas.registros.mse_1o_bimestre_beneficios` |
| CREAS - MSE - Medidas Socioeducativas | 1º Bimestre [CadÚnico] | `mse_1o_bimestre_cadunico` | `creas.mse_1o_bimestre_cadunico`, `creas.registros.mse_1o_bimestre_cadunico` |
| CREAS - MSE - Medidas Socioeducativas | 1º Bimestre [Localização] | `mse_1o_bimestre_localizacao` | `creas.mse_1o_bimestre_localizacao`, `creas.registros.mse_1o_bimestre_localizacao` |
| CREAS - MSE - Medidas Socioeducativas | 1º Bimestre [Outras Ações] | `mse_1o_bimestre_outras_acoes` | `creas.mse_1o_bimestre_outras_acoes`, `creas.registros.mse_1o_bimestre_outras_acoes` |
| CREAS - MSE - Medidas Socioeducativas | 1º Bimestre [Saúde] | `mse_1o_bimestre_saude` | `creas.mse_1o_bimestre_saude`, `creas.registros.mse_1o_bimestre_saude` |
| CREAS - MSE - Medidas Socioeducativas | Adolescente em reinfração? | `mse_adolescente_em_reinfracao` | `creas.mse_adolescente_em_reinfracao`, `creas.registros.mse_adolescente_em_reinfracao` |
| CREAS - MSE - Medidas Socioeducativas | Adolescente tem histórico de cumprimento de MSE em meio fechado ou semiliberdade? | `mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` | `creas.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade`, `creas.registros.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` |
| CREAS - MSE - Medidas Socioeducativas | Avaliação Técnica Bimestral  \| 1º Bimestre | `mse_avaliacao_tecnica_bimestral_1o_bimestre` | `creas.mse_avaliacao_tecnica_bimestral_1o_bimestre`, `creas.registros.mse_avaliacao_tecnica_bimestral_1o_bimestre` |
| CREAS - MSE - Medidas Socioeducativas | CRAS de Referência | `mse_cras_de_referencia` | `creas.mse_cras_de_referencia`, `creas.registros.mse_cras_de_referencia` |
| CREAS - MSE - Medidas Socioeducativas | CRAS de referência | `mse_cras_de_referencia` | `creas.mse_cras_de_referencia`, `creas.registros.mse_cras_de_referencia` |
| CREAS - MSE - Medidas Socioeducativas | CREAS de Referência | `mse_creas_de_referencia` | `creas.mse_creas_de_referencia`, `creas.registros.mse_creas_de_referencia` |
| CREAS - MSE - Medidas Socioeducativas | CREAS de referência | `mse_creas_de_referencia` | `creas.mse_creas_de_referencia`, `creas.registros.mse_creas_de_referencia` |
| CREAS - MSE - Medidas Socioeducativas | Carga horária | `mse_carga_horaria` | `creas.mse_carga_horaria`, `creas.registros.mse_carga_horaria` |
| CREAS - MSE - Medidas Socioeducativas | Com quem residia anteriormente? | `mse_com_quem_residia_anteriormente` | `creas.mse_com_quem_residia_anteriormente`, `creas.registros.mse_com_quem_residia_anteriormente` |
| CREAS - MSE - Medidas Socioeducativas | Data | `mse_data` | `creas.mse_data`, `creas.registros.mse_data` |
| CREAS - MSE - Medidas Socioeducativas | Data de inclusão no MSE | `mse_data_de_inclusao_no_mse` | `creas.mse_data_de_inclusao_no_mse`, `creas.registros.mse_data_de_inclusao_no_mse` |
| CREAS - MSE - Medidas Socioeducativas | Desdobramento | `mse_desdobramento` | `creas.mse_desdobramento`, `creas.registros.mse_desdobramento` |
| CREAS - MSE - Medidas Socioeducativas | Em caso de cumprimento de MSE, qual o status? | `mse_em_caso_de_cumprimento_de_mse_qual_o_status` | `creas.mse_em_caso_de_cumprimento_de_mse_qual_o_status`, `creas.registros.mse_em_caso_de_cumprimento_de_mse_qual_o_status` |
| CREAS - MSE - Medidas Socioeducativas | Em caso de cumprimento de PSC, qual o local? | `mse_em_caso_de_cumprimento_de_psc_qual_o_local` | `creas.mse_em_caso_de_cumprimento_de_psc_qual_o_local`, `creas.registros.mse_em_caso_de_cumprimento_de_psc_qual_o_local` |
| CREAS - MSE - Medidas Socioeducativas | Endereço (CRAS) | `mse_endereco_cras` | `creas.mse_endereco_cras`, `creas.registros.mse_endereco_cras` |
| CREAS - MSE - Medidas Socioeducativas | Endereço (CREAS) | `mse_endereco_creas` | `creas.mse_endereco_creas`, `creas.registros.mse_endereco_creas` |
| CREAS - MSE - Medidas Socioeducativas | Especialidade | `mse_especialidade` | `creas.mse_especialidade`, `creas.registros.mse_especialidade` |
| CREAS - MSE - Medidas Socioeducativas | Gostaria de retornar para casa ou convívio anterior? | `mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` | `creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior`, `creas.registros.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` |
| CREAS - MSE - Medidas Socioeducativas | Gostaria de retornar para casa ou convívio anterior?  - Com quem moraria? | `mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` | `creas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria`, `creas.registros.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` |
| CREAS - MSE - Medidas Socioeducativas | Local | `mse_local` | `creas.mse_local`, `creas.registros.mse_local` |
| CREAS - MSE - Medidas Socioeducativas | Mantém contato com a Família ou pessoa de referência? | `mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` | `creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia`, `creas.registros.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` |
| CREAS - MSE - Medidas Socioeducativas | Mantém contato com a Família ou pessoa de referência?  - Com que frequência vai à casa da família ou pessoa de referência? | `mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` | `creas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia`, `creas.registros.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` |
| CREAS - MSE - Medidas Socioeducativas | Medida aplicada | `mse_medida_aplicada` | `creas.mse_medida_aplicada`, `creas.registros.mse_medida_aplicada` |
| CREAS - MSE - Medidas Socioeducativas | Nome do local de cumprimento de PSC | `mse_nome_do_local_de_cumprimento_de_psc` | `creas.mse_nome_do_local_de_cumprimento_de_psc`, `creas.registros.mse_nome_do_local_de_cumprimento_de_psc` |
| CREAS - MSE - Medidas Socioeducativas | Outras atividades | `mse_outras_atividades` | `creas.mse_outras_atividades`, `creas.registros.mse_outras_atividades` |
| CREAS - MSE - Medidas Socioeducativas | Por que se afastou da família ou pessoa de referência? | `mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` | `creas.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia`, `creas.registros.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` |
| CREAS - MSE - Medidas Socioeducativas | Procedimento | `mse_procedimento` | `creas.mse_procedimento`, `creas.registros.mse_procedimento` |
| CREAS - MSE - Medidas Socioeducativas | Profissional (CRAS) | `mse_profissional_cras` | `creas.mse_profissional_cras`, `creas.registros.mse_profissional_cras` |
| CREAS - MSE - Medidas Socioeducativas | Profissional (CREAS) | `mse_profissional_creas` | `creas.mse_profissional_creas`, `creas.registros.mse_profissional_creas` |
| CREAS - MSE - Medidas Socioeducativas | Quais atividades gostaria de participar? | `mse_quais_atividades_gostaria_de_participar` | `creas.mse_quais_atividades_gostaria_de_participar`, `creas.registros.mse_quais_atividades_gostaria_de_participar` |
| CREAS - MSE - Medidas Socioeducativas | Registrar o que conseguiu cumprir e justificar o que não conseguiu cumprir no prazo estabelecido | `mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` | `creas.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido`, `creas.registros.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` |
| CREAS - MSE - Medidas Socioeducativas | Serviço de proteção social ao adolescente em cumprimento de LA ou PSC | `mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` | `creas.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc`, `creas.registros.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` |
| CREAS - MSE - Medidas Socioeducativas | Telefone (CRAS) | `mse_telefone_cras` | `creas.mse_telefone_cras`, `creas.registros.mse_telefone_cras` |
| CREAS - MSE - Medidas Socioeducativas | Telefone (CREAS) | `mse_telefone_creas` | `creas.mse_telefone_creas`, `creas.registros.mse_telefone_creas` |
| CREAS - MSE - Medidas Socioeducativas | Tempo de cumprimento | `mse_tempo_de_cumprimento` | `creas.mse_tempo_de_cumprimento`, `creas.registros.mse_tempo_de_cumprimento` |
| CREAS - MSE - Medidas Socioeducativas | Tipo | `mse_tipo` | `creas.mse_tipo`, `creas.registros.mse_tipo` |
| CREAS - MSE - Medidas Socioeducativas | Tipo de ato infracional | `mse_tipo_de_ato_infracional` | `creas.mse_tipo_de_ato_infracional`, `creas.registros.mse_tipo_de_ato_infracional` |
| CREAS - MSE - Medidas Socioeducativas | Usuário Interditado? | `mse_usuario_interditado` | `creas.mse_usuario_interditado`, `creas.registros.mse_usuario_interditado` |
| CREAS - MSE - RETORNO | Adolescente em reinfração? | `mse_retorno_adolescente_em_reinfracao` | `creas.mse_retorno_adolescente_em_reinfracao`, `creas.registros.mse_retorno_adolescente_em_reinfracao` |
| CREAS - MSE - RETORNO | Data do desligamento | `mse_retorno_data_do_desligamento` | `creas.mse_retorno_data_do_desligamento`, `creas.registros.mse_retorno_data_do_desligamento` |
| CREAS - MSE - RETORNO | Desligamento | `mse_retorno_desligamento` | `creas.mse_retorno_desligamento`, `creas.registros.mse_retorno_desligamento` |
| CREAS - MSE - RETORNO | Em caso de cumprimento de MSE, qual o status? | `mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` | `creas.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status`, `creas.registros.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` |
| CREAS - MSE - RETORNO | Em caso de cumprimento de PSC, qual o local? | `mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` | `creas.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local`, `creas.registros.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` |
| CREAS - MSE - RETORNO | Motivo do desligamento | `mse_retorno_motivo_do_desligamento` | `creas.mse_retorno_motivo_do_desligamento`, `creas.registros.mse_retorno_motivo_do_desligamento` |
| CREAS - MSE - RETORNO | Nome do local de cumprimento de PSC | `mse_retorno_nome_do_local_de_cumprimento_de_psc` | `creas.mse_retorno_nome_do_local_de_cumprimento_de_psc`, `creas.registros.mse_retorno_nome_do_local_de_cumprimento_de_psc` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Data da inclusão no PETI | `peti_data_da_inclusao_no_peti` | `creas.peti_data_da_inclusao_no_peti`, `creas.registros.peti_data_da_inclusao_no_peti` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Demanda Inicial | `peti_demanda_inicial` | `creas.peti_demanda_inicial`, `creas.registros.peti_demanda_inicial` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Forma de acesso (informe a origem de acesso) | `peti_forma_de_acesso_informe_a_origem_de_acesso` | `creas.peti_forma_de_acesso_informe_a_origem_de_acesso`, `creas.registros.peti_forma_de_acesso_informe_a_origem_de_acesso` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Marcação nos campo CadÚnico | `peti_marcacao_nos_campo_cadunico` | `creas.peti_marcacao_nos_campo_cadunico`, `creas.registros.peti_marcacao_nos_campo_cadunico` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Orientações e Encaminhamentos | `peti_orientacoes_e_encaminhamentos` | `creas.peti_orientacoes_e_encaminhamentos`, `creas.registros.peti_orientacoes_e_encaminhamentos` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Outras demandas / vulnerabilidades | `peti_outras_demandas_vulnerabilidades` | `creas.peti_outras_demandas_vulnerabilidades`, `creas.registros.peti_outras_demandas_vulnerabilidades` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Outras formas de acesso | `peti_outras_formas_de_acesso` | `creas.peti_outras_formas_de_acesso`, `creas.registros.peti_outras_formas_de_acesso` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Outros | `peti_outros` | `creas.peti_outros`, `creas.registros.peti_outros` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | Tipo de trabalho infantil | `peti_tipo_de_trabalho_infantil` | `creas.peti_tipo_de_trabalho_infantil`, `creas.registros.peti_tipo_de_trabalho_infantil` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | É acompanhado em outro CREAS? | `peti_e_acompanhado_em_outro_creas` | `creas.peti_e_acompanhado_em_outro_creas`, `creas.registros.peti_e_acompanhado_em_outro_creas` |
| CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil | É necessário realizar a Gestão de Benefícios? | `peti_e_necessario_realizar_a_gestao_de_beneficios` | `creas.peti_e_necessario_realizar_a_gestao_de_beneficios`, `creas.registros.peti_e_necessario_realizar_a_gestao_de_beneficios` |
| CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil | Data da solicitação do acolhimento | `scfv_data_da_solicitacao_do_acolhimento` | `creas.scfv_data_da_solicitacao_do_acolhimento`, `creas.registros.scfv_data_da_solicitacao_do_acolhimento` |
| CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil | Demanda de acolhimento institucional por decisão judicial? | `scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` | `creas.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial`, `creas.registros.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` |
| CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil | Nome do CRAS responsável | `scfv_nome_do_cras_responsavel` | `creas.scfv_nome_do_cras_responsavel`, `creas.registros.scfv_nome_do_cras_responsavel` |
| CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil | O usuário participa do SCFV (Serviço de Convivência e Fortalecimento de Vínculos)? | `scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` | `creas.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos`, `creas.registros.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` |
| CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil | Oriundo de instituição de acolhimento interditado? | `scfv_oriundo_de_instituicao_de_acolhimento_interditado` | `creas.scfv_oriundo_de_instituicao_de_acolhimento_interditado`, `creas.registros.scfv_oriundo_de_instituicao_de_acolhimento_interditado` |
| CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil | Usuário Interditado? | `scfv_usuario_interditado` | `creas.scfv_usuario_interditado`, `creas.registros.scfv_usuario_interditado` |
| CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil | Órgão de origem do acolhimento | `scfv_orgao_de_origem_do_acolhimento` | `creas.scfv_orgao_de_origem_do_acolhimento`, `creas.registros.scfv_orgao_de_origem_do_acolhimento` |
| CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil | Data do desligamento do PETI | `peti_desligamento_data_do_desligamento_do_peti` | `creas.peti_desligamento_data_do_desligamento_do_peti`, `creas.registros.peti_desligamento_data_do_desligamento_do_peti` |
| CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil | Motivo do desligamento | `peti_desligamento_motivo_do_desligamento` | `creas.peti_desligamento_motivo_do_desligamento`, `creas.registros.peti_desligamento_motivo_do_desligamento` |
| CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil | Outros | `peti_desligamento_outros` | `creas.peti_desligamento_outros`, `creas.registros.peti_desligamento_outros` |
