> Contrato público atual: `por_assunto_v9_campos_diretos_formularios_texto`; veja a seção de campos diretos e formulários em texto ao final.

# Perfil Centro POP por usuário

> A ampliação da supertabela com os 13 campos do BI está documentada em [Complementos do BI](complementos_bi_planilha_unificada.md). O perfil Centro POP mantém seu contrato.

`dim_centro_pop` tem uma linha por usuário com respostas específicas do Centro POP. O intermediário materializado `int_pu_registros_centro_pop` conserva todas as ocorrências datadas para histórico e auditoria. Atendimentos, evoluções, presenças, unidades, cadastro pessoal e composição familiar não são replicados na dimensão. CRAS, CREAS e a mart Centro POP existente mantêm seus contratos e tabelas.

## Contratos

A dimensão mantém `centro_pop_usuario_v3`. A supertabela usa
`por_assunto_v9_campos_diretos_formularios_texto`, expondo os 35 campos do perfil como filhos
diretos de `escopos.centro_pop`, sem o agrupador `respostas`.

A chave `id_usuario INT64` é obrigatória e única. A dimensão contém somente colunas simples e `ARRAY<STRING>`, sem STRUCTs. Representa o último perfil conhecido, considerando todas as unidades, sem corte mensal. Usuários com somente participação não precisam de uma linha de perfil: permanecem na supertabela por `dim_usuarios`.

Uma ocorrência inteira é selecionada por usuário e assunto (PAI, atendimento social, saúde, situação de rua e desligamento), ordenando `data_registro DESC NULLS LAST`, `data_resposta DESC NULLS LAST`, `id_registro DESC`. A seleção precede a projeção dos campos: respostas vazias do último formulário permanecem vazias. Não há preenchimento com formulários anteriores nem inferência de acompanhamento ativo. `possui_registro_pai` usa `Sim` quando existe formulário, inclusive incompleto, e `''` sem evidência.

| Assunto | Colunas de resposta |
|---|---|
| PAI | `data_inclusao_pai`, `data_inclusao_informada`, `demandas_pai`, `encaminhamentos_pai`, `observacoes_pai` |
| Atendimento social | `motivo_secundario_permanencia_rua`, `possui_referencias_familiares`, `territorio_referencia_familia`, `possibilidade_reinsercao_familiar`, `demandas_atendimento_social`, `encaminhamentos_atendimento_social`, `observacoes_atendimento_social` |
| Saúde | `uso_substancias_psicoativas`, `situacao_saude`, `local_tratamento`, `possui_problema_saude`, `realiza_acompanhamento_saude`, `observacoes_saude` |
| Situação de rua | `motivo_principal_permanencia_rua` |
| Desligamento | `data_desligamento`, `data_desligamento_informada`, `motivo_desligamento`, `motivo_desligamento_outros`, `observacoes_desligamento` |

Cada assunto também possui `data_registro_<assunto>` e `data_resposta_<assunto>` DATETIME. Datas declaradas de inclusão/desligamento são DATE e não recebem fallback pela data da evolução. O texto informado original permanece separado. Observações procedem do próprio formulário; demandas e encaminhamentos são separados para PAI e atendimento social. Encaminhamentos viram listas `rótulo: resposta`, sem duplicatas e com ordenação lexical estável. Campos sem fonte comprovada, como empregabilidade, habilidades, interesse em curso e resultados de acesso, não são criados no perfil.

```text
usuario | familias | projetos_sociais | atendimentos | evolucoes | escopos
escopos.centro_pop.historico[]     # contrato anterior integral
escopos.centro_pop.participacoes[] # contrato anterior integral
escopos.centro_pop.<campo>         # 35 campos do perfil diretamente no escopo
```

A supertabela cruza a dimensão por `id_usuario` usando LEFT JOIN e conserva a população de `dim_usuarios`. A lista `escopos.centro_pop.unidades` e sua lógica de vínculo foram removidas. Textos públicos ausentes são `''`, listas são `[]` e datas são convertidas em STRING somente na apresentação. As contagens existentes continuam INT64. O histórico conserva os encaminhamentos originais em label/valor e os campos legados vazios, sem mudanças de schema ou conteúdo.

## Fontes e temporalidade

A extração histórica permanece em `pu_cpop_registros_sql`, sem alteração. Formulários procedem de `int_pu_formularios` e suas evoluções não canceladas. A população de origem inclui usuários com atendimento Centro POP, formulário específico ou presença, associados ao cadastro válido. Saúde considera o histórico desses usuários também em outras unidades. Motivo principal usa o questionário estruturado da mart, com evolução/template/módulo e associação do prontuário ao usuário.

Presenças usam `raw_presencas_usuarios` e `raw_atividades_grupo` congeladas no lote, restritas a unidades Centro POP. A identidade usuário/unidade/atividade/data/hora mantém sessões distintas e deduplica repetições idênticas. Oficinas homônimas conservam seus IDs.

Flags são Sim/Não/vazio. Respostas escalares contraditórias na mesma ocorrência não recebem escolha arbitrária e reprovam `pu_cpop_integridade`. Textos múltiplos de saúde conservam a interpretação existente. O histórico exportado não reconstrói edições sobrescritas. Consultas mensais devem usar o histórico e aplicar seus próprios cortes; o perfil contém as respostas mais recentes de todo o lote.

A classificação de atendimentos continua centralizada: técnico quando algum CBO começa por 2; depois recepção pelas regras existentes; caso contrário Outros Atendimentos. Outros serviços recebem classificação vazia. PKs, tipos originais e contagens permanecem iguais.

## Construção e publicação manual

Para esta migração, reutilize o lote congelado indicado no controle da supertabela e uma execução nova. Backups antigos de dimensões não são restaurados como implementação, pois não têm a granularidade por usuário/assunto. O comando `preparar` continua disponível para capturar lotes de desenvolvimento; `consolidar` exige o mesmo lote da versão publicada nesta migração.

```bash
.venv/bin/python queries/operations/planilha_unificada.py dimensao --lote lote_publicado --execucao execucao_nova --servico centro_pop
.venv/bin/python queries/operations/planilha_unificada.py consolidar --lote lote_publicado --execucao execucao_nova --servicos centro_pop
.venv/bin/python queries/operations/planilha_unificada.py publicar --execucao execucao_nova
```

A dimensão e o histórico são construídos e testados no dataset privado `pu_cand_<execucao>`. O manifesto v3 registra lote, fontes e fingerprints e recusa artefatos de contratos anteriores. A consolidação copia a supertabela publicada para `_pu_antes_planilha_unificada` e verifica igualdade integral por usuário de todos os blocos existentes, incluindo PKs, valores, ordem das listas e quantidades de atendimentos/evoluções, histórico e participações. A comparação v9 projeta a tabela anterior para os novos caminhos aprovados e converte os pares de respostas para textos; todos os demais campos participam da comparação integral.

A publicação manual verifica geração, fingerprints e consulta programada pausada, sem execuções pendentes. Cria backups da supertabela e do controle antes da primeira substituição. As dimensões, o intermediário histórico e a mart Centro POP publicada são protegidos por fingerprints e não são substituídos. O manifesto `_pu_migracao` registra os destinos e backups. Copia somente a unificada candidata validada, verifica os dados e atualiza somente a linha supertabela no controle. Como a migração muda schemas, as cópias são sequenciais com restauração compensatória em caso de falha; não há transação atômica entre os schemas. Uma falha restaura cada tabela cuja substituição foi tentada, verifica os conteúdos restaurados e registra `restaurado` ou `restauracao_pendente` com os backups necessários.

A operação não executa deploy nem retoma agendas. O ciclo automático permanece desabilitado e a consulta programada permanece pausada. Os backups ficam no dataset da execução publicada, protegido pela rotina de limpeza enquanto estiver referenciado no controle. Migrações em estado preparado ou restauracao_pendente também impedem a limpeza dos backups.

## Matriz das 50 colunas

Arquivo de referência: `Planilha Unificada - Centro POP (1).xlsx`, aba Página1, cabeçalhos das linhas 1/3 e notas das linhas 4–11. O XLSX é referência de desenvolvimento e não é dependência de execução. SHA256: `9c863ad55fcbdc586dae397c5e71f2c7a8c8ad0f89c9e91977411645b722c32f`.

| Coluna | Pergunta | Destino | Fonte / regra | Situação |
|---|---|---|---|---|
| A | Nº | Futura mart: numero | Sequência de apresentação, não identidade do usuário ou evento. | Derivado no consumo / evento |
| B | MÊS DE REFERÊNCIA | atendimentos.registros.data_atendimento | Mês de referência derivado no consumo. | Derivado no consumo / evento |
| C | CENTRO POP | atendimentos.registros.nome_unidade | Unidade do evento, sem copiar eventos para a dimensão. | Derivado no consumo / evento |
| D | PROFISSIONAL DE REFERÊNCIA | atendimentos.registros.profissionais | Profissionais do próprio evento, conforme referência existente; não inventar referência continuada. | Derivado no consumo / evento |
| E | NOME DO USUÁRIO DO SERVIÇO | usuario.nome | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| F | NOME SOCIAL | usuario.nome_social | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| G | USUÁRIO/FAMÍLIA FOI ATENDIDO PONTUALMENTE ? (SIM OU NÃO) | atendimentos.registros.tipo_atendimento_descricao | Regra de pontual versus PAI aplicada no consumo; uma pessoa pode ter ambos no mês. | Derivado no consumo / evento |
| H | USUÁRIO/ FAMILIA FOI INSERIDO NO ACOMPANHAMENTO? (SIM OU NÃO) | escopos.centro_pop.historico.tipo_registro=pai | Presença de formulário, sem inferir acompanhamento ativo. | Escopo datado; vazio sem evidência |
| I | DATA DA INCLUSÃO NO ACOMPANHAMENTO DD/MM/AAAA | escopos.centro_pop.historico.data_inclusao_pai | Data do atendimento declarada no PAI; manter data de registro separada e não usar fallback. | Escopo datado; vazio sem evidência |
| J | USUÁRIO/ FAMÍLIA TEM PLANO DE ACOMPANHAMENTO INDIVIDUAL? (SIM OU NÃO) | escopos.centro_pop.historico.tipo_registro=pai | Evidência do plano; primeira/última ocorrência serão escolhidas no consumo mensal. | Escopo datado; vazio sem evidência |
| K | DATA DO ÚLTIMO ATENDIMENTO TÉCNICO | atendimentos.registros.classificacao_atendimento e data_atendimento | Último técnico até o fechamento, inclusive meses anteriores; sem agregar na dimensão. | Derivado no consumo / evento |
| L | QUANTIDADE DE ATENDIMENTOS REALIZADOS NO MÊS | atendimentos.registros e quantidade | Totais por mês/tipo permanecem na unificada/consumo. A anotação pode ignorar não remove eventos. | Derivado no consumo / evento |
| M | 1 - PRINCIPAL MOTIVO DE PERMANÊNCIA NA RUA | escopos.centro_pop.historico.motivo_principal_permanencia_rua | Questionário estruturado utilizado pela mart: template 3, questão 18; qualificar módulo. | Escopo datado; vazio sem evidência |
| N | 2 – MOTIVO SECUNDÁRIO DE PERMANÊNCIA NA RUA | escopos.centro_pop.historico.motivo_secundario_permanencia_rua | Atendimento Social: Motivo secundário da ida às ruas. | Escopo datado; vazio sem evidência |
| O | DATA DE NASCIMENTO DD/MM/AAAA | usuario.data_nascimento | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| P | IDADE INFORMADA | usuario.data_nascimento | Idade calculada no fechamento do mês; não inventar idade manual. | Derivado no consumo / evento |
| Q | NOME DA MÃE | usuario.filiacao_mae | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| R | RCN E DOCUMENTAÇÃO | usuario.cpf, usuario.nis, usuario.numero_rg e demais números/flags de documentos | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| S | GÊNERO | usuario.genero | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| T | ORIENTAÇÃO SEXUAL | usuario.orientacao_sexual | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| U | COR E ETNIA | usuario.raca_cor | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| V | NATURALIDADE | usuario.uf_nascimento (UF, sem inferir município) | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| W | VOCÊ ESTUDA? (SIM OU NÃO) | usuario.flag_frequenta_escola | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| X | CASO SIM, QUAL ANO ESTÁ CURSANDO? | usuario.serie_escolar | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| Y | NÍVEL DE ESCOLARIDADE | usuario.escolaridade_indice | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| Z | POSSUI DEFICIÊNCIA? QUAL? | usuario.flag_deficiencia e deficiencia.descricao | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| AA | FAZ USO DE SUBSTÂNCIAS PSICOATIVAS? | escopos.centro_pop.historico.uso_substancias_psicoativas | Situação de Saúde; histórico de pessoa, inclusive em outras unidades. | Escopo datado; vazio sem evidência |
| AB | TEM OU TEVE ALGUM PROBLEMA DE SAÚDE? | escopos.centro_pop.historico.possui_problema_saude e situacao_saude | Evidências datadas. Futuro consumo tem ou teve usa histórico até o corte; cadastro é snapshot atual. | Escopo datado; vazio sem evidência |
| AC | REALIZA ALGUM ACOMPANHAMENTO DE SAÚDE? | escopos.centro_pop.historico.realiza_acompanhamento_saude e local_tratamento | Situação de Saúde; preservar texto e interpretação existente. | Escopo datado; vazio sem evidência |
| AD | USUÁRIO POSSUI VÍNCULO FAMILIAR? (SIM OU NÃO) | escopos.centro_pop.historico.possui_referencias_familiares | Atendimento Social: Possui referências familiares? | Escopo datado; vazio sem evidência |
| AE | TERRITÓRIO DE REFERÊNCIA DA FAMÍLIA | escopos.centro_pop.historico.territorio_referencia_familia | Atendimento Social: Território da referência familiar. | Escopo datado; vazio sem evidência |
| AF | EXISTE POSSIBILIDADE DE REINSERÇÃO FAMILIAR? (SIM OU NÃO) | escopos.centro_pop.historico.possibilidade_reinsercao_familiar | Atendimento Social: Existe possibilidade de reinserção familiar? | Escopo datado; vazio sem evidência |
| AG | EXERCE ALGUMA ATIVIDADE QUE GERE RENDA? QUAL? | usuario.flag_trabalha e profissao | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| AH | VALOR DA RENDA OBTIDA ATRAVÉS DA ATIVIDADE? | usuario.renda_ativa | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| AI | ESTÁ PRONTO (A) PARA EMPREGABILIDADE IMEDIATA? (SIM OU NÃO) | escopos.centro_pop.historico.empregabilidade_imediata | Sem coleta comprovada nas fontes mapeadas; manter vazio e registrar pendência. | Sem coleta |
| AJ | CAPACIDADE/HABILIDADES | escopos.centro_pop.historico.capacidade_habilidades | Sem coleta comprovada nas fontes mapeadas; manter vazio e registrar pendência. | Sem coleta |
| AK | TEM INTERESSE EM FAZER CURSO? (SIM OU NÃO) | escopos.centro_pop.historico.interesse_curso | Sem coleta comprovada nas fontes mapeadas; manter vazio e registrar pendência. | Sem coleta |
| AL | USUÁRIO POSSUI ALGUM BENEFÍCIO? | usuario.flag_recebe_beneficio e beneficio | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| AM | USUÁRIO POSSUI CAD ÚNICO? (SIM OU NÃO) | usuario.flag_cadunico | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| AN | CADASTRO ESTÁ ATUALIZADO? (SIM OU NÃO) | usuario.data_atualizacao e data_limite_cadastro_atual | Confrontar o snapshot oficial com o corte; usuario.data_cadunico_informada é outra informação. | Derivado no consumo / evento |
| AO | NIS DO USUÁRIO | usuario.nis | Cadastro/documentos comuns; apresentação Sim/Não deve respeitar ausência e snapshot atual. | Comum existente |
| AP | USUÁRIO PARTICIPA DE OFICINA E/OU ATIVIDADES COLETIVAS? (SIM OU NÃO) | escopos.centro_pop.participacoes | Somente presença comprovada na unidade Centro POP; lista vazia não fabrica Não. | Escopo datado; vazio sem evidência |
| AQ | DE QUANTAS OFICINA E/OU ATIVIDADES COLETIVAS O USUÁRIO PARTICIPOU NO MÊS DE REFERÊNCIA? | escopos.centro_pop.participacoes.id_atividade | Futura mart: COUNT DISTINCT atividade no mês/unidade; manter sessões datadas, sem contar atendimento como oficina. | Escopo datado; vazio sem evidência |
| AR | PROCEDIMENTO 1 | escopos.centro_pop.historico.demandas | Demandas de Atendimento Social e PAI; não substituir pelo nome do atendimento. Preservar múltiplos valores. | Escopo datado; vazio sem evidência |
| AS | ENCAMINHAMENTO 1 | escopos.centro_pop.historico.encaminhamentos | Campos Encaminhamentos dos formulários PAI/social, preservando label e valor. | Escopo datado; vazio sem evidência |
| AT | RESULTADO 1 / ACESSO | escopos.centro_pop.historico.resultado_acesso | Sem coleta comprovada nas fontes mapeadas; manter vazio e registrar pendência. | Sem coleta |
| AU | RESULTADO 1 / DESCRIÇÃO | escopos.centro_pop.historico.resultado_descricao | Sem coleta comprovada nas fontes mapeadas; manter vazio e registrar pendência. | Sem coleta |
| AV | DATA DE DESLIGAMENTO DO ACOMPANHAMENTO | escopos.centro_pop.historico.data_desligamento | Desligamento PAI: data declarada; sem fallback pela data da evolução. | Escopo datado; vazio sem evidência |
| AW | MOTIVO DO DESLIGAMENTO | escopos.centro_pop.historico.motivo_desligamento e motivo_desligamento_outros | Preservar partes separadas; apresentação pode concatenar posteriormente. | Escopo datado; vazio sem evidência |
| AX | OBSERVAÇÕES | escopos.centro_pop.historico.observacoes | Somente observações exportadas do formulário; sem preencher com outro relato livre. | Escopo datado; vazio sem evidência |

## Verificação

Os testes sintéticos executam SQL real no BigQuery com múltiplos formulários/unidades, desempates por data de resposta e identificador técnico, datas ausentes, último formulário incompleto, listas repetidas, saúde em outras unidades e ausência de multiplicação de usuários/eventos. Os testes dbt verificam chave não nula/única, ausência de STRUCTs, projeção do perfil, conteúdo integral do histórico e participações e igualdade da população da raiz.

Os testes da operação cobrem lote divergente, artefato antigo, geração obsoleta, programada ativa/pendente, criação de todos os backups antes das substituições e falhas de publicação/restauração. Habilite `RUN_BIGQUERY_TESTS=1` para as regressões SQL sintéticas; os demais testes não precisam de credenciais.

## Publicação verificada em 27/10/2026

Contrato `centro_pop_usuario_v3`, execução `e20261027_cpop_usuario`, lote congelado
`l20261027_cpop_respostas_qa`. Publicação concluída em `rj-smas-dev`, após
construção isolada e comparação integral da mesma base.

| Medida | Antes | Depois |
|---|---:|---:|
| Usuários na supertabela | 586.840 | 586.840 |
| Atendimentos | 1.021.410 | 1.021.410 |
| Evoluções | 654.748 | 654.748 |
| Registros históricos Centro POP | 1.128 | 1.128 |
| Participações comprovadas | 0 | 0 |

A dimensão publicada tem **769 usuários distintos**, **36 colunas**, chave não
nula e nenhum STRUCT. Seis perfis têm evidência de PAI. Ausência de participação
neste lote não equivale a resposta negativa. A igualdade integral por usuário
incluiu conteúdo, chaves, contagens e ordem das listas anteriores, além da
comparação do intermediário histórico com suas identidades técnicas.

Os fingerprints de `dim_cras`, `dim_creas`, `dim_outros_equipamentos` e da mart
`mart_planilha_unificada_centro_pop` permaneceram iguais. A consulta programada
foi confirmada pausada e sem execuções pendentes após a publicação.

Passaram os 28 testes dbt (8 da dimensão/histórico e 20 da supertabela),
24 testes locais, 22 regressões SQL dos modelos, 8 regressões SQL do escopo e
5 testes BigQuery de publicação/comparação/restauração. O ensaio de falha trocou
o schema de uma dimensão isolada e verificou a restauração do schema e dos dados;
os testes negativos também detectaram mudanças de chaves/histórico com totais
iguais. Ruff, SQLFluff dos chamadores SQL e a verificação de espaços do Git passaram.
O lint Jinja usa projeções locais para as macros com retorno de dicionários;
os testes dbt/BigQuery executaram o SQL completo.

Os quatro backups de dimensão, histórico, supertabela e controle estão registrados
em `rj-smas-dev.pu_cand_e20261027_cpop_usuario._pu_migracao`, estado `publicado`.
Os nomes terminam em `20261027_165430_676699`. A consulta programada permanece
pausada e o ciclo automático continua desabilitado.

## Campos diretos na supertabela — v8

O perfil continua uma dimensão plana por usuário. Sua projeção agora resulta em
`escopos.centro_pop.possui_registro_pai`, `escopos.centro_pop.situacao_saude` e
demais campos, mantendo histórico/participações no mesmo escopo. São 31 campos
STRING e quatro ARRAY<STRING>; não há conversão para JSON ou concatenação das
listas. A estrutura anterior `escopos.centro_pop.respostas.<campo>` foi removida.

```sql
SELECT
    p.usuario.id_usuario,
    p.escopos.centro_pop.* EXCEPT (historico, participacoes)
FROM `rj-smas-dev.relatorio.planilha_unificada` AS p;
```

A consulta mantém todos os usuários. Para consultar somente quem possui registros
específicos, adicionar `WHERE ARRAY_LENGTH(p.escopos.centro_pop.historico) > 0`.
Consultar o STRUCT inteiro pela API continua usando sua serialização padrão;
a seleção acima apresenta os campos individualmente e preserva as listas tipadas.

## Publicação verificada do escopo direto em 27/10/2026

Rematerializada `rj-smas-dev.relatorio.planilha_unificada`, contrato
`por_assunto_v8_centro_pop_direto`, execução `e20261027_cpop_direto`, usando o mesmo
lote congelado `l20261027_complementos_bi`, sem nova captura de fontes.

O schema publicado tem 37 filhos em `escopos.centro_pop`: histórico, participações
e os 35 campos do perfil. O campo `respostas` não existe mais. A consulta de
colunas mostrada acima foi validada com 36 colunas: ID do usuário e 35 campos,
sendo 31 STRING e quatro ARRAY<STRING> no perfil, sem conversão para JSON.

A comparação integral, reconstruindo o formato anterior apenas na validação,
confirmou todos os valores antigos, ordem e multiplicidade das listas. Permanecem
586.840 usuários, 1.021.410 atendimentos, 654.748 evoluções, 1.050.539 linhas
profissionais, 769 perfis Centro POP e 1.128 registros históricos. Os 586.071
usuários sem perfil recebem campos vazios; nenhum perfil vinculado ficou vazio.
Participações continuam com zero registros comprovados neste lote.

Passaram os 35 testes dbt (8 da dimensão/histórico e 27 da consolidação),
35 testes locais e oito regressões SQL do escopo. O teste SQL da movimentação
preservou os valores e detectou alterações em escalares, histórico e ordem das
listas. O ensaio real de falha confirmou a restauração do agrupador antigo e dos
dados após trocar o schema. Ruff, SQLFluff e a verificação de espaços do Git passaram.

A publicação alterou somente supertabela e sua linha de controle. As dimensões,
o histórico materializado, a mart e os controles dos demais serviços permaneceram
intactos. O manifesto `rj-smas-dev.pu_cand_e20261027_cpop_direto._pu_migracao`, estado
`publicado`, registra os dois backups com sufixo `20261027_185638_336211`.
A consulta programada permanece pausada e o ciclo automático desabilitado.

[Schema publicado e evidência agregada](validacao_centro_pop_direto_20261027.json).


### Contrato v9: campos diretos e formulários em texto

O contrato atual é `por_assunto_v9_campos_diretos_formularios_texto`. Os campos dos antigos
agrupadores `usuario.cadastro_complementar`, `usuario.documentos_cadunico` e
`usuario.endereco` ficam diretamente em `usuario`, com os mesmos nomes dos campos.
Em `atendimentos` e `evolucoes`, as contagens por serviço ficam em
`quantidade_cras`, `quantidade_creas`, `quantidade_centro_pop` e `quantidade_outros`.

`evolucoes.registros.formularios.campos` é `ARRAY<STRING>` com itens `rótulo: valor`,
sem deduplicar, reordenar, dividir respostas ou remover itens vazios. Título,
observações e vínculo com cada evolução permanecem. Sem campos, a lista é `[]`.
Os pares originais `label`/`valor` continuam no intermediário para auditoria.

Os seis blocos principais continuam nativos; selecionar um bloco inteiro pode
exibir um objeto na ferramenta de consulta. Exemplo de seleção dos campos:

```sql
SELECT p.usuario.id_usuario, p.usuario.logradouro,
       p.atendimentos.quantidade_cras, f.titulo_formulario, f.campos
FROM `rj-smas-dev.relatorio.planilha_unificada` p
LEFT JOIN UNNEST(p.evolucoes.registros) e ON TRUE
LEFT JOIN UNNEST(e.formularios) f ON TRUE
```

Esta consulta tem granularidade por formulário; a tabela permanece com uma linha
por usuário. A publicação v9 usa a mesma base congelada, backup, validação integral
e restauração em caso de falha. A consulta programada permanece pausada.


### Publicação v9 em 27/10/2026

A execução `e20261027_campos_diretos` rematerializou somente
`rj-smas-dev.relatorio.planilha_unificada`, usando o lote congelado
`l20261027_complementos_bi`. Cadastro complementar, documentos e endereço são
campos diretos de `usuario`; as contagens por serviço são campos diretos dos
blocos de eventos; `formularios.campos` contém textos `rótulo: valor`.

Foram aprovados os 8 testes dbt do Centro POP e os 28 da consolidação, além dos
testes de esquema, preservação de respostas repetidas e restauração real após
falha na troca de esquema. A comparação integral por usuário usa exclusivamente
as transformações aprovadas. O perfil, histórico e participações Centro POP e
as demais dimensões/mart publicadas permaneceram preservados.

Os backups da unificada e do controle estão registrados em
`rj-smas-dev.pu_cand_e20261027_campos_diretos._pu_migracao`. Somente a linha
`supertabela` do controle foi atualizada. A consulta programada permaneceu pausada.

As métricas, o esquema publicado e os nomes completos dos backups estão em
[Validação da publicação v9](validacao_campos_diretos_formularios_20261027.json).
