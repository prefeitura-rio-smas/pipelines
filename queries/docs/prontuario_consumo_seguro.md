# Parecer técnico sobre o filtro de e-mail do Prontuário Eletrônico

## Conclusão

**Parecer favorável à regra implementada no SQL. A aplicação do filtro no relatório permanece pendente de validação autenticada.** No PR 163, a autorização de consumo usa o `email_filtro` do registro, associado à sua unidade. A consulta do Looker recebe a identidade do visualizador em `@DS_USER_EMAIL` e só retorna registros em que o endereço consta integralmente na lista de destinatários.

O campo `email_filtro` é metadado de autorização gravado na mart. Para confirmar a proteção no relatório, é preciso verificar qual fonte cada gráfico usa e testar com contas que tenham acesso a uma unidade, a duas unidades e a nenhuma. O acesso direto à tabela segue as permissões do BigQuery.

## Escopo

Este parecer descreve a origem de `email_filtro`, a regra aplicada pela consulta personalizada e as evidências disponíveis. `filtro_email_dev` é a planilha que fornece destinatários adicionais. `email_filtro` é o nome da coluna publicada e consumida pelo SQL.

## Regra de autorização

A planilha unificada tem uma linha por cidadão e unidade. Se uma pessoa foi atendida em duas unidades, ela pode aparecer em duas linhas, cada uma com os eventos da unidade correspondente. O campo `email_filtro` lista os destinatários autorizados. A consulta segura do Looker usa essa lista para decidir quais registros devolver a quem está logado.

1. O modelo `int_filtro_email_unidades` reúne, por `id_unidade`, o e-mail da CAS, o e-mail cadastrado para a unidade e os e-mails adicionais da planilha `filtro_email_dev`. A planilha é associada pelo nome da unidade, com espaços nas pontas removidos e letras minúsculas. Se o mesmo nome apontar para mais de uma unidade com permissão na planilha, ou se houver configurações conflitantes, o modelo falha.
2. Os endereços são separados por vírgula, ponto e vírgula ou quebra de linha. O modelo remove espaços nas pontas, converte para minúsculas, descarta vazios e elimina repetições. Se a CAS não estiver mapeada, usa `gdados.smas@prefeitura.rio` para aquela unidade.
3. A mart publica a lista no topo da linha e nos registros que carregam a autorização da unidade. A consulta personalizada recebe o e-mail do visualizador pelo parâmetro padrão `@DS_USER_EMAIL` e compara o endereço inteiro com cada endereço da lista do registro. E-mail desconhecido ou vazio, lista vazia ou unidade ausente impedem que aquele registro apareça.
4. A consulta aplica a regra a projetos sociais, atendimentos, evoluções, perfis Centro POP e CREAS, histórico mensal Centro POP e ciclos de alta complexidade. Os itens dentro de uma entrada mensal são exibidos junto com a entrada autorizada. A linha só aparece se sobrar pelo menos um registro autorizado. Os contadores de eventos são recalculados a partir dos registros retornados.

Por exemplo, se uma pessoa tem atendimentos nas unidades A e B e o visualizador só está na lista de A, a consulta mostra os atendimentos de A. Se o e-mail está nas duas listas, mostra os registros das duas unidades.

A publicação da mart grava a lista de destinatários naquele momento. Para aplicar inclusões ou revogações da planilha, é preciso atualizar a mart. O acesso direto pelo BigQuery é controlado pelas permissões do BigQuery. Para o filtro descrito aqui valer no relatório, a fonte deve usar a consulta personalizada com `DS_USER_EMAIL` habilitado. A configuração ativa do Looker ainda precisa ser confirmada.

Implementação no repositório: [destinatários por unidade](../models/intermediate/seguranca/int_filtro_email_unidades.sql), [mart do prontuário](../models/marts/prontuario_eletronico/prontuario_eletronico.sql), [filtro de consumo](../macros/dominios/prontuario_eletronico/consumo_seguro.sql) e [análise do Looker](../analyses/looker_prontuario_seguro.sql).

## Diagnóstico histórico

Head investigado: `dc4d278021ac070d3bacb8fcbe1aa5cf3ac88dda`. Em `prontuario_eletronico.sql`, `atendimentos_por_usuario`, `registros_evolucoes`, perfis e histórico mensal agregam eventos por cidadão. O SELECT final publica esses arrays junto com quantidades e resumos globais. `email_filtro` é apenas metadado: nenhuma cláusula consulta a identidade do visualizador nessa mart. Filtrar o cidadão pela união dos destinatários libera os demais registros da mesma linha. O contrato correto do consumo exige autorização por registro/unidade, antes de calcular os resumos.

Profissionais são agrupados por evento em `int_atendimentos_por_usuario`. A mart publica um registro por participação, com um único profissional e o ID do evento original. Formulários são associados à chave composta cidadão/evolução na mart. Perfis Centro POP e CREAS por unidade são reconstruídos nos intermediates correspondentes antes de anexar destinatários.

O commit `03178e0` removeu as antigas sete consultas por assunto. A descrição do PR ainda as menciona. Naquele head, a consulta usava uma única transformação sobre a mart, então com uma linha por cidadão.

Não foi acessada a fonte real do relatório. Portanto, a configuração que efetivamente ocasionou o incidente não foi comprovada. A falha potencial no consumo da mart está demonstrada pela construção do SQL; não se atribui ao relatório uma configuração presumida.

## Permissões preservadas

`int_filtro_email_unidades` agrega por identificador da unidade. Combina CAS mapeado, e-mail da unidade e planilha; normaliza destinatários, elimina vazios e repetições. A planilha é relacionada por nome normalizado, pois não contém uma chave utilizável nesse contrato. O código interrompe a consulta se nomes homônimos recebem permissões da planilha ou se configurações da mesma unidade conflitam.

CAS desconhecido usa `gdados.smas@prefeitura.rio`. O usuário confirmou nesta tarefa que esse fallback é administrativo legítimo. Ele foi preservado no escopo existente, sem conceder administração universal. Unidade ausente ou sem correspondência não ganha acesso pelo consumo seguro; o registro precisa ter unidade e destinatário explícitos. A macro legada `aplicar_filtro_email` enriquece capturas por nome, mas o consumo usa `email_filtro` por identificador publicado na mart, não o e-mail legado consolidado da raiz.

As permissões refletem o estado da última publicação da mart. Revogações exigem atualização desses metadados; conferir a atualização antes da ativação do relatório. Esta mudança não altera agendamentos ou IAM.

## Avaliação da implementação

| Alternativa | Vantagens | Custos/riscos |
|---|---|---|
| Consulta personalizada com DS_USER_EMAIL (escolhida) | Mantém mart e grão; sem nova tabela; testa a mesma transformação com fixtures | Projeção extensa para preservar o schema; leitura de aproximadamente 3 GB; compatibilidade de arrays deve ser validada no conector |
| Fonte por registro/unidade | Filtro nativo com um endereço por linha; projeções simples por assunto | Altera consumo dos gráficos; exige expansão dos destinatários e novos grãos/fontes |

A macro `prontuario_consumo_seguro` aplica comparação exata e normalizada por registro nos sete arrays protegidos. Identidade ausente, lista ausente ou unidade ausente negam acesso. Um erro de schema/permissão falha a consulta e não retorna a mart como fallback. A ordem e multiplicidade dos itens restantes são preservadas pelos offsets. A linha do cidadão só existe se algum array protegido permanecer preenchido. Os destinatários retornados são substituídos pelo e-mail do próprio visualizador.

Quantidades totais e por serviço são recalculadas. `nome_unidade` lista somente unidades dos atendimentos autorizados; não pretende listar todos os outros assuntos. Último ciclo segue a prioridade de ciclos abertos e os desempates da dimensão existente. Os demais resumos de domínio sem proveniência suficiente ficam nulos. O filtro nativo por e-mail é desnecessário para esta consulta; se usado adicionalmente, a raiz agora contém um endereço individual.

## Política dos campos

| Caminho | Regra |
|---|---|
| `usuario.*` | Cadastro compartilhado após existir registro autorizado, exceto as supressões abaixo |
| `usuario.violacoes`, `flag_possui_violacao_direito` | Vazio/nulo: misturam cadastro com evoluções de várias unidades |
| `usuario.nome_unidade_referencia` | Nulo: chave de referência não está publicada |
| `usuario.logradouro`, `numero`, `complemento`, `referencia_ou_comunidade` | Nulos: captura de atendimentos sem proveniência publicada |
| `familias.*` | Composição/vínculos compartilhados; `servicos` vazio por perda de chave de unidade na projeção |
| `projetos_sociais` | Cada vínculo filtrado pela própria unidade/lista |
| `atendimentos.registros` | Participação filtrada com profissional e CBO preservados. Para contar eventos por usuário e unidade, deduplicar por `id_atendimento_modulo` dentro da linha. `projetos_sociais_familia` fica nulo por ser resumo sem proveniência. |
| `evolucoes.registros` | Evento filtrado com descrição, responsável e seus formulários |
| `centro_pop.registros`, `historico_mensal` | Cada perfil/entrada mensal filtrado pela unidade; contagens internas pertencem à unidade do perfil |
| Outros escalares de `centro_pop`, `creas` | Nulos tipados; não selecionar arbitrariamente um perfil visível para substituir um global |
| `creas.registros` | Perfis por unidade filtrados, incluindo MSE/PETI/SCFV e participações derivadas |
| `alta_complexidade.registros` | Ciclos filtrados; escalares recalculados do último ciclo visível |

O SQL enumera explicitamente os campos de primeiro nível e dos registros autorizáveis. Profissionais, formulários, composição e benefícios cadastrais mantêm seus contratos internos. O teste de schema detecta mudanças no contrato antes de uma nova entrega. Arrays suprimidos são tipados e vazios; escalares suprimidos são NULL do tipo publicado.

## Reprodução e configuração

Usar dbt Core do ambiente do projeto; o executável global é Fusion e rejeita configurações antigas alheias a esta alteração.

```bash
.venv/bin/dbt parse --project-dir queries --profiles-dir queries --target ci
.venv/bin/dbt compile --project-dir queries --profiles-dir queries --target ci --select looker_prontuario_seguro --no-introspect --no-populate-cache
.venv/bin/python queries/validation/validar_consumo_seguro.py --real
sqlfluff lint queries/analyses/looker_prontuario_seguro.sql queries/macros/dominios/prontuario_eletronico/consumo_seguro.sql
sqlfluff lint --config .sqlfluff --dialect bigquery --templater raw queries/target/compiled/queries/analyses/looker_prontuario_seguro.sql
git diff --check
```

A compilação resolve `ref` para o target escolhido. A relação candidata padrão pode não existir: conferir o FROM antes de conectar. Foi entregue também um SQL compilado em `/opt/pipelines/exports/pr163-isolamento/looker_prontuario_seguro.sql`, apontando para a tabela existente `rj-smas-dev.abordagem.planilha_unificada`. Isso não comprova que essa é a tabela utilizada pelo relatório; confirmar a origem real.

1. No relatório, abrir Recursos → Gerenciar fontes de dados adicionadas. Inventariar cada fonte e cada gráfico, blend, extração e fonte reutilizável. Registrar consulta/tabela, credenciais, editores e configuração atual de Filter by email.
2. Na conexão BigQuery escolher Consulta personalizada, projeto de faturamento autorizado e colar o SQL compilado, sem delimitadores Markdown. Usar GoogleSQL e habilitar o parâmetro padrão **DS_USER_EMAIL** em Parameters. Não criar um parâmetro de identidade editável nem substituí-lo por controle do relatório.
3. Conferir campos, tipos e suporte a arrays/structs. Adaptar gráficos aos perfis por unidade para substituir escalares globais suprimidos. Não voltar a uma fonte irrestrita em caso de erro.
4. Migrar todos os gráficos e blends protegidos para a fonte segura; revisar extrações, cópias antigas e fontes auxiliares que podem reintroduzir dados globais.
5. Verificar credenciais da fonte e acesso direto ao dataset. Credenciais do proprietário precisam ficar limitadas aos editores responsáveis; visualizadores com acesso direto à tabela não estão isolados por essa consulta. Registrar o diagnóstico; alterações de IAM/credenciais estão fora desta entrega.
6. Testar contas reais de A, A+B, administração e desconhecida, consentimento negado, exportações e troca de conta. Confirmar que os filtros visuais não são a barreira de segurança e que B/C não aparecem por contagens ou campos de domínio.
7. Registrar o SQL ativo e as evidências sem dados pessoais. Encerrar como corrigido e validado somente após esses testes autenticados.

## Referências oficiais

- [Filter by email address](https://docs.cloud.google.com/data-studio/filter-by-email-address): segurança por linha configurada na fonte.
- [Filter by multiple email addresses per row](https://docs.cloud.google.com/data-studio/filter-by-multiple-email-addresses-per-row): listas concatenadas não funcionam como um único endereço no filtro nativo.
- [Use parameters in a custom query](https://docs.cloud.google.com/data-studio/use-parameters-in-a-custom-query): DS_USER_EMAIL padrão identifica o visualizador autenticado mediante consentimento.

## Evidências da validação anterior

Os resultados abaixo registram a validação da versão anterior da consulta. Eles não comprovam a configuração atual do Looker nem a execução com o schema publicado mais recente.

- `dbt parse` e `dbt compile` com Core 1.12.5: aprovados; permanece aviso preexistente de caminho de configuração não utilizado.
- SQLFluff da consulta renderizada: aprovado, sem pular arquivo por tamanho. A regra ST06 fica desabilitada para preservar ordem pública das colunas.
- `git diff --check`: aprovado.
- Nove identidades simuladas e três configurações de arrays: aprovadas no BigQuery, usando a mesma macro da análise. As fixtures incluem A/B/C, destinatários explícitos, evento sem destinatário, evento sem unidade apesar de conter destinatário, histórico mensal, eventos familiares, dois profissionais idênticos no evento e formulários aninhados.
- Comparação real: um cidadão multiunidade do CI, selecionado em leitura; escolheu-se um destinatário de escopo parcial. Todos os sete arrays foram comparados integralmente com a seleção esperada dos registros originais, após as únicas substituições intencionais de destinatários e resumo familiar. Nenhum dado pessoal integra as evidências salvas.
- Schema de saída: nomes, ordem, tipos e caráter repetido iguais ao schema físico de CI. Nulabilidade dos campos suprimidos é intencional.
- Dry run completo no CI: 3.033.777.967 bytes. Uma execução real consumiu esse volume e aproximadamente 1.176 slot-ms; valores variam com dados/cache. A seleção da amostra originalmente processou 3.728.364.336 bytes; repetições utilizaram cache. Esta medida não representa a carga de todos os gráficos do relatório.
- SQL pronto apontando para `abordagem`: validado pelo dry run do BigQuery. Usar esse artefato somente após confirmar que é a origem correta do relatório.

| Cenário | Evidência/resultado |
|---|---|
| Somente A, cidadão em A/B/C | Aprovado: apenas registros/profissionais/formulários de A |
| A+B | Aprovado: duas unidades, C ausente |
| Administrativo explícito | Aprovado na fixture: A/B/C, sem unidade sem configuração |
| Maiúsculas/espaços e delimitadores | Aprovado: comparação normalizada de tokens exatos |
| Desconhecido, parcial, vazio e NULL | Aprovado: zero cidadãos |
| Evento sem destinatário ou unidade | Aprovado: item negado |
| Arrays vazios | Aprovado: cidadão ausente |
| Apenas evolução ou apenas histórico mensal | Aprovado: cidadão incluído sem depender de atendimentos |
| Escalares globais e serviços familiares | Aprovado: campos suprimidos e arrays vazios |
| Múltiplos profissionais | Aprovado: duplicidade preservada e contagem recalculada |
| Família/formulários | Aprovado sintético; associações reais dependem da integridade das chaves da mart |
| CAS/unidade/planilha, conflitos e homônimos | Linhagem e proteções inspecionadas; não foram injetados conflitos nas fontes reais nem executadas materializações para reconstruir permissões |
| Fallback administrativo real | Regra preservada por decisão funcional; não foi usado login autenticado administrativo |
| Consentimento, credenciais, fonte ativa e acesso direto | Pendente de acesso/configuração do Looker e conferência administrativa |

Não há cenário de isolamento reprovado na execução final. Uma execução intermediária detectou alteração de ordem das colunas pelo lint; foi corrigida e a equivalência do schema foi novamente comprovada. Os testes com e-mail controlado não substituem a validação autenticada no Looker.

Arquivos de implementação: análise `analyses/looker_prontuario_seguro.sql`, macro `macros/dominios/prontuario_eletronico/consumo_seguro.sql`, teste de leitura `validation/validar_consumo_seguro.py`, este relatório e esclarecimento da descrição YAML da mart. Não foram alteradas regras de autorização nem modelos de dados publicados.
