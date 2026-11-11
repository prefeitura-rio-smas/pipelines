# Fontes por unidade no Looker Studio

As consultas `looker_*.sql` são as fontes de conexão. As tabelas de apoio contêm várias unidades: conectá-las diretamente não aplica isolamento por visualizador.

1. Substituir projeto/dataset pelo ambiente validado (na revisão, o dataset isolado informado no PR).
2. Criar uma fonte BigQuery por consulta e habilitar o parâmetro nativo `@DS_USER_EMAIL`. O visualizador deve estar autenticado e consentir em compartilhar o e-mail.
3. Usar a consulta de atendimentos para contar eventos; linhas profissionais para reproduzir a referência programada; evoluções para consultar histórico; perfil/unidade para cadastro, perfis e quantidades por unidade.
4. O filtro usa correspondência exata de e-mail normalizado e `EXISTS`. Não trocar pelo filtro do campo `email` da tabela central, não usar parâmetros editáveis de e-mail nem remover a cláusula de autorização. Não expor a tabela de permissões no relatório.
5. Para indicadores de cidadãos em várias unidades, usar contagem distinta de `id_usuario`. Na fonte perfil/unidade, somar quantidades de eventos/linhas/evoluções; não repetir esses contadores nas fontes de eventos.
6. A fonte de eventos não expõe a lista de profissionais; os formulários das evoluções são texto legível em `formularios_texto`. As fontes de consumo não têm campos repetidos. Contadores do perfil/unidade representam todo o histórico; para períodos, usar as datas das fontes de atendimentos ou evoluções. Não juntar fontes de granularidades distintas por cidadão no Studio. Seletores de unidade devem usar a própria fonte autorizada.
7. Validar com logins de unidade A, unidade B e gestão: uma pessoa atendida em ambas só apresenta os registros de cada unidade autorizada. Gestão vê as unidades permitidas sem duplicação. Um login sem permissão não recebe linhas. Registros sem unidade ficam fora das fontes.
8. Somente após aceite em staging, substituir as fontes do painel operacional. O prontuário central e os nomes físicos existentes não mudam nesta entrega.

O cadastro acompanha os atributos escalares já normalizados do prontuário central, incluindo idade, faixa etária, documentos e flags. Listas cadastrais, famílias, serviços, projetos sociais e referências a unidades de outras origens não são transportados. A ampliação dessas listas exige origem por unidade validada.

A variável de execução `pu_bi_planilha` permite apontar uma captura materializada da planilha durante testes reproduzíveis, sem depender de credenciais Drive locais. Em staging, a ausência dessa variável mantém a fonte vigente `pu_filtro_email()`; permissões precisam ser reconstruídas após mudanças da planilha. Publicar a candidata congelada como se tivesse permissões atuais não é permitido.

Para reconstruir as fontes com permissões vigentes, executar no ambiente de staging com as credenciais usuais da pipeline:

```sh
dbt build --target staging --select +tag:prontuario_bi_unidade --project-dir queries --profiles-dir queries
```

O seletor com `+` inclui as dependências necessárias. A publicação inicial das tabelas de revisão pode usar a candidata validada, desde que a captura de permissões seja conferida com a planilha vigente imediatamente antes da publicação. Isso não migra as fontes do relatório operacional.

As quatro fontes usam `id_usuario` STRING e `id_unidade` INT64. Usar essas chaves para os controles entre fontes, mantendo os identificadores de campo correspondentes no Studio.

## Histórico mensal Centro POP

`centro_pop.historico_mensal` preserva uma linha por cidadão na planilha, com entradas por competência/unidade desde novembro de 2026. A consulta `looker_centro_pop_historico.sql` explode somente esse histórico e autoriza cada entrada pela unidade; não juntar a outras listas. Selecionar os campos do histórico para gráficos mensais, não os campos do perfil vigente.

A prévia contém todo cidadão com histórico na unidade, inclusive com zero eventos no mês. Atendimentos, evoluções e participações são contados separadamente; formulários não criam atendimentos adicionais. O perfil inclui ocorrências até o período, usando apenas o vínculo explícito da unidade. Datas ausentes não recebem mês de ocorrência artificial. O estado cadastral da origem é observado na captura, sem promessa de reconstrução de alterações antigas.

Fechar manualmente somente após o fim da competência. No mesmo target/schema de publicação, executar `dbt build --select tag:centro_pop_mensal --vars '{centro_pop_mes: "2026-11-01"}'` e conferir os testes. Em seguida, `dbt run-operation fechar_centro_pop_mes --args '{mes: "2026-11-01", id_fechamento: "centro-pop-202611-v1"}'`. Repetir o mesmo identificador é idempotente. Para corrigir, usar outro identificador e `nova_versao: true`; versões anteriores ficam preservadas. Concorrência é controlada por uma transação com atualização de uma linha compartilhada; caso haja conflito, repetir a operação com o mesmo identificador.

Após fechar, reconstruir a prévia corrente e a planilha com `dbt build --select tag:centro_pop_mensal prontuario_eletronico` sem a variável de competência. As reconstruções diárias não removem snapshots. A lista mostra a última versão publicada de cada competência; auditoria das anteriores na tabela `centro_pop_snapshots`. Usar o target de staging; não executar full refresh manual da tabela de snapshots.
