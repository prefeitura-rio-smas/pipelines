# Validação da supertabela — 16/10/2026

Branch: `feat/acolherio-atendimentos-medallion-prefect`, PR #147.
A referência anterior por usuário é o commit `96faed410b846aa0b4b112e81deb4e34b4963219`.

## Ambiente e limites da execução

Construção real no BigQuery em
`rj-smas-dev.ci_147_socioassistencial_20261016.mart_socioassistencial_unificada`,
dataset isolado, com expiração de dois dias. Sem escrita em produção ou alteração
de consumidores/agendamentos. Leituras de origem e validações usam o projeto de
cobrança rj-smas-dev. A consulta compilada é válida e a construção foi executada.

A credencial OAuth local não acessa a planilha externa do Drive. Somente no SQL
local de validação, essa relação foi substituída pelos e-mails por unidade já
materializados em `rj-smas-dev.relatorio.atendimentos`. O código versionado mantém
a fonte original; o CI usa as credenciais próprias e lê o Drive. Portanto,
validação local não certifica a atualização dos e-mails da planilha externa.

Os raws e fatos físicos foram expandidos a partir do dbt compilado para ler as
fontes efetivas, evitando depender de views dev desatualizadas. O mapa de identidade
ArcGIS existente foi reutilizado materializado, sem executar nova deduplicação.
Os resultados agregados e IDs de jobs ficam em
[socioassistencial_validacao_resultados.json](socioassistencial_validacao_resultados.json).
Não há dados pessoais nesse artefato.

## Volumes e integridade

| Natureza | Registros | IDs distintos |
|---|---:|---:|
| ATENDIMENTO | 1.001.937 | 1.001.937 |
| ACOMPANHAMENTO_MENSAL | 21.533 | 21.533 |
| VINCULO_SERVICO | 5.188 | 5.188 |
| ATIVIDADE_COLETIVA | 15.752 | 15.752 |
| FORMULARIO | 379.129 | 379.129 |
| ENCAMINHAMENTO | 76.041 | 76.041 |
| ABORDAGEM | 542.563 | 542.563 |

Total técnico: 2.042.143 linhas; não é um indicador socioassistencial somável.
São 81 formulários de grupo além dos formulários administrativos/familiares/de
usuário, preservados sem explosão dos participantes. Os 5.203 registros brutos de
serviço geram 5.188 observações distintas por conteúdo; cópias exatas permanecem
quantificadas. Não há intervalos invertidos nem início nulo na fonte auditada.
O primeiro ingresso disponível é 05/09/2025; isso não prova ausência de serviços
antes dessa data.

13.419 registros mensais não possuem atendimento da família naquela competência.
Eles permanecem publicados: acompanhamento não depende de atendimento no mês.

Unidades originais ausentes: 11.336 registros mensais, 2.110 vínculos, 116 presenças
e 371.303 ocorrências ArcGIS. Nenhuma unidade é inventada. A responsabilidade
ArcGIS também permanece em nome/CAS/serviço informados. Unidade não identificada
não é classificada automaticamente como conveniada.

## Comparações executadas

### PR anterior e Planilha Unificada

- Paridade de chaves de atendimento, usuário/família/unidade, cancelamentos,
  elegibilidade e equipe completa com CBOs/ranks: zero divergências.
- Cobertura das duas fontes de atendimento: nenhum ID descartado.
- Paridade do multiconjunto de contextos da planilha, com todos os campos exceto
  Nº: zero divergências e nenhuma multiplicação. O SQL original anterior à
  extração de intermediários foi executado como baseline independente.
- Matriz documental: 50/50 posições do YAML mapeadas. Disponibilidade de coluna
  não equivale a coleta preenchida nem a histórico cadastral.

### Consulta programada física atual

Foram comparadas participações elegíveis por profissional com rank 1, e não
atendimentos brutos com linhas profissionais:

| Métrica | Programada | Nova base com recorte V2 |
|---|---:|---:|
| Participações/grupos analíticos | 1.022.073 | 1.022.187 |

Nos 1.822 grupos unidade × competência, 35 diferem, com saldo de +114. A comparação
das chaves analíticas completas não encontrou grupo exclusivo da programada e
encontrou 114 exclusivos da nova base. Não há perda de grupo legado. Preservação
de módulo e desempate determinístico explicam a recuperação de ocorrências que a
chave/rank antigos podem suprimir. Esse resultado não deve ser confundido com os
1.001.937 atendimentos originais nem com uma certificação de cada filtro oficial.

### RMA CRAS — setembro de 2026

O modelo existente foi recalculado para 2026-09 sobre as mesmas fontes, em tabela
temporária, sem substituir sua mart pública. Comparação por cada uma das 48 unidades:

| Indicador | Resultado |
|---|---|
| C1 | 33.442 nos dois resultados; zero unidades divergentes |
| C6 | Zero unidades divergentes |
| A2 | Zero unidades divergentes |
| A1 | 13 unidades divergentes; 2.072 no modelo existente × 1.949 na projeção por unidade original |

A1 tem diferença conceitual de atribuição: o modelo existente seleciona um vínculo
por família e usa serviço → operador atual → atendimento CRAS recente como fallback.
A base geral conserva os vínculos por unidade original e inclui famílias mesmo
sem membro cadastrado. Não foi imposta igualdade artificial. A regra de atribuição
e o recorte familiar precisam de homologação antes de migrar esse indicador.
B1–B6 e D1–D7 têm fontes/caminhos documentados; não foram declarados numericamente
homologados por esta comparação de C1/C6/A1/A2.

### ArcGIS

- 542.563 GlobalIDs distintos; nenhuma ocorrência perdida.
- 39.998 datas recuperadas de milissegundos textuais `.0`; duas continuam sem data.
- O parser legado perdia competência de 40 mil registros; o novo mantém a origem
  e uma flag de recuperação para explicar diferenças históricas de contagem.
- Join legado de ficha por objectid: zero correspondências. Chave verificada
  parentrowid/uniquerowid: 542.549 ocorrências associadas, 14 sem ficha.
- A fonte de fichas possui duas chaves normalizadas ambíguas, em 13.499 linhas.
  Somente chaves únicas e não vazias podem fornecer controles ao registro; não
  se escolhe arbitrariamente uma ficha. As ocorrências continuam preservadas.
- Normalização já existente `clean_name` permite conciliação unívoca de unidades,
  sem fuzzy matching, sem usar referência territorial como unidade responsável.
- Clusters anuais não são descartados como se fossem contatos repetidos inválidos.
  Não se certificou a elegibilidade oficial de cada repeat nem a deduplicação
  municipal de pessoas entre anos/sistemas.

## Testes e revisão

Executados localmente: `dbt compile`, dez testes via `dbt test` no dataset isolado
(seis genéricos da mart e quatro singulares de integridade/regras), e os testes
singulares compilados de cobertura, paridade e vínculo ArcGIS no BigQuery.
As evidências finais registram falhas por teste e os respectivos jobs. SQLFluff
foi executado nos SQLs criados/modificados, **sem pular arquivos grandes**.

Os testes cobrem unicidade global e chave de origem, competências, uma equipe
com exatamente um principal por atendimento, campos de sujeito/serviço mensais,
estoque no fechamento, fronteiras de datas, cobertura independente de presença de
atendimento, preservação dos compartilhados, contexto Centro POP e chave de ficha.

A revisão removeu duplicação das classificações de serviços/atividades, evitou
join com a mart ArcGIS de e-mails, manteve contextos com uma linha por chave,
separou extração documental/formulários da montagem da planilha e usou projeções
explícitas na união dos fatos. O arquivo maior da planilha reaproveita sua
transformação existente; não contém uma reimplementação paralela das 50 colunas.
Não foram introduzidas dependências Python ou novos agendamentos.

## Pendências para homologação/migração

1. Homologar atribuição de unidade e seleção de vínculos PAIF/PAEFI, especialmente
   registros sem unidade e casos individuais, e comparar várias competências fechadas.
2. Identificar vítima/caso/data de violação no PAEFI; não assumir que todo membro
   familiar seja vítima ou que marcação atual prove condição no ingresso.
3. Conciliar inclusão, retornos e modalidades simultâneas em ciclos LA/PSC. Campos
   existem, mas os ciclos não estão certificados. PAI registrado também não prova
   acompanhamento ativo; validar ingressos/desligamentos e reingressos.
4. Disponibilizar histórico de eventos CadÚnico e de concessão/entrega de benefícios;
   snapshot, encaminhamento e prazo de validade não são eventos de inclusão/concessão.
5. Validar contato contabilizável e controles de exclusão ArcGIS, domínios dos perfis,
   unidade responsável e identidade entre anos/sistemas.
6. Resolver as lacunas de coleta da planilha e versionar fechamentos/extrações para
   reproduzir RMAs homologados sem reinterpretar cadastros atuais como históricos.
7. Validar Looker e aprovar migração dos consumidores; somente depois planejar a
   retirada dos produtos legados ou da consulta programada.
