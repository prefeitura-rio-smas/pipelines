# PR #163 — contrato de paridade com a consulta programada

A seleção de atendimentos agora acontece em `int_atendimentos_bi`, a partir dos
raws, antes da agregação por evento e usuário. A construção não lê
`dashboard_acolherio.dev_atendimentos`: essa tabela é somente o oráculo opcional
de validação. O filtro de acesso continua vindo da configuração da programada.

## Regras preservadas

- Módulo usuário: LEFT JOIN com os vínculos familiares e filtro `datsaida IS NULL`,
  inclusive sua consequência de excluir pessoas que só têm vínculos encerrados.
- Mesmos filtros de nomes de usuário, unidade e cadastrante. Cadastrante ausente
  é excluído pela semântica de `NOT LIKE` sobre NULL. Não há filtro extra de
  cancelamento de atendimento.
- Mesma expansão do profissional principal e da lista compartilhada, joins de CBO,
  classificação, nome ajustado, projetos da família e composição de email.
- Mesmas partições dos dois rankings, inclusive a chave legada sem módulo.
  Essa chave serve somente à seleção; a identidade do evento contém o módulo.

O desempate das duas implementações passa a ser data de cadastro, módulo,
sequencial do atendimento, usuário, família, código CBO, profissional e JSON da
linha completa, em ordem crescente com NULL primeiro. Cópias integralmente iguais
são removidas antes dos rankings. Assim, não existe disputa entre cópias idênticas
nos dois `ROW_NUMBER` calculados antes do `QUALIFY`.

O SQL independente com os mesmos desempates está em
`queries/operations/sql/acolherio_atendimentos_programada.sql`. Não é um modelo
executado pelo dbt. Sua publicação na consulta agendada é uma etapa separada;
os horários não devem ser alterados.

## Granularidade e campos para o Looker

Uma linha por usuário continua contendo listas de eventos e profissionais.
Eventos sem profissionais sobreviventes às regras legadas são excluídos. Um
profissional desconhecido que represente uma linha real aceita continua na lista,
com seus campos desconhecidos vazios; não é um item fictício de ausência.

| Métrica/filtro legado | Campo ou operação nova |
|---|---|
| Quantidade de eventos | `atendimentos.quantidade` ou quantidade de registros |
| Quantidade de linhas da programada | `atendimentos.quantidade_linhas_bi`, sem expandir outros arrays |
| Linhas filtradas por tipo/CBO | Expandir registros e seus profissionais; contar os profissionais selecionados |
| Tipo de atendimento | `atendimentos.registros.profissionais.classificacao_atendimento` |
| CBO original/normalizado | `profissionais.cbo_principal_descricao` / `profissionais.profissional_cbo` |
| Nome ajustado do atendimento | `profissionais.nome_atendimento` |
| Projetos da família do atendimento | `atendimentos.registros.projetos_sociais_familia` |
| Família, data, unidade e acesso | Campos do mesmo `atendimentos.registros` |

Recepção tem precedência sobre as quatro ocupações técnicas da programada. Isso
vale para todos os serviços. A classificação do evento é a dos seus profissionais
quando única e `Misto` quando divergente; métricas por classificação devem usar o
campo do profissional. As quantidades por serviço existentes continuam contando
eventos. Projetos cadastrais por usuário não substituem os projetos da família do
atendimento. Famílias vazias e identidade de visualizador vazia não são contadas.

Não expandir famílias cadastrais, evoluções ou formulários junto com os
profissionais para contar atendimentos: isso multiplica as linhas.

## Causas das diferenças anteriores

Na comparação congelada de 03/11, os 7.633 eventos extras se explicam por:

| Causa | Eventos |
|---|---:|
| Mesmo grupo que a programada deduplica, incluindo um caso com colisão de chave | 7.494 |
| Colisão de número/profissional na chave sem módulo, sem coincidência do grupo | 99 |
| Usuários apenas com vínculos familiares encerrados | 36 |
| Unidade cujo nome contém TESTE | 4 |

Os dois últimos grupos foram conferidos também nas fontes congeladas da validação
nova. A divergência de classificação vinha da precedência de técnico por qualquer
CBO iniciado em 2, substituída pela regra por linha profissional da programada.

## Validação e limites

A fixture `validar_selecao_atendimentos_bi` executa a macro completa e cobre
profissionais compartilhados/repetidos, múltiplos CBOs, precedência de recepção,
deduplicação natural, colisão entre módulos, operador ausente, usuário de teste,
vínculo encerrado, cancelamento, profissional desconhecido e projetos cancelados,
inativos e sem rótulo conhecido.

`pu_selecao_bi_paridade` compara o multiconjunto dos 39 campos da programada com a
seleção dbt. É habilitado por `pu_validar_programada: true` somente quando as duas
execuções usam as mesmas fontes congeladas e o SQL com desempates estáveis.
Comparar fontes vivas atualizadas em horários diferentes não comprova paridade.

`pu_linhas_bi_preservadas` compara seleção e structs, incluindo identidade,
família, profissionais, CBO, classificação, projetos, datas e acesso;
`pu_quantidades_bi` verifica as duas quantidades. Esses testes são permanentes.
Os testes anteriores de eventos, formulários, famílias e Centro POP continuam.

A validação foi isolada em `rj-smas-dev.ci_pr163_align_20261105_173226`. Os raws
necessários à seleção foram reconstruídos de snapshots dos brutos, e a dimensão
de usuários foi reconstruída. Outros pré-requisitos foram congelados da base de
validação anterior. A supertabela é construída integralmente pelo dbt, sem aplicar
um patch sobre a tabela publicada.

**Limitação de acesso:** a credencial da sessão não consegue ler o Google Sheets
usado por `filtro_email_dev` (erro de credenciais Drive). Para o ensaio, a parte
observável da configuração foi reconstruída da saída publicada, compartilhada
pelas duas implementações. Isso valida igualdade sob essa configuração, mas não
homologa configurações da planilha que não aparecem na saída. A publicação
coordenada exige repetir a comparação com uma cópia autorizada da configuração
completa, confirmar o acesso à planilha pela credencial que executará o dbt,
revisar o delta contra a versão anterior e migrar os campos dos cartões.
Até lá, os destinos publicados e a consulta agendada permanecem inalterados.

## Resultado executado em 05/11/2026

- Build dbt integral da candidata: **4 tabelas e 31 testes, todos aprovados**.
- Mais **9 testes dos contextos/intermediários aprovados**; a versão final do
  teste de paridade também foi reexecutada e aprovada.
- Paridade da seleção: **zero diferenças no multiconjunto dos 39 campos**.
- Preservação dos structs, quantidades, famílias, eventos, formulários e perfil
  Centro POP: testes aprovados no mesmo build.
- SQLFluff: zero violações nos SQLs novos de modelo, fixture e consulta revisada,
  e no modelo da supertabela alterado. Depreciações dbt preexistentes permanecem.

| Métrica na mesma base congelada | Programada revisada | Structs novos |
|---|---:|---:|
| Linhas de atendimento/profissional | 1.079.158 | 1.079.158 |
| Eventos por usuário + atendimento/módulo | 1.049.241 | 1.049.241 |
| Famílias elegíveis, com acesso do visualizador de validação, 01/01–02/11/2026 | 254.853 | 254.853 |

A população da supertabela coincide com os **598.737 usuários** da dimensão
reconstruída. Houve **zero recortes divergentes** por mês, unidade, serviço,
classificação e projetos, comparando linhas, eventos e famílias. Visualizadores
vazio e desconhecido retornaram zero famílias. Os totais diferem da fotografia de
03/11 porque as fontes desta execução são outras; a comparação é entre as duas
implementações usando as mesmas fontes, não contra um total histórico fixo.

A validação continua sujeita à limitação do filtro de emails descrita acima.

### Delta contra a tabela publicada anterior

O total de linhas continua 1.079.158, mas a seleção estabilizada não deve ser
confundida com a fotografia antiga: 3.917 identidades de linha saem e outras
3.917 entram. Nas 1.075.241 linhas comuns, 33 famílias, 10.875 CBOs e 2.846
classificações diferem; emails e projetos coincidem. Essa comparação usa a foto
publicada e as fontes congeladas do ensaio, e deve ser revisada na homologação.
A paridade exata demonstrada acima é entre a consulta **revisada** e o dbt.

Os testes permanentes de famílias comparam os structs à seleção dbt, evitando
falhas por defasagem da consulta agendada. Somente o teste opcional dos 39 campos
consulta o oráculo materializado da programada.
