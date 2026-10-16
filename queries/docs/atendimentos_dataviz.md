# Usuários e seus atendimentos no dataviz geral

`mart_atendimentos` (alias `atendimentos`) entrega **uma linha por usuário que
possui atendimentos**, com todos os atendimentos em `ATENDIMENTOS`, um
`ARRAY<STRUCT>`. Os dados cadastrais, sociais e de comorbidades ficam no
usuário; cada atendimento mantém os dados de sua ocorrência e sua equipe.

Nenhum atendimento é excluído por testes, cadastro incompleto, vínculo
encerrado, cancelamento ou rank. A filtragem acontece no dataviz.

## Estrutura medallion

- Raw: leitura das fontes de atendimentos, famílias, profissionais e cadastro
  social. `raw_usuarios_informacoes_medicas` lê `gh_pac_info_med`.
- Intermediate: `int_atendimentos_completos` normaliza a equipe sem repetir
  IDs compartilhados; `int_atendimentos_dataviz` enriquece cada atendimento
  com sua família, projetos, serviços, perfil social, flags e ranks individuais.
- Mart: agrupa os atendimentos por ID_USUARIO, sem remover ocorrências.
  O perfil cadastral atual é comum às ocorrências; um STRUCT completo,
  selecionado pelo menor ID de atendimento com módulo, fornece o perfil externo.

Estrutura resumida:

```text
ID_USUARIO / ID_USUARIO_SK
NOME_USUARIO, documentação, endereço, idade, sexo, raça/cor
VIOLACOES, BENEFICIOS, DEFICIENCIAS, FLAG_SITUACAO_RUA
FLAG_POSSUI_COMORBIDADE, COMORBIDADES, data, local e detalhes
VINCULOS_FAMILIARES
QUANTIDADE_ATENDIMENTOS
ATENDIMENTOS[]
  ID_ATENDIMENTO / ID_ATENDIMENTO_MODULO / ID_ATENDIMENTO_SK
  data, hora, unidade, família
  PROJETOS_SOCIAIS / SERVICOS_SOCIOASSISTENCIAIS e históricos
  flags de elegibilidade, teste, cadastro e cancelamento
  dados do profissional principal e ranks
  PROFISSIONAIS[]
    ID, nome, CBOs, classificação, flag_principal, atendimento_unico_rank
```

Projetos e serviços dependem da família do atendimento e, por isso, não são
reduzidos a um único valor por usuário. O array de atendimentos é ordenado
por data, hora e ID com módulo. IDs de atendimento incluem o módulo para
preservar ocorrências com sequências iguais nas duas fontes.

## Consultas e contagens

- `COUNT(*)` na mart conta usuários.
- `SUM(QUANTIDADE_ATENDIMENTOS)` conta atendimentos antes de filtros.
- Depois de `UNNEST(u.ATENDIMENTOS)`, `COUNT(*)` conta atendimentos.
- Depois de expandir também `a.PROFISSIONAIS`, `COUNT(*)` conta participações;
  para contar atendimentos use `COUNT(DISTINCT a.ID_ATENDIMENTO_MODULO)`.
- Usuários atendidos após filtros: `COUNT(DISTINCT u.ID_USUARIO)`.

Exemplo de atendimentos por tipo de unidade:

```sql
SELECT a.TIPO_UNIDADE,
  COUNT(*) AS atendimentos,
  COUNT(DISTINCT u.ID_USUARIO) AS pessoas,
  COUNT(DISTINCT IF(u.FLAG_POSSUI_COMORBIDADE = 'Sim', u.ID_USUARIO, NULL))
    AS pessoas_com_comorbidade
FROM `projeto.dataset.atendimentos` AS u,
  UNNEST(u.ATENDIMENTOS) AS a
WHERE a.FLAG_ELEGIVEL_DATAVIZ
GROUP BY a.TIPO_UNIDADE;
```

`FLAG_ELEGIVEL_DATAVIZ` informa o universo da V2: nomes de usuário, unidade e
cadastrante não nulos e sem TESTE, com vínculo familiar ativo ou ausente no
módulo usuário. Nunca é nula e não filtra a mart. Flags específicas continuam
nos níveis correspondentes: as do usuário no cadastro externo, as da ocorrência
no atendimento e as do profissional no integrante da equipe.

O `atendimento_unico_rank` no atendimento é o rank do principal.
Para analisar participações com as regras da V2, filtre o rank **do integrante**:

```sql
SELECT p.nome, p.tipo_atendimento, COUNT(*) AS participacoes
FROM `projeto.dataset.atendimentos` AS u,
  UNNEST(u.ATENDIMENTOS) AS a,
  UNNEST(a.PROFISSIONAIS) AS p
WHERE a.FLAG_ELEGIVEL_DATAVIZ
  AND p.atendimento_unico_rank = 1
GROUP BY p.nome, p.tipo_atendimento;
```

Não filtre antes o rank do principal: isso pode eliminar compartilhados
válidos. Os ranks individuais usam profissional cadastrado, hora, data,
nome original do atendimento, usuário e nome da unidade. Priorizam o universo
V2 e desempatam por cadastro, sequência e módulo. Se outros filtros mudarem
o representante do grupo, recalcule o rank após esses filtros no dataviz.

## Regras preservadas

Mantém `CPF informado`/`CPF não informado`, `Atendimento Recepção`,
`CRAS/CREAS - Recepção - Ação CadÚnico`, ESCRITÓRIO SOCIAL e LARES CARIOCAS.
Os dados escalares profissionais de cada atendimento representam o principal;
o vínculo usa o ID do profissional, nunca o login do cadastrante.
A classificação usa o CBO exibido, escolhido por código e descrição. Todos
os CBOs permanecem nos detalhes. CAS GE mantém `gdados.smas@prefeitura.rio`,
conforme a tabela programada atual.

A família prioriza vínculo ativo, entrada mais recente e maior ID. O histórico
e os vínculos encerrados são preservados. A idade usa mês/dia para corrigir
o erro de DAYOFYEAR da V2 em aniversários de nascidos em anos bissextos.
A V2 tem desempates incompletos; compare grupos analíticos, não apenas os
IDs escolhidos em uma execução antiga.

## Perfil social e comorbidades

Projetos/programas ativos da família e serviços não cancelados ficam em cada
atendimento, com histórico completo. PAIF e PAEFI têm nomes validados; outros
serviços preservam seus IDs como não mapeados.

Violações, benefícios, deficiência, situação de rua e comorbidades ficam no
usuário, com flags, descrições e procedência. Comorbidades incluem indicador,
texto livre, data, local de tratamento e detalhes de `gh_pac_info_med`.
São informações do **cadastro atual**, inclusive para atendimentos antigos,
e não necessariamente condições na data do atendimento. Ausência de informação
não significa Não; texto livre não é convertido em diagnóstico.

O alias segue os schemas/ambientes medallion do dbt. A implantação da fonte do
dataviz e a retirada do agendamento existente são etapas posteriores.

## Verificações

Os testes verificam usuários únicos, contagem dos arrays, unicidade e cobertura
dos IDs de atendimento das duas fontes, integridade dos ranks e a representação
dos grupos elegíveis de profissionais, inclusive compartilhados.

Na validação de 16/10/2026, foram obtidos 310.199 usuários únicos e 1.001.937
atendimentos nos arrays, com no máximo 878 atendimentos por usuário. Expandir
os structs e reconstruir a projeção anterior recuperou todos os atendimentos,
sem diferenças em nenhum campo. Os 10 testes dbt compilados passaram no BigQuery.
Os 655 registros fora do universo opcional da V2, 633 marcados como teste e
7.440 com rank do principal maior que 1 continuam preservados nos arrays.

A credencial local não acessa o Drive. Na validação local, e-mails por unidade
usam os valores materializados no dev; o modelo e o CI leem a planilha original.
