# Atendimentos para o dataviz geral

`mart_atendimentos` (alias `atendimentos`) entrega **uma linha por atendimento**,
com o módulo incluído na identificação. Não exclui atendimentos por nomes de
teste, cadastro incompleto, vínculo familiar encerrado, cancelamento ou rank.
A filtragem analítica acontece diretamente no dataviz.

## Camadas

- Raw: leitura das fontes de atendimentos, famílias, profissionais e cadastro
  social. `raw_usuarios_informacoes_medicas` lê `gh_pac_info_med`.
- Intermediate: `int_atendimentos_completos` normaliza os profissionais sem
  repetir IDs compartilhados; `int_atendimentos_dataviz` reúne cada atendimento
  com a equipe, família, projetos, serviços e perfil social. Cada integrante
  recebe seu próprio rank sem remover participações.
- Mart: publica os atendimentos com as flags de seleção e o rank do principal.
  Não expande a equipe nem aplica os filtros da consulta programada V2.

## Profissionais e contagens

As colunas `PROFISSIONAL`, `PROFISSIONAL_ID`, `PROFISSIONAL_CBO`,
`TIPO_ATENDIMENTO` e `NOME_ATENDIMENTO` representam o **profissional principal**.
`PROFISSIONAIS` contém a equipe completa, incluindo os compartilhados, os CBOs,
a classificação, a indicação de principal e o rank de cada integrante.

- `ID_ATENDIMENTO` e `ID_ATENDIMENTO_MODULO`: sequência+módulo, únicos por
  atendimento. Não usam o formato legado sequência-profissional.
- `ID_ATENDIMENTO_SK`: chave técnica do atendimento, independente da equipe.
- `COUNT(*)`: quantidade de atendimentos, antes de expandir arrays.
- `COUNT(DISTINCT ID_USUARIO)`: quantidade de pessoas.
- Após expandir `PROFISSIONAIS`, `COUNT(*)` conta participações profissionais;
  para continuar contando atendimentos use `COUNT(DISTINCT ID_ATENDIMENTO)`.

## Filtros e ranks no dataviz

`FLAG_ELEGIVEL_DATAVIZ` informa se o atendimento pertence ao universo da V2:
nomes de usuário, unidade e cadastrante não nulos e sem TESTE, e vínculo
familiar ativo ou ausente no módulo usuário. A flag nunca é nula e não remove
linhas. As flags individuais de teste e cadastro continuam disponíveis.

`atendimento_unico_rank` no nível da linha é o rank do profissional principal.
`cbo_unico_rank` é 1 porque o CBO escalar já foi reduzido a um valor único.
Repetições analíticas permanecem publicadas com seus ranks maiores que 1.

Cada item de `PROFISSIONAIS` também tem `atendimento_unico_rank`, calculado
independentemente do principal. O grupo usa profissional cadastrado, hora,
data, nome original do atendimento, usuário e nome da unidade. O rank
prioriza registros elegíveis da V2 e desempata por cadastro, sequência e
módulo. Um registro inelegível não ocupa o primeiro rank de um grupo elegível.

Exemplo de filtro opcional de atendimentos pelo principal:

```sql
SELECT TIPO_UNIDADE, COUNT(*) AS atendimentos
FROM `projeto.dataset.atendimentos`
WHERE FLAG_ELEGIVEL_DATAVIZ
  AND atendimento_unico_rank = 1
GROUP BY TIPO_UNIDADE;
```

Para analisar participações com as regras da V2, expanda a equipe e filtre
o rank **do integrante**, sem filtrar antes pelo rank do principal:

```sql
SELECT p.nome, p.tipo_atendimento, COUNT(*) AS participacoes
FROM `projeto.dataset.atendimentos` AS a,
  UNNEST(a.PROFISSIONAIS) AS p
WHERE a.FLAG_ELEGIVEL_DATAVIZ
  AND p.atendimento_unico_rank = 1
GROUP BY p.nome, p.tipo_atendimento;
```

Filtrar o rank do principal antes de expandir a equipe pode eliminar
compartilhados válidos. Os ranks pré-calculados priorizam o universo da V2;
se outros filtros mudarem os representantes dos grupos, recalcule o rank
depois desses filtros na consulta do dataviz.

## Rótulos e desempates

Mantém `CPF informado`/`CPF não informado`, `Atendimento Recepção`,
`CRAS/CREAS - Recepção - Ação CadÚnico`, ESCRITÓRIO SOCIAL e LARES CARIOCAS.
O profissional é associado pelo ID, nunca pelo login do cadastrante.
A classificação usa o mesmo CBO mostrado para aquele profissional.

Para CAS GE, mantém `gdados.smas@prefeitura.rio`, conforme a tabela programada
atual, evitando perda dos e-mails do dataviz. Esse mapeamento não aparece no
trecho V2 fornecido, mas existe na tabela materializada atual.

- CBO: primeiro código ordenado, com descrição como desempate; todos os CBOs
  permanecem nos detalhes do profissional.
- Família: prioriza vínculo ativo e, entre ativos, entrada mais recente e
  maior ID da família. Vínculos encerrados e histórico são preservados.
- Idade: usa mês e dia, sem reproduzir o erro da V2 com DAYOFYEAR para
  aniversariantes nascidos em anos bissextos.

A V2 não define completamente os desempates de CBO, família e duplicatas.
Portanto, comparar os mesmos grupos analíticos é mais estável que exigir
o mesmo ID escolhido em uma execução da consulta antiga.

## Enriquecimento social e de saúde

- `PROJETOS_SOCIAIS`: projetos/programas ativos e não cancelados da família,
  com nomes do catálogo e IDs preservados no histórico.
- `SERVICOS_SOCIOASSISTENCIAIS`: serviços não cancelados da família; nomes
  validados de PAIF e PAEFI. Outros serviços preservam seus IDs como não mapeados.
- Violações, benefícios, deficiência e situação de rua: flags e descrições,
  com códigos e procedência nos arrays de detalhes.
- Comorbidades: indicador, descrição livre, data da informação, local de
  tratamento e dados originais de `gh_pac_info_med`.

São atributos do cadastro atual, inclusive para atendimentos antigos.
Não descrevem necessariamente a situação na data do atendimento. As flags
distinguem Sim, Não e Não Informado; ausência não vira Não e o texto livre
de comorbidades não é convertido em diagnóstico.

O alias segue os schemas/ambientes medallion do dbt. Atualizar a fonte do
dataviz e retirar a consulta programada são etapas de implantação; o modelo
não sobrescreve `dashboard_acolherio.dev_atendimentos`.

## Verificações

Os testes verificam a unicidade dos IDs de atendimento, a cobertura das duas
fontes sem exclusões analíticas e exatamente um primeiro rank para cada grupo
elegível de profissionais, incluindo compartilhados. As categorias da V2 e
o enriquecimento social também são conferidos na validação do BigQuery.

Na validação de 16/10/2026, a mart preservou os 1.001.937 atendimentos das
fontes, todos com ID e chave técnica únicos e preenchidos. Permaneceram 655
registros fora do universo opcional da V2, 633 sinalizados como teste, 41 do
módulo usuário com vínculo encerrado e 7.440 com rank do principal maior que 1.
Esses recortes se sobrepõem e não devem ser somados.

Ao expandir a equipe e aplicar os filtros opcionais de elegibilidade e rank
do integrante, foram obtidas 1.022.187 participações: todos os 1.022.072 grupos
da V2 e 115 adicionais antes descartados por IDs legados nulos ou colidentes.
Não houve divergências de classificação, CBO, documentação, tipo de unidade
ou nome do atendimento nos grupos da V2. Os 12 testes dbt compilados passaram
no BigQuery. Oito asserções com dados sintéticos
também verificaram a preservação de inelegíveis e os ranks independentes dos
compartilhados, inclusive quando o principal é duplicata.

A credencial local não acessa a planilha do Drive. Somente na validação local,
os e-mails por unidade vieram dos valores já materializados no dev. O modelo
e o build de CI continuam lendo a planilha original.
