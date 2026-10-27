# Correlação da planilha unificada com dev_atendimentos para migração do BI

Comparação em 27/10/2026 entre `rj-smas-dev.dashboard_acolherio.dev_atendimentos`
e `rj-smas-dev.relatorio.planilha_unificada`, usando os schemas reais, as regras
da consulta programada e consultas agregadas de conferência. A programada está
pausada. Os metadados das duas fontes permaneceram iguais durante a validação.
As métricas publicadas abaixo são do lote comparado; não certificam alterações
futuras nem os campos calculados/configurações internos de um relatório de BI.

> Atualização v7: os 13 complementos foram incorporados nos blocos da supertabela.
> Consulte [o contrato dos complementos](complementos_bi_planilha_unificada.md).

A substituição exige uma camada de consumo plana por atendimento/profissional.
Dos **39 campos antigos**, **23 são diretos**, **13 são derivados**, **2 são técnicos
já filtrados para 1** e **1 é parcialmente equivalente**. Os 13 campos anteriormente
marcados como complemento agora têm caminhos públicos na supertabela. Os campos
recuperáveis são comparados considerando as conversões de tipos e de ausências.

- [Matriz completa em CSV para planilha](correlacao_campos_dev_atendimentos.csv), UTF-8, separador ponto e vírgula.
- [Consulta de apoio para o BI](../analyses/bi/planilha_unificada_atendimentos_bi.sql), somente SELECT, com 38 campos antigos recuperáveis e duas colunas de apoio.
- [Evidência agregada e schemas](validacao_bi_dev_atendimentos_20261027.json), sem exportação de dados pessoais.

## Grão e indicadores

| Medida | dev_atendimentos | Nossa planilha |
|---|---:|---:|
| Linhas na tabela principal | 1.050.540 | 586.840 usuários |
| Atendimentos distintos por sequência + módulo | 1.021.410 | 1.021.410 |
| Usuários atendidos | 316.708 | 316.708 |
| Usuários sem atendimento | Não entram na tabela plana | 270.132 |
| Linhas após expandir atendimentos e profissionais | 1.050.540 | 1.050.539 |

As chaves de eventos e os IDs de usuários atendidos são idênticos nos dois sentidos,
sem eventos duplicados na saída nova. Há 23.826 eventos com mais de um profissional,
chegando a oito profissionais por evento. As listas de CBO, famílias, projetos,
evoluções e formulários devem ser agregadas ou consultadas separadamente para
manter o grão da medida.

A diferença de uma linha vem do registro antigo sem `PROFISSIONAL_ID`: o
intermediário de referência inclui na lista apenas profissionais com ID. O mesmo
atendimento está preservado e possui outro profissional na lista. A diferença
precisa ser considerada em indicadores baseados em `COUNT(*)`; nenhum evento
foi perdido. Os ranks antigos são ambos 1 em todas as 1.050.540 linhas, pois a
programada já aplica os dois filtros na publicação.

| Indicador desejado | Regra na nova fonte |
|---|---|
| Atendimentos únicos | `COUNT(DISTINCT a.id_atendimento_modulo)` na camada expandida; ou `SUM(atendimentos.quantidade)` diretamente na base por usuário |
| Pessoas atendidas | `COUNT(DISTINCT usuario.id_usuario)` após `CROSS JOIN UNNEST(atendimentos.registros)` |
| Pessoas cadastradas | `COUNT(DISTINCT usuario.id_usuario)` na raiz: inclui 270.132 sem atendimento |
| Linhas de atendimento/profissional | Expandir eventos e fazer `LEFT JOIN UNNEST(a.profissionais) ... ON TRUE`; ensaio retorna 1.050.539 |
| Pessoas/atendimentos por mês ou unidade | Usar data e unidade do próprio evento e contar IDs distintos no recorte |
| Famílias atendidas | Exige vínculo da família com a linha/evento; `familias[]` do usuário não resolve a mesma métrica |

`SUM(atendimentos.quantidade)` pertence ao grão por usuário; somá-lo depois de
expandir profissionais repete o total do usuário. `SEQ_ATENDIMENTO` sem módulo
não é a chave canônica do evento. `ID_ATENDIMENTO` antigo combina sequência e
profissional, enquanto `ID_ATENDIMENTO_MODULO` novo representa o atendimento.

## Matriz dos 39 campos

Notação: `p` é uma linha da supertabela; `a` vem de
`UNNEST(p.atendimentos.registros)`; `prof` vem de `UNNEST(a.profissionais)`.
Direto admite conversão simples de STRING para DATE/INT64 e retorno de ausência
vazia para NULL. Derivado exige regra explícita. Complemento indica dado ausente
na saída atual, com fonte disponível ou regra a recuperar na camada de consumo.

| Campo em dev_atendimentos | Equivalência | Campo/regra na nova fonte | Observação |
|---|---|---|---|
| modulo | Derivado | a.origem_modulo | usuario → u; familia → f. Não confundir com id_atendimento_modulo. |
| CAS | Direto | a.cas | Foto congelada do BI. Para endereços, usuários fora da foto usam o cadastro congelado, sem preencher lacunas da foto; converter ausência vazia para NULL quando necessário. |
| TIPO_UNIDADE | Derivado | CASE sobre a.nome_unidade | Reproduzir os prefixos da consulta antiga. a.tipo_servico agrupa cras/creas/centro_pop/outros e não cobre a taxonomia antiga. |
| UNIDADE_ATENDIMENTO | Direto | a.nome_unidade | Unidade do evento; usuario.nome_unidade_referencia é outro conceito. |
| EMAIL_UNIDADE | Direto | a.email_unidade | Foto congelada do BI. Para endereços, usuários fora da foto usam o cadastro congelado, sem preencher lacunas da foto; converter ausência vazia para NULL quando necessário. |
| EMAIL_CAS | Direto | a.email_cas | Foto congelada do BI. Para endereços, usuários fora da foto usam o cadastro congelado, sem preencher lacunas da foto; converter ausência vazia para NULL quando necessário. |
| SEQ_ATENDIMENTO | Derivado | SAFE_CAST(REGEXP_EXTRACT(a.id_atendimento_modulo, r'^(\d+)[uf]$') AS INT64) | Sequência isolada repete entre módulos; a chave do evento continua sendo id_atendimento_modulo. |
| DATA_DE_ATENDIMENTO | Direto | SAFE_CAST(NULLIF(a.data_atendimento, '') AS DATE) | Restaurar tipo DATE para os controles temporais do BI. |
| HORA_DE_ATENDIMENTO_ORIGINAL | Derivado | SAFE_CAST(REPLACE(NULLIF(a.hora_atendimento, ''), ':', '') AS INT64) | Recompor HHMM. A origem bruta pode conter horas inválidas que a normalização descarta; zero diferenças no lote comparado. |
| HORA_DE_ATENDIMENTO | Direto | NULLIF(a.hora_atendimento, '') | Hora HH:MM; converter ausência pública vazia em NULL para reproduzir a tabela plana. |
| ID_USUARIO | Direto | SAFE_CAST(p.usuario.id_usuario AS INT64) | Contar usuários atendidos depois de expandir atendimentos; a raiz também inclui usuários sem atendimento. |
| ID_FAMILIA | Direto | prof.id_familia_referencia | Família da foto do BI preservada na linha profissional; pode variar entre profissionais do mesmo evento. Converter para INT64 no BI. |
| NOME_USUARIO | Direto | NULLIF(p.usuario.nome, '') | Cadastro do usuário do lote; conferido contra a publicação antiga. |
| DOCUMENTACAO | Derivado | CASE sobre p.usuario.cpf | CPF vazio → CPF não informado; preenchido → CPF informado. Não equivale às flags de documentos do CadÚnico. |
| NUMERO_DOCUMENTO | Derivado | Máscara de p.usuario.cpf | Reproduzir LPAD de 11 posições e máscara 000.000.000-00; não usar NIS/RG como substituto. |
| ENDERECO | Direto | p.usuario.logradouro | Foto congelada do BI. Para endereços, usuários fora da foto usam o cadastro congelado, sem preencher lacunas da foto; converter ausência vazia para NULL quando necessário. |
| ENDERECO_NUMERO | Direto | p.usuario.numero | Foto congelada do BI. Para endereços, usuários fora da foto usam o cadastro congelado, sem preencher lacunas da foto; converter ausência vazia para NULL quando necessário. |
| ENDERECO_COMPLEMENTO | Direto | p.usuario.complemento | Foto congelada do BI. Para endereços, usuários fora da foto usam o cadastro congelado, sem preencher lacunas da foto; converter ausência vazia para NULL quando necessário. |
| BAIRRO | Derivado | p.usuario.bairro | Vazio → NAO INFORMADO, conforme regra antiga. A apresentação da supertabela unifica ausências como texto vazio. |
| REFERENCIA_OU_COMUNIDADE | Direto | p.usuario.referencia_ou_comunidade | Foto congelada do BI. Para endereços, usuários fora da foto usam o cadastro congelado, sem preencher lacunas da foto; converter ausência vazia para NULL quando necessário. |
| NOME_ATENDIMENTO_ORIGINAL | Direto | a.tipo_atendimento_descricao | Descrição original do tipo do evento. |
| DATA_NASCIMENTO | Direto | SAFE_CAST(NULLIF(p.usuario.data_nascimento, '') AS DATE) | Restaurar DATE; não substituir por data do atendimento. |
| IDADE | Derivado | Cálculo sobre p.usuario.data_nascimento | A antiga calcula idade na data de execução da carga, e não na data do atendimento. A conferência usa 26/10/2026; a consulta de apoio usa CURRENT_DATE(). |
| RACA_COR | Derivado | p.usuario.raca_cor + normalização de rótulos | Amarelo → Amarela; Indígena → Indigena; vazio → Nao informado, para preservar categorias do BI. |
| SEXO | Direto | NULLIF(p.usuario.sexo, '') | Mesmo código bruto do cadastro; usuario.genero é outro atributo. |
| PROFISSIONAL_ID | Direto | prof.id_profissional | ID apresentado na referência, preservado por participante do evento. Converter para INT64 no BI; homônimos mantêm IDs distintos. |
| PROFISSIONAL | Direto | NULLIF(prof.nome_profissional, '') | Expandir a.profissionais. O nome já preserva o profissional apresentado pela referência antiga. |
| PROFISSIONAL_CBO_ORIGINAL | Direto | NULLIF(prof.cbo_principal_descricao, '') | Descrição escolhida na referência antiga. Expandir descricoes_cbo[] produz outro grão. |
| PROFISSIONAL_CBO | Derivado | Normalização de prof.cbo_principal_descricao | Aplicar os prefixos de Articulador, Assistente social/administrativo, Educador, Entrevistador, Pedagogo, Psicólogo e Recepcionista da programada. |
| CADASTRANTE | Direto | a.cadastrante | Pessoa que cadastrou o atendimento, copiada da referência congelada; não equivale ao executor. |
| DATA_CADASTRO_ATENDIMENTO | Direto | a.data_cadastro_atendimento | Data da referência como STRING na unificada; converter para DATE no BI, sem fallback pela data de atendimento. |
| TIPO_ATENDIMENTO | Derivado | CASE por nome original e CBO do profissional | Preservar a prioridade Recepção → regra CadÚnico → Técnico → Outros da programada, por linha profissional. classificacao_atendimento é específica do Centro POP e usa outra regra. |
| NOME_ATENDIMENTO | Derivado | Regra CadÚnico por TIPO_UNIDADE e PROFISSIONAL_CBO | Reproduzir CRAS/CREAS - Recepção - Ação CadÚnico quando aplicável; demais usam o nome original. |
| IDADE_FAIXA | Derivado | CASE sobre IDADE | Reproduzir limites e rótulos antigos, inclusive Mais de 75 anos para idade >= 75. Usa a mesma data de referência da idade. |
| ID_ATENDIMENTO | Direto | prof.id_atendimento_legado | Chave legada atendimento/profissional, preservada da referência. A chave canônica do evento continua sendo ID_ATENDIMENTO_MODULO. |
| EMAIL | Direto | a.email | Composição original da referência congelada, sem recalcular por email_filtro. |
| cbo_unico_rank | Técnico | Constante 1 na camada de compatibilidade | Todos os registros publicados na origem já passaram por QUALIFY cbo_unico_rank = 1. Remover o filtro no BI ou manter 1. |
| atendimento_unico_rank | Técnico | Constante 1 na camada de compatibilidade | Todos os registros publicados na origem já passaram por QUALIFY atendimento_unico_rank = 1. A fonte de eventos já preserva as chaves publicadas. |
| PROJETOS_SOCIAIS | Parcial | p.projetos_sociais[]; agregação por usuário | Antiga: projetos ativos e sem cancelamento da família da linha. Nova: projetos dos membros de famílias ativas, último cadastro por usuário/projeto. Filtrar S e sem cancelamento aproxima, mas não reconstrói a origem por família. |

## Diferenças que precisam ser tratadas antes da troca

**Classificação de atendimento.** A antiga classifica cada linha profissional,
com prioridade para recepção, uma regra específica para CadÚnico e depois uma
lista de ocupações técnicas. `a.classificacao_atendimento` usa a regra do Centro
POP, prioriza CBO do grupo 2 e fica vazia em outros serviços. Substituir
`TIPO_ATENDIMENTO` por esse campo produziria 1.047.411 divergências, sendo 373
nas 3.502 linhas Centro POP. A consulta de apoio reproduz a regra antiga por
profissional e obteve zero divergências. `NOME_ATENDIMENTO` também precisa da
regra de renomeação de recepção CadÚnico em CRAS/CREAS.

**Projetos sociais.** Na antiga o texto vem da família selecionada na linha,
considerando `indativo = 'S'` e `datcancel IS NULL`. Na nova a lista é consolidada
por usuário a partir de membros familiares ativos e do último registro por
projeto. Agregar somente projetos S e sem cancelamento gera 154 linhas diferentes
entre as pareadas, afetando 32 usuários, com 11.960 valores preenchidos na antiga e 11.930 na nova.
A consulta chama essa coluna `PROJETOS_SOCIAIS_USUARIO` para explicitar o escopo.
Equivalência por família requer conservar essa associação e a mesma regra de
seleção da família da consulta antiga.

**Idade e faixa.** A materialização antiga é de 26/10/2026 e calcula idade na
data de execução da carga. Com essa mesma data, idade e faixa têm zero diferenças.
Recalcular a idade em 27/10/2026 altera 2.467 linhas do conjunto pareado. O BI
precisa usar a data de referência desejada para os dois campos; idade na data do
atendimento representa outra regra. A consulta de apoio mantém o cálculo original
com `CURRENT_DATE()`, incluindo o uso de dia do ano para o aniversário.

**Raça/cor e ausências.** A nova fonte usa Amarelo/Indígena/vazio, enquanto a
antiga usa Amarela/Indigena/Nao informado. A consulta faz essa tradução e restaura
a máscara de CPF, o rótulo de bairro não informado, tipos DATE/INT64 e NULLs
necessários. Zero diferenças foi observado no lote; futuras cargas precisam
continuar sendo comparadas, pois o cadastro representa o estado do lote.

**IDs e complementos adicionados.** Os 13 dados agora estão nos caminhos da
matriz. Endereços ficam diretamente em usuario; CAS, e-mails, cadastrante e data de
cadastro ficam no evento; ID profissional, família da referência e chave legada
ficam na linha profissional. Há três grupos de profissionais homônimos no mesmo
evento; os IDs preservam a associação. O vínculo familiar não é inferido da lista
de famílias do cadastro do usuário.

**E-mails de filtro.** A supertabela conserva em `a.email` a composição original
da foto do BI. A consulta de apoio usa esse campo, em vez do email_filtro da
dimensão de unidades. A normalização de ausência pública como texto vazio deve
ser considerada nos controles e filtros do relatório.

## Aplicação no BI

1. Usar uma camada por atendimento/profissional para comparar os indicadores atuais e uma camada por usuário para cadastro/perfil.
2. Aplicar os 38 campos reproduzidos pela consulta de apoio e manter tipos e regras de categorias conferidos acima.
3. Usar os caminhos da matriz para os 13 complementos incorporados; validar filtros, relacionamentos e gráficos do relatório.
4. Definir se o indicador de projetos deve continuar por família ou passar a ser por usuário; validar a data de referência da idade.
5. Conferir os totais mensais, por unidade, por profissional e por categoria do relatório antes de alterar a fonte do BI, incluindo a linha antiga sem ID profissional.

A consulta fornecida tem 40 colunas: 38 campos recuperáveis da fonte anterior,
`ID_ATENDIMENTO_MODULO` e `PROJETOS_SOCIAIS_USUARIO`. A diferença de regra de projetos
por família e a linha antiga sem ID profissional permanecem explícitas. Fórmulas,
filtros e relacionamentos internos do BI precisam ser verificados no relatório.

A evidência anterior ao v7 permanece em `validacao_bi_dev_atendimentos_20261027.json`;
a validação da ampliação está em [validacao_complementos_bi_20261027.json](validacao_complementos_bi_20261027.json). A consulta de apoio v7 reproduziu os 38 campos recuperáveis sem diferenças em 1.050.539 linhas pareadas, após conversões de tipos, ausências e referência da idade.


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
