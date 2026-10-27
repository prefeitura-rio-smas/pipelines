# Complementos do BI na planilha unificada

Os complementos foram introduzidos por `por_assunto_v7_complementos_bi` e permanecem no contrato atual `por_assunto_v9_campos_diretos_formularios_texto`. Acrescenta
13 campos à supertabela por usuário, mantendo os seis blocos, suas populações,
contagens e todos os valores anteriores. Escalares públicos são STRING com ausência
`''`, listas ausentes são `[]` e as quantidades existentes continuam INT64.

## Campos e origem

| Campo em dev_atendimentos | Caminho na supertabela |
|---|---|
| ENDERECO | usuario.logradouro |
| ENDERECO_NUMERO | usuario.numero |
| ENDERECO_COMPLEMENTO | usuario.complemento |
| REFERENCIA_OU_COMUNIDADE | usuario.referencia_ou_comunidade |
| CAS | atendimentos.registros[].cas |
| EMAIL_UNIDADE | atendimentos.registros[].email_unidade |
| EMAIL_CAS | atendimentos.registros[].email_cas |
| EMAIL | atendimentos.registros[].email |
| CADASTRANTE | atendimentos.registros[].cadastrante |
| DATA_CADASTRO_ATENDIMENTO | atendimentos.registros[].data_cadastro_atendimento |
| PROFISSIONAL_ID | atendimentos.registros[].profissionais[].id_profissional |
| ID_FAMILIA | atendimentos.registros[].profissionais[].id_familia_referencia |
| ID_ATENDIMENTO | atendimentos.registros[].profissionais[].id_atendimento_legado |

Endereços usam a foto congelada de `dev_atendimentos` para qualquer usuário
presente nessa referência. A presença seleciona o endereço inteiro: lacunas da
foto permanecem vazias, sem completar componentes com outro cadastro. Usuários
fora da foto usam o snapshot adicional de `gh_cidadao_pac`, com a mesma limpeza
de vírgulas, espaços e ponto e vírgula da consulta antiga. `usuario.bairro`
permanece no local anterior. Os endereços retratam os snapshots, não a residência
na data histórica de cada atendimento.

Os outros nove campos vêm da própria referência congelada. CAS, e-mails,
cadastrante e data de cadastro são únicos por evento; conflitos impedem aprovação.
`EMAIL` conserva a composição original, sem substituição por `email_filtro` da
dimensão de unidades. Cadastrante é quem registrou o atendimento; data de cadastro
é distinta da data de realização, sem fallback entre essas datas.

A família da referência é mantida por profissional: o lote contém um evento com
famílias diferentes entre profissionais válidos. `id_atendimento_legado` conserva
o valor da chave antiga, que combina sequência e profissional sem módulo. A
identidade canônica do evento permanece `id_atendimento_modulo`; não há garantia
de unicidade global para a chave legada em futuras origens.

## Construção congelada

Os intermediários `int_pu_enderecos_bi` e `int_pu_complementos_bi` preparam os dados
com unicidade por usuário e por evento, respectivamente. O contexto profissional
é único por evento/profissional. A agregação precede os cruzamentos da saída e
preserva a ordem dos itens existentes. Profissionais homônimos são associados
pelo ID interno antes da projeção pública.

```bash
.venv/bin/python queries/operations/planilha_unificada.py preparar --lote lote_bi --derivar-de lote_publicado
.venv/bin/python queries/operations/planilha_unificada.py dimensao --lote lote_bi --execucao execucao_bi --servico centro_pop
.venv/bin/python queries/operations/planilha_unificada.py consolidar --lote lote_bi --execucao execucao_bi --servicos centro_pop
.venv/bin/python queries/operations/planilha_unificada.py publicar --execucao execucao_bi
```

O lote derivado copia os insumos do lote pai, conserva a referência e o instante
de captura anteriores e acrescenta somente o snapshot cadastral dos endereços.
O manifesto registra lote pai, fingerprint de seu manifesto, fingerprints das
fontes herdadas e a origem/instante próprios do complemento cadastral. Se o lote pai já contém esse snapshot, ele é herdado sem nova captura. O lote pai
é imutável. A limpeza protege pais de lotes derivados referenciados.

O perfil Centro POP é construído/testado privadamente na mesma execução para
verificar a reprodução do escopo; seu contrato continua `centro_pop_usuario_v3`.
A publicação desta migração substitui somente a supertabela e atualiza somente
a linha `supertabela` do controle. Dimensões, histórico materializado e controle
específico de Centro POP permanecem intactos. Há backup da supertabela e do
controle antes de qualquer substituição, verificação de geração e restauração
compensatória em caso de falha; uma nova tentativa exige reconsolidação. Migrações pendentes protegem seus backups da limpeza.

A consulta programada permanece pausada e sem execuções pendentes, e o ciclo
automático continua desabilitado. A operação não troca a fonte do relatório de BI.

## Validação e compatibilidade

- Schema aditivo: apenas os 13 campos aprovados, mantendo nomes, tipos e ordem dos campos anteriores.
- Comparação integral por usuário projetando o candidato no schema anterior, inclusive arrays ordenados, STRUCTs nulos e multiplicidade dos itens.
- Igualdade de chaves, usuários, atendimentos, evoluções e todos os blocos Centro POP anteriores.
- Comparação dos nove campos de contexto contra a referência congelada por evento/profissional e dos quatro campos de endereço contra sua origem selecionada.
- Unicidade, homônimos, famílias distintas por profissional, endereços parcialmente vazios, zero/zeros à esquerda, módulos u/f, ausência de fonte e contexto conflitante.
- Proteção de geração, lote pai, fontes herdadas e programada pausada; restauração real de schema/dados após falha simulada em tabelas isoladas.

A consulta em [planilha_unificada_atendimentos_bi.sql](../analyses/bi/planilha_unificada_atendimentos_bi.sql)
passa a ler os complementos da supertabela. Ela restaura tipos e aliases antigos
na apresentação para o BI, convertendo texto vazio em NULL quando necessário.
A comparação de ausências considera NULL e texto vazio equivalentes para textos.

Projetos sociais continuam por usuário e a classificação Centro POP continua com
sua regra atual. A consulta de apoio mantém a derivação da classificação antiga
por linha profissional e chama os projetos de `PROJETOS_SOCIAIS_USUARIO`, sem
prometer equivalência com projetos por família. A linha antiga sem ID profissional
continua fora da lista de profissionais, preservando a cardinalidade anterior da
supertabela: nenhum item vazio é criado para compensar essa diferença.

## Publicação verificada em 27/10/2026

Publicada em `rj-smas-dev.relatorio.planilha_unificada`, contrato
`por_assunto_v7_complementos_bi`, execução `e20261027_complementos_bi` e lote
`l20261027_complementos_bi`, derivado de `l20261027_cpop_respostas_qa`.

| Medida | Resultado preservado |
|---|---:|
| Usuários | 586.840 |
| Atendimentos | 1.021.410 |
| Evoluções | 654.748 |
| Linhas profissionais expandidas | 1.050.539 |
| Registros históricos Centro POP | 1.128 |
| Participações Centro POP | 0 |

Dos 270.132 usuários sem atendimento, 254.736 receberam pelo cadastro congelado
algum dos quatro componentes do endereço. Isso não significa endereço completo.
Os endereços dos usuários presentes na referência permaneceram inteiros conforme
a foto do BI, inclusive lacunas. A comparação integral sobre o schema anterior
confirmou igualdade de todos os campos, ordem e multiplicidade de listas antigas.

Passaram 34 testes dbt (8 do perfil/histórico em área privada e 26 da ampliação),
31 testes locais, as 22 regressões SQL de modelos e cinco testes sintéticos dos
complementos. Os testes BigQuery de publicação, restauração real e projeção
aditiva também passaram, incluindo rejeição de alterações em valores antigos,
ordem de listas e diferença entre STRUCT nulo e STRUCT com campos nulos. Ruff,
SQLFluff e a verificação de espaços do Git passaram.

A consulta de apoio foi validada com 40 colunas: nomes/tipos dos 38 campos antigos
recuperáveis, a chave canônica e projetos por usuário. Os 38 campos tiveram zero
diferenças em 1.050.539 linhas pareadas, normalizando ausências textuais e fixando
a idade em 26/10/2026, data da publicação antiga. Projetos por família e a linha
antiga sem ID profissional permanecem fora do escopo desta ampliação.

Fingerprints confirmaram as dimensões Centro POP, CRAS, CREAS e outros, o
intermediário histórico e a mart Centro POP intactos. As linhas de controle dos
demais serviços foram comparadas integralmente com o backup e permaneceram iguais.
A consulta programada foi confirmada pausada ao final.

O manifesto `rj-smas-dev.pu_cand_e20261027_complementos_bi._pu_migracao` registra
os dois backups, estado `publicado`, com sufixo `20261027_180516_926188`.
A [evidência agregada da publicação](validacao_complementos_bi_20261027.json)
registra fontes, instantes de captura, métricas, validações e backups sem exportar
valores pessoais.


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
