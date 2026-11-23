# Prontuário eletrônico por usuário e unidade

O modelo `prontuario_eletronico` publica uma linha por `(id_usuario, id_unidade)`. O ID no topo também está disponível em `usuario.id_usuario`.
`id_unidade` é STRING; vazio representa registros sem unidade identificada ou um usuário sem vínculos.
A unidade de referência do cadastro não cria um vínculo de atendimento.

Dois atendimentos no Centro POP A e um no B geram duas linhas para o usuário. Com um profissional por atendimento, A tem quantidade 2 e B tem quantidade 1.
Unidades homônimas permanecem separadas por ID. Um usuário com eventos identificados e outros sem unidade também terá uma linha com ID vazio.

Cadastro, documentos e famílias se repetem nas unidades. Projetos, atendimentos, evoluções, perfis e históricos pertencem à unidade da linha.
Os últimos formulários, participações, encaminhamentos e ciclos de acolhimento são selecionados dentro dessa unidade.
`centro_pop.data_ultimo_atend_tecnico_informada` também passa a representar usuário/unidade, com a mesma extração de `Registro:` e alternativa na data da evolução.

## Consumo

- Número de linhas: vínculos usuário/unidade, incluindo a categoria sem unidade.
- Pessoas: `COUNT(DISTINCT usuario.id_usuario)`.
- Participações profissionais: somar `atendimentos.quantidade` ou contar itens de `atendimentos.registros`. Cada item tem uma classificação e um único elemento em `profissionais`, inclusive quando o ID profissional está vazio. Os contadores por serviço também contam participações.
- Eventos por usuário e unidade: abrir `atendimentos.registros`, filtrar IDs de evento preenchidos e deduplicar por `(id_usuario, id_unidade, id_atendimento_modulo)`. Para eventos únicos no conjunto, usar `COUNT(DISTINCT NULLIF(id_atendimento_modulo, ''))`. O ID inclui o módulo e pode se repetir entre participações e entre usuários de um atendimento familiar.
- Indicador de atendimentos no Looker: os cartões que abrem `atendimentos.registros` contam participações com a consulta atual. Usar `classificacao_atendimento` do registro para os tipos e contar IDs profissionais distintos e preenchidos para o indicador de profissionais.
- Nos cartões, manter a dimensão de data do indicador, `data_atendimento_informada` para ocorrência ou `data_atendimento_sistema` para cadastro.
- Atendimentos compartilhados: filtrar por `atendimento_compartilhado = 'Sim'` e deduplicar pela chave correspondente ao escopo da métrica de eventos. A flag exige pelo menos dois profissionais distintos com identificador preenchido. Um evento com três participações gera três registros com a mesma flag.
- Quantidades de pessoas de unidades distintas não são aditivas: a mesma pessoa pode estar em mais de uma unidade.
- Usar `id_unidade` para chaves, filtros e junções; `nome_unidade` contém um único nome e não é uma chave.
- As primeiras colunas são `id_usuario_unidade`, `id_usuario`, `id_unidade`, `nome_unidade`, `email_filtro` e `usuario`. A chave `id_usuario_unidade` é uma STRING no formato `123:45` ou `123:` para linhas sem unidade. Os destinatários da unidade ficam em `email_filtro` na raiz. O Looker usa a consulta autorizada para filtrar a linha e os itens da unidade.
- Consumidores que tratavam `usuario.id_usuario` como único precisam usar a chave composta. Joins apenas por usuário podem multiplicar linhas.

## Validação e publicação

O contrato mantém os campos e tipos publicados. O grão de `atendimentos.registros` e dos seus contadores é a participação profissional.
Os testes dbt verificam unicidade composta, cobertura dos vínculos e usuários, isolamento de listas, contadores, perfis locais e a preservação da identidade e multiplicidade de participações e evoluções.

Validar primeiro em uma candidata, comparar os tipos e os cadastros com a tabela publicada e criar backup antes de substituir o destino.
Quando a fonte externa de permissões estiver indisponível, só utilizar uma cópia nativa de destinatários com cobertura de todas as unidades e equivalência comprovada com os destinatários publicados. A execução mantém a data desse mapa; não pressupõe atualização da planilha externa.
