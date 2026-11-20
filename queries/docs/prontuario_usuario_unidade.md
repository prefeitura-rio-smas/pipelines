# Prontuário eletrônico por usuário e unidade

O modelo `prontuario_eletronico` publica uma linha por `(usuario.id_usuario, id_unidade)`.
`id_unidade` é STRING; vazio representa registros sem unidade identificada ou um usuário sem vínculos.
A unidade de referência do cadastro não cria um vínculo de atendimento.

Dois atendimentos no Centro POP A e um no B geram duas linhas para o usuário: A com quantidade 2 e B com quantidade 1.
Unidades homônimas permanecem separadas por ID. Um usuário com eventos identificados e outros sem unidade também terá uma linha com ID vazio.

Cadastro, documentos e famílias se repetem nas unidades. Projetos, atendimentos, evoluções, perfis e históricos pertencem à unidade da linha.
Os últimos formulários, participações, encaminhamentos e ciclos de acolhimento são selecionados dentro dessa unidade.
`centro_pop.data_ultimo_atend_tecnico_informada` também passa a representar usuário/unidade, com a mesma extração de `Registro:` e alternativa na data da evolução.

## Consumo

- Número de linhas: vínculos usuário/unidade, incluindo a categoria sem unidade.
- Pessoas: `COUNT(DISTINCT usuario.id_usuario)`.
- Atendimentos: somar `atendimentos.quantidade` nas linhas desejadas, ou abrir `atendimentos.registros`.
- Quantidades de pessoas de unidades distintas não são aditivas: a mesma pessoa pode estar em mais de uma unidade.
- Usar `id_unidade` para chaves, filtros e junções; `nome_unidade` contém um único nome e não é uma chave.
- `email_filtro` e os destinatários dos eventos ficam restritos à unidade da linha. A consulta autorizada continua necessária.
- Consumidores que tratavam `usuario.id_usuario` como único precisam usar a chave composta. Joins apenas por usuário podem multiplicar linhas.

## Validação e publicação

O contrato v17 preserva os nomes e tipos existentes e acrescenta `id_unidade`.
Os testes dbt verificam unicidade composta, cobertura dos vínculos e usuários, isolamento de listas, contadores, perfis locais e identidade/multiplicidade dos eventos.

Validar primeiro em uma candidata, comparar os tipos e os cadastros com a tabela publicada e criar backup antes de substituir o destino.
Quando a fonte externa de permissões estiver indisponível, só utilizar uma cópia nativa de destinatários com cobertura de todas as unidades e equivalência comprovada com os destinatários publicados. A execução mantém a data desse mapa; não pressupõe atualização da planilha externa.
