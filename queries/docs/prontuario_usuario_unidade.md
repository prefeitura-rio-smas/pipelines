# Prontuário eletrônico por usuário e unidade

Cada linha representa um par `(id_usuario, id_unidade)`. A chave
`id_usuario_unidade` tem o formato `123:45` ou `123:` quando a unidade é
desconhecida ou o usuário está sem vínculo. Use os IDs para junções e filtros.
No Looker, use a consulta que aplica `prontuario_consumo_seguro` com
`DS_USER_EMAIL` para restringir os registros à unidade autorizada.

- **Pessoas** — Use `COUNT(DISTINCT id_usuario)` no recorte de interesse. Uma
  pessoa pode ter linhas em várias unidades.
- **Participações profissionais** — Some `atendimentos.quantidade` ou conte
  os itens de `atendimentos.registros`. Cada item contém uma classificação e
  um elemento em `profissionais`, inclusive quando o ID profissional está
  vazio. Para contar profissionais, conte IDs distintos e preenchidos.
- **Eventos** — Conte chaves distintas `(id_usuario, id_unidade, id_atendimento_modulo)`
  para eventos por usuário e unidade. Para eventos únicos em todo o recorte,
  use `COUNT(DISTINCT NULLIF(id_atendimento_modulo, ''))`. O ID pode se repetir
  entre participações e entre membros da família. Para contar eventos
  compartilhados, aplique `atendimento_compartilhado = 'Sim'` antes de deduplicar.

Para recortes temporais, use `data_atendimento_informada` para a ocorrência
e `data_atendimento_sistema` para o registro no sistema.

[Rotina de atualização e validação em staging](../validation/publicar_prontuario_dev.md)
