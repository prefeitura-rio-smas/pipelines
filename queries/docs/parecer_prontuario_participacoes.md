# Parecer de publicação do Prontuário — 23/11/2026

Destino `rj-smas-dev.abordagem.prontuario_eletronico`, região US. A mart mantém uma linha por usuário e unidade. Cada item em `atendimentos.registros` representa uma participação, com a classificação da programada e um único elemento em `profissionais`. O identificador do evento, a data, a unidade e a flag de compartilhamento foram preservados em cada participação. Para métricas de evento, usar a chave documentada em [Prontuário por usuário e unidade](prontuario_usuario_unidade.md).

## Candidata

O snapshot `rj-smas-dev.abordagem.prontuario_eletronico_backup_participacoes_6364` foi criado pelo job `prontuario_participacoes_6364_backup`. O job `prontuario_participacoes_6364_candidata` materializou `rj-smas-dev.abordagem.prontuario_eletronico_candidata_participacoes_6364` a partir do snapshot, aplicando a mesma projeção de participação usada pela mart. A candidata expira após sete dias.

| Verificação | Resultado na candidata | Job BigQuery |
| --- | --- | --- |
| Reconciliação com a programada | 1.132.046 participações, incluindo uma sem ID profissional, em 118 unidades e 46 meses. Zero diferenças de multiplicidade por usuário, evento, módulo e profissional, e zero diferenças nos totais por mês, unidade e classificação. | `pu6364_candidata_reconciliacao_bdd07529-e246-4cde-aec3-ac2909c3b48b` |
| Esquema, chaves e preservação | Esquema idêntico ao snapshot, 692.858 chaves usuário–unidade únicas e 1.100.591 IDs de evento distintos antes e depois. Sem diferenças de dados fora de `atendimentos`, incluindo destinatários. Os contadores, a ordem das participações e a unidade de cada item conferem. | `pu6364_candidata_preservacao_cc9178d0-3ca0-4194-af93-95dfe0cc2349` |
| Compartilhamento com tipos diferentes | 2.582 eventos compartilhados com dois profissionais e duas classificações. A comparação por posição confirmou as duas participações e preservou a flag original. | Mesmo job de preservação |
| Consumo seguro | Casos sintéticos de acesso passaram. Para um cidadão vinculado a unidades com destinatários diferentes, a consulta retornou somente os registros autorizados para a identidade testada. | `3fe6bd08-a8be-466e-85f0-cb8ccb313c44` (caso real) |

## Cartões do CRAS Acari

Foram reproduzidos os jobs do Looker no BigQuery com os filtros originais de unidade, data e acesso, de 01/01 a 22/11/2026. Para consultar a candidata, mudou apenas a referência da tabela nas consultas originais.

| Indicador | Resultado | Job original | Job na candidata | Job no destino |
| --- | ---: | --- | --- | --- |
| Participações | 38.534 | `job_OPBpmU55uAk1V2oaSO4DVpDkojh5` | `pu6364_candidata_total_por_tipo_7eb0ed10-4a7c-4de8-9115-9088b2d02be3` | `pu6364_destino_total_por_tipo_31898601-b72e-44a7-a801-b7eb590eb355` |
| Recepção | 19.134 | `job_X12c-WMAO2K1A-9pSzT18vkIibT0` | `pu6364_candidata_recepcao_b316a0a5-fc26-4a26-86fb-b6553a165045` | `pu6364_destino_recepcao_755aa645-0dc3-4539-9e62-37b182375516` |
| Técnicas | 9.280 | `job_R7JFFn4gADcBssv59jJ_NLEUkpio` | `pu6364_candidata_tecnico_ae6a81ea-665f-4e7e-8d24-84746d108f12` | `pu6364_destino_tecnico_95e5f39f-f916-482f-bb9a-1504f9e9ba6b` |
| Outros | 10.120 | `job_WjVg1LtzPnH738wGps0zcWmmy3QT` | `pu6364_candidata_outros_ea08e289-a94f-433e-b776-547c6763d0e6` | `pu6364_destino_outros_691b7591-0988-45cf-b49e-f259d8a2f6da` |
| Profissionais distintos | 22 | `job_802AQvpNqz7fDlso4ZD6-j0b7gHx` | `pu6364_candidata_distinto_3_94260aca-181e-47c0-9153-3f18813c8189` | `pu6364_destino_distinto_3_1c793ac5-18b2-4806-921c-704a90768505` |

## Publicação

O job `prontuario_participacoes_6364_publicacao` verificou o destino contra o snapshot no início da transação e executou `TRUNCATE TABLE` e `INSERT` na mesma transação. O esquema e as políticas de acesso da tabela e do dataset conferem com os metadados anteriores. A listagem de políticas de acesso por linha estava vazia antes e depois da troca.

A reconciliação no destino repetiu 1.132.046 participações e zero divergências no job `pu6364_destino_reconciliacao_b4266532-9acc-4c71-a852-0b14f6c0c54c`. O job `pu6364_destino_preservacao_b545302c-b488-4c51-b925-778b0ef74dc2` repetiu a verificação das chaves, dos eventos distintos, da ordem, dos contadores, dos dados de cadastro e dos destinatários.

Os testes sintéticos de acesso passaram novamente. O teste com um cidadão vinculado a múltiplas unidades e destinatários distintos passou no job `d66a89c9-1681-43df-86c7-8c25b02350eb`. O snapshot foi mantido para restauração.

## Validação do dbt

O dbt compilou a mart e os testes. SQLFluff passou nos arquivos SQL alterados. As dependências padrão de desenvolvimento apresentaram um dataset ausente e uma falha de credenciais do Drive. Os testes foram executados em uma sessão isolada, usando a programada, as relações de CI disponíveis e uma cópia temporária da fonte de permissões.

| Teste na candidata | Divergências | Job BigQuery |
| --- | ---: | --- |
| Isolamento por unidade e contadores | 0 | `pu6364_dbt_unidades_35628f59-2712-4c38-b51c-57b88e84fb55` |
| Identidade e multiplicidade de participações e evoluções | 0 | `pu6364_dbt_preservacao_eventos_d976c70b-a7c5-4e88-8f9f-007674b9cf8d` |
| População | 0 | `pu6364_dbt_populacao_196e1229-2292-42e6-a352-90f23ebf9156` |
| Perfis por unidade | 0 | `pu6364_dbt_perfis_unidade_99b66abe-1c66-4992-92e2-90024059e1e0` |

A mart compilada foi materializada em tabela temporária no job `pu6364_dbt_modelo_66ed5c20-5666-41fb-a5c1-389aa2e0c163`. A reconstrução obteve 692.858 linhas e 1.132.046 participações. O job `pu6364_dbt_compara_modelo_ebdb48b0-73b6-4121-92b5-78891bd19c2c` comparou os registros completos de atendimento por chave usuário–unidade, incluindo multiplicidade, e encontrou zero divergências com a candidata.
