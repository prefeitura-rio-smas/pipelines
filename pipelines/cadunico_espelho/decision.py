# -*- coding: utf-8 -*-
"""Decisão de exportação do espelho cd_cidadao_pac.

O gatilho é a PARTIÇÃO (mês, `partition_id` YYYYMM) mais recente da tabela
`identificacao_primeira_pessoa`. Recarga corretiva no mesmo mês NÃO dispara
nova exportação (decisão do Leone, 2026-10-21) — o pior caso é uma
sobrescrita manual.
"""


def should_export(
    source_partition: str | None,
    exported_partition: str | None,
    *,
    export_table_exists: bool,
) -> bool:
    """Decide se a exportação deve rodar.

    Args:
        source_partition: partição (YYYYMM) mais recente da tabela-gatilho.
        exported_partition: partição registrada no marcador GCS da última
            exportação bem-sucedida.
        export_table_exists: se a tabela materializada de destino existe.

    Returns:
        True quando há partição de origem e a tabela de destino não existe,
        o marcador está ausente/ilegível ou a partição de origem é mais nova
        que a exportada.
    """
    if source_partition is None:
        return False
    if not export_table_exists or exported_partition is None:
        return True
    return source_partition > exported_partition
