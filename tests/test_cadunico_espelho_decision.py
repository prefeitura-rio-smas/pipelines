# ruff: noqa: S101
"""Decisão de exportação do espelho cd_cidadao_pac.

Partições são `partition_id` do BigQuery (YYYYMM). Recarga corretiva no mesmo
mês não reexporta (decisão do Leone, 2026-10-21).
"""

from pipelines.cadunico_espelho.decision import should_export


def test_exporta_quando_datalake_e_mais_novo():
    assert (
        should_export(
            "202608",
            "202603",
            export_table_exists=True,
        )
        is True
    )


def test_nao_exporta_quando_particoes_iguais():
    assert (
        should_export(
            "202608",
            "202608",
            export_table_exists=True,
        )
        is False
    )


def test_exporta_quando_tabela_ausente():
    assert (
        should_export(
            "202608",
            None,
            export_table_exists=False,
        )
        is True
    )


def test_exporta_quando_marcador_ausente_com_tabela_existente():
    assert (
        should_export(
            "202608",
            None,
            export_table_exists=True,
        )
        is True
    )


def test_nao_exporta_sem_particao_de_origem():
    assert (
        should_export(
            None,
            "202608",
            export_table_exists=True,
        )
        is False
    )


def test_nao_exporta_quando_origem_e_mais_antiga():
    assert (
        should_export(
            "202603",
            "202608",
            export_table_exists=True,
        )
        is False
    )
