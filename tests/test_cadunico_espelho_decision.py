# ruff: noqa: S101
from datetime import date

from pipelines.cadunico_espelho.decision import should_export


def test_exporta_quando_datalake_e_mais_novo():
    assert (
        should_export(
            date(2026, 8, 7),
            date(2026, 3, 13),
            export_table_exists=True,
        )
        is True
    )


def test_nao_exporta_quando_datas_iguais():
    assert (
        should_export(
            date(2026, 8, 7),
            date(2026, 8, 7),
            export_table_exists=True,
        )
        is False
    )


def test_exporta_quando_tabela_ausente():
    assert (
        should_export(
            date(2026, 8, 7),
            None,
            export_table_exists=False,
        )
        is True
    )
