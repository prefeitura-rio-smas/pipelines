from datetime import date


def should_export(
    datalake_date: date | None,
    exported_date: date | None,
    *,
    export_table_exists: bool,
) -> bool:
    if datalake_date is None:
        return False
    if not export_table_exists or exported_date is None:
        return True
    return datalake_date > exported_date
