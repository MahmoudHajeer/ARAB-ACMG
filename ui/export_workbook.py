from __future__ import annotations

import datetime as dt
from io import BytesIO
from typing import Final, Iterable

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Alignment, Font, PatternFill

try:  # pragma: no cover
    from ui.schema_columns import PRE_GME_EXTRA_COLUMNS, REQUIRED_COLUMNS, column_payload, pre_gme_columns
except ModuleNotFoundError:  # pragma: no cover
    from schema_columns import PRE_GME_EXTRA_COLUMNS, REQUIRED_COLUMNS, column_payload, pre_gme_columns

PRE_GME_EXPORT_FILENAME: Final[str] = "supervisor_variant_registry_brca_pre_gme_v1.xlsx"
PRE_GME_EXPORT_SHEET: Final[str] = "Pre_GME_Review"
PRE_GME_PIPELINE_STAGE: Final[str] = "PRE_GME_REVIEW"


def export_metadata_lines(created_at: str | None = None) -> list[str]:
    timestamp = created_at or dt.datetime.now(dt.UTC).strftime("%d/%m/%Y %H:%M")
    lines = [
        f"ARAB_ACMG_PRE_GME_PIPELINE - Created on: {timestamp}",
        '##WORKFLOW=<ID=PRE_GME_REVIEW,Description="BRCA review artifact generated after direct raw-to-checkpoint extraction from ClinVar + gnomAD and before adding GME frequencies">',
        '##RULE=<ID=HEADER_FLOOR,Description="The requested publication-facing header is treated as the minimum required schema; unsupported fields remain NULL instead of being guessed">',
        '##RULE=<ID=EXTRAS,Description="Columns after the required header are pipeline extras and are colored differently in the workbook header">',
    ]
    for column in column_payload(pre_gme_columns()):
        lines.append(
            f'##INFO=<ID={column["name"]},Number=1,Type=String,Description="{column["description"]}">'
        )
    return lines


def export_header_columns() -> list[str]:
    return [column_name for column_name, _ in pre_gme_columns()]


def export_column_payload() -> list[dict[str, str]]:
    return column_payload(pre_gme_columns())


def normalize_cell_value(value: object) -> object:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return value


def map_pre_gme_export_row(row: dict[str, object]) -> list[object]:
    return [normalize_cell_value(row.get(column_name)) for column_name in export_header_columns()]


def build_pre_gme_workbook_bytes(
    rows: Iterable[dict[str, object]],
    created_at: str | None = None,
) -> bytes:
    workbook = Workbook(write_only=True)
    worksheet = workbook.create_sheet(PRE_GME_EXPORT_SHEET)

    title_font = Font(bold=True, size=12, color="FFFFFF")
    title_fill = PatternFill("solid", fgColor="1D4E89")
    info_font = Font(color="0B304F")
    required_header_font = Font(bold=True, color="FFFFFF")
    required_header_fill = PatternFill("solid", fgColor="163B64")
    extra_header_fill = PatternFill("solid", fgColor="B45F06")

    for index, line in enumerate(export_metadata_lines(created_at=created_at), start=1):
        cell = WriteOnlyCell(worksheet, value=line)
        cell.alignment = Alignment(wrap_text=True)
        if index == 1:
            cell.font = title_font
            cell.fill = title_fill
        else:
            cell.font = info_font
        worksheet.append([cell])

    required_names = {name for name, _ in REQUIRED_COLUMNS}
    header_row: list[WriteOnlyCell] = []
    for header in export_header_columns():
        cell = WriteOnlyCell(worksheet, value=header)
        cell.font = required_header_font
        cell.fill = required_header_fill if header in required_names else extra_header_fill
        header_row.append(cell)
    worksheet.append(header_row)

    for row in rows:
        worksheet.append(map_pre_gme_export_row(row))

    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()
