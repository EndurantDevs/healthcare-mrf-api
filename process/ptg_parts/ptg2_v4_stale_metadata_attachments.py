# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional-relation discovery and attachment-count SQL for V4 repair."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_attempt_registry import (
    ATTEMPT_ATTACHMENTS,
    MANIFEST_STAGE_KINDS,
)


_OPTIONAL_ATTACHMENTS = tuple(
    attachment
    for attachment in ATTEMPT_ATTACHMENTS
    if attachment.optional_relation
)


def optional_attachment_probe(
    schema_name: str,
) -> tuple[str, dict[str, str]]:
    """Return one catalog-only query for explicitly optional relations."""

    fields = tuple(
        "to_regclass("
        f":optional_relation_{index}) IS NOT NULL "
        f"AS {_quote_ident(attachment.name)}"
        for index, attachment in enumerate(_OPTIONAL_ATTACHMENTS)
    )
    parameter_by_name = {
        f"optional_relation_{index}": (
            f"{_quote_ident(schema_name)}."
            f"{_quote_ident(attachment.table_name)}"
        )
        for index, attachment in enumerate(_OPTIONAL_ATTACHMENTS)
    }
    return "SELECT\n" + ",\n".join(fields), parameter_by_name


def present_optional_attachment_names(
    optional_presence_by_name: Mapping[str, Any],
) -> frozenset[str]:
    """Normalize the catalog probe to the exact optional registry names."""

    return frozenset(
        attachment.name
        for attachment in _OPTIONAL_ATTACHMENTS
        if optional_presence_by_name.get(attachment.name)
    )


def _attachment_presence_sql(
    schema_name: str,
    *,
    present_optional_names: frozenset[str],
) -> list[str]:
    schema = _quote_ident(schema_name)
    presence_sql_parts: list[str] = []
    for attachment in ATTEMPT_ATTACHMENTS:
        if (
            attachment.optional_relation
            and attachment.name not in present_optional_names
        ):
            presence_sql_parts.append(
                f"0::bigint AS {_quote_ident(attachment.name)}"
            )
            continue
        conditions = [
            f"{_quote_ident(column)} = :snapshot_id"
            for column in attachment.snapshot_columns
        ]
        conditions.extend(
            f"{_quote_ident(column)} = :internal_run_id"
            for column in attachment.run_columns
        )
        presence_sql_parts.append(
            "CASE WHEN EXISTS ("
            f"SELECT 1 FROM {schema}.{_quote_ident(attachment.table_name)} "
            f"WHERE {' OR '.join(conditions)}"
            ") THEN 1 ELSE 0 END::bigint "
            f"AS {_quote_ident(attachment.name)}"
        )
    return presence_sql_parts


def attachment_count_query(
    schema_name: str,
    *,
    stage_table_count: int,
    present_optional_names: frozenset[str],
) -> str:
    """Count attached state without parsing absent optional relations."""

    fields = _attachment_presence_sql(
        schema_name,
        present_optional_names=present_optional_names,
    )
    fields.extend(
        "CASE WHEN to_regclass("
        f":manifest_stage_{stage_index}) IS NULL "
        "THEN 0 ELSE 1 END::bigint "
        f"AS {_quote_ident(f'manifest_stage_{stage_kind}')}"
        for stage_index, stage_kind in enumerate(
            MANIFEST_STAGE_KINDS[:stage_table_count]
        )
    )
    fields.append(
        "CAST(:manifest_stage_identity_missing AS bigint) "
        "AS manifest_stage_identity_missing"
    )
    return "SELECT\n" + ",\n".join(fields)


__all__ = [
    "attachment_count_query",
    "optional_attachment_probe",
    "present_optional_attachment_names",
]
