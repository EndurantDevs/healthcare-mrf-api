# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure column-shape proof for the NPPES runtime catalog fence."""

from __future__ import annotations

from copy import deepcopy

import pytest

from process import nppes_public_evidence_catalog as catalog


def _exact_column_records() -> list[dict[str, object]]:
    column_records: list[dict[str, object]] = []
    for table_name, column_names in catalog._TABLE_COLUMNS.items():
        for column_name in column_names:
            is_new_table = table_name in catalog._NEW_TABLES
            data_type = (
                catalog._NEW_TYPE_BY_COLUMN[column_name]
                if is_new_table
                else "legacy-type-not-checked"
            )
            is_nullable_member = (
                table_name == catalog._MEMBER
                and column_name in catalog._NULLABLE_MEMBER_COLUMNS
            )
            is_not_null = not is_nullable_member if is_new_table else False
            if column_name == "nppes_admission_ref":
                data_type = "character varying(50)"
                is_not_null = table_name != catalog._SOURCE_RECORD
            default_expression = None
            if is_new_table and column_name in {"created_at", "sealed_at"}:
                default_expression = "transaction_timestamp()"
            column_records.append(
                {
                    "relname": table_name,
                    "attname": column_name,
                    "data_type": data_type,
                    "attnotnull": is_not_null,
                    "default_expr": default_expression,
                }
            )
    return column_records


def _record_index(
    records: list[dict[str, object]],
    table_name: str,
    column_name: str,
) -> int:
    return next(
        index
        for index, record in enumerate(records)
        if record["relname"] == table_name and record["attname"] == column_name
    )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda records: records.pop(),
        lambda records: records[
            _record_index(records, catalog._ADMISSION, "admission_ref")
        ].update(data_type="wrong"),
        lambda records: records[
            _record_index(records, catalog._MEMBER, "entity_type_code")
        ].update(attnotnull=True),
        lambda records: records[
            _record_index(records, catalog._ADMISSION, "created_at")
        ].update(default_expr=None),
        lambda records: records[
            _record_index(records, catalog._COMMON, "nppes_admission_ref")
        ].update(data_type="text"),
        lambda records: records[
            _record_index(records, catalog._SOURCE_RECORD, "nppes_admission_ref")
        ].update(attnotnull=True),
    ),
)
def test_catalog_column_contract_rejects_each_shape_drift(mutation) -> None:
    exact_records = _exact_column_records()
    assert catalog._has_exact_columns(tuple(exact_records)) is True
    mutated_records = deepcopy(exact_records)
    mutation(mutated_records)
    assert catalog._has_exact_columns(tuple(mutated_records)) is False
