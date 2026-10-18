# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure column-shape proof for the NPPES runtime catalog fence."""

from __future__ import annotations

import sys
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nppes_public_evidence_catalog as catalog


def _exact_column_records() -> list[dict[str, object]]:
    column_records: list[dict[str, object]] = []
    for table_name, column_names in catalog._TABLE_COLUMNS.items():
        for column_name in column_names:
            is_new_table = table_name in catalog._NEW_TABLES
            data_type = catalog._NEW_TYPE_BY_COLUMN[column_name] if is_new_table else "legacy-type-not-checked"
            is_nullable_member = table_name == catalog._MEMBER and column_name in catalog._NULLABLE_MEMBER_COLUMNS
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
        lambda records: records[_record_index(records, catalog._ADMISSION, "admission_ref")].update(data_type="wrong"),
        lambda records: records[_record_index(records, catalog._MEMBER, "entity_type_code")].update(attnotnull=True),
        lambda records: records[_record_index(records, catalog._ADMISSION, "created_at")].update(default_expr=None),
        lambda records: records[_record_index(records, catalog._COMMON, "nppes_admission_ref")].update(
            data_type="text"
        ),
        lambda records: records[_record_index(records, catalog._SOURCE_RECORD, "nppes_admission_ref")].update(
            attnotnull=True
        ),
    ),
)
def test_catalog_column_contract_rejects_each_shape_drift(mutation) -> None:
    exact_records = _exact_column_records()
    assert catalog._has_exact_columns(tuple(exact_records)) is True
    mutated_records = deepcopy(exact_records)
    mutation(mutated_records)
    assert catalog._has_exact_columns(tuple(mutated_records)) is False


def _control_columns():
    return [
        dict(
            relname=table,
            attname=column,
            data_type=catalog._CONTROL_TYPES[column],
            attnotnull=not (table == catalog._SOURCE_RELEASE and column in catalog._NULLABLE_RELEASE),
            default_expr="transaction_timestamp()" if column in {"created_at", "updated_at", "sealed_at"} else None,
        )
        for table, columns in catalog._CONTROL_COLUMNS.items()
        for column in columns
    ]


def _control_constraints():
    prepared_records = []
    for name in sorted(catalog._CONTROL_CONSTRAINTS):
        record_by_key = dict(
            conname=name,
            condeferrable=False,
            condeferred=False,
            convalidated=True,
            conenforced=True,
            parent_schema=None,
            parent_table=None,
            columns=[],
            parent_columns=[],
            confdeltype=" ",
            confupdtype=" ",
            confmatchtype=" ",
        )
        if name in catalog._CONTROL_FOREIGN_KEYS:
            child, columns, parent, parent_columns = catalog._CONTROL_FOREIGN_KEYS[name]
            record_by_key.update(
                relname=child,
                contype="f",
                columns=list(columns),
                parent_schema="source_schema",
                parent_table=parent,
                parent_columns=list(parent_columns),
                confdeltype="r",
                confupdtype="a",
                confmatchtype="s",
            )
        elif name in catalog._CONTROL_KEYS:
            table, kind, columns = catalog._CONTROL_KEYS[name]
            record_by_key.update(relname=table, contype=kind, columns=list(columns))
        else:
            table = next(
                (
                    table
                    for table in sorted(catalog._CONTROL_COLUMNS, key=len, reverse=True)
                    if name.startswith(table + "_")
                ),
                catalog._CHAIN,
            )
            record_by_key.update(relname=table, contype="c")
        prepared_records.append(record_by_key)
    return prepared_records


def _control_triggers():
    records = []
    for (table, name), (
        kind,
        function,
        deferred,
        initially,
        definer,
        new_table,
    ) in catalog._candidate_triggers().items():
        records.append(
            dict(
                relname=table,
                tgname=name,
                tgtype=kind,
                tgenabled="A",
                tgdeferrable=deferred,
                tginitdeferred=initially,
                function_schema="source_schema",
                function_name=function,
                security_definer=definer,
                exact_search_path=True,
                new_table=new_table,
                old_table=None,
            )
        )
    return records


def test_chain_archive_append_contract_uses_native_ascii_identifier_truncation():
    key = (catalog._CHAIN_ARCHIVE, "public_evidence_nppes_registry_chain_archive_admission_append_g")
    expected = catalog._candidate_triggers()
    assert key in expected
    assert expected[key][1] == "guard_public_evidence_nppes_registry_chain_archive_admission_ap"
    assert len(key[1].encode("ascii")) == len(expected[key][1].encode("ascii")) == 63


def test_chain_seal_guards_use_exact_native_ascii_identifier_truncation():
    expected = catalog._candidate_triggers()
    table = "public_evidence_nppes_registry_chain_admission_seal"
    for name in (
        "public_evidence_nppes_registry_chain_admission_seal_mutation_gu",
        "public_evidence_nppes_registry_chain_admission_seal_truncate_gu",
    ):
        assert (table, name) in expected
        assert len(name.encode("ascii")) == 63
    assert all(len(name.encode("ascii")) <= 63 for _, name in expected)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda rows: rows.pop(),
        lambda rows: rows[0].update(data_type="text"),
        lambda rows: rows[0].update(attnotnull=False),
        lambda rows: rows[-1].update(default_expr=None),
        lambda rows: rows[0].update(identity_mode="a"),
        lambda rows: rows[0].update(generated_mode="s"),
        lambda rows: rows.append(dict(rows[0])),
    ],
)
def test_candidate_columns_require_exact_control_shapes(mutation):
    rows = _control_columns()
    assert catalog._has_candidate_columns(rows)
    mutation(rows)
    assert not catalog._has_candidate_columns(rows)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda rows: rows.pop(),
        lambda rows: rows[0].update(convalidated=False),
        lambda rows: rows[0].update(conenforced=False),
        lambda rows: rows[0].update(condeferrable=True),
        lambda rows: next(row for row in rows if row["contype"] == "f").update(parent_schema="other_schema"),
        lambda rows: next(row for row in rows if row["contype"] == "f").update(parent_table="unrelated"),
        lambda rows: next(row for row in rows if row["contype"] == "f").update(parent_columns=["admission_ref"]),
        lambda rows: next(row for row in rows if row["contype"] == "f").update(confdeltype="c"),
        lambda rows: next(row for row in rows if row["contype"] == "p").update(columns=["unrelated"]),
    ],
)
def test_candidate_controls_preserve_native_keys_and_full_foreign_identities(mutation):
    rows = _control_constraints()
    assert catalog._has_candidate_constraints(rows, "source_schema")
    mutation(rows)
    assert not catalog._has_candidate_constraints(rows, "source_schema")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda rows: rows.pop(),
        lambda rows: rows[0].update(tgenabled="D"),
        lambda rows: rows[0].update(security_definer=False),
        lambda rows: rows[0].update(function_schema="other_schema"),
        lambda rows: rows[0].update(function_name="unrelated"),
        lambda rows: rows[0].update(exact_search_path=False),
        lambda rows: rows[-1].update(new_table="other_rows"),
        lambda rows: rows[-2].update(tgdeferrable=False),
        lambda rows: rows.append(
            dict(rows[0], tgname="public_evidence_nppes_registry_admission_integrity_guard", relname=catalog._ADMISSION)
        ),
    ],
)
def test_candidate_controls_keep_immutable_and_chain_guards_but_reject_archive_validator(mutation):
    rows = _control_triggers()
    assert catalog._has_candidate_triggers(rows, "source_schema")
    mutation(rows)
    assert not catalog._has_candidate_triggers(rows, "source_schema")


@pytest.mark.asyncio
async def test_candidate_catalog_requires_authenticated_custody_before_absent_archive_guard(monkeypatch):
    custody = AsyncMock()
    monkeypatch.setitem(
        sys.modules,
        "process.nppes_public_evidence_control_custody",
        SimpleNamespace(assert_nppes_control_custody=custody),
    )
    columns = AsyncMock(return_value=_control_columns())
    monkeypatch.setattr(catalog, "_column_records", columns)
    monkeypatch.setattr(catalog, "_candidate_constraints", AsyncMock(return_value=_control_constraints()))
    monkeypatch.setattr(catalog, "_trigger_records", AsyncMock(return_value=_control_triggers()))
    connection, policy = object(), object()
    await catalog.assert_nppes_candidate_catalog(connection, "source_schema", policy)
    custody.assert_awaited_once_with(connection, "source_schema", policy)
    custody.side_effect = RuntimeError("untrusted custody")
    columns.reset_mock()
    with pytest.raises(catalog.writer_error().__class__, match="nppes_public_evidence_writer_invalid"):
        await catalog.assert_nppes_candidate_catalog(connection, "source_schema", policy)
    columns.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_fence_still_requires_its_original_archive_validator(monkeypatch):
    records = _control_triggers()
    for table in (catalog._MEMBER,):
        for suffix, kind in (("mutation_guard", 27), ("truncate_guard", 34)):
            records.append(dict(records[0], relname=table, tgname=table + "_" + suffix, tgtype=kind))
    for table in catalog._APPEND_TABLES:
        if table != catalog._CHAIN_ARCHIVE:
            records.append(dict(records[-1], relname=table, tgname=table + "_admission_append_guard", tgtype=4))
    records.append(
        dict(
            records[-2],
            relname=catalog._ADMISSION,
            tgname=catalog._ADMISSION + "_integrity_guard",
            tgtype=5,
            tgdeferrable=True,
            tginitdeferred=True,
            function_name=catalog._ADMISSION_VALIDATOR,
            security_definer=True,
        )
    )
    monkeypatch.setattr(catalog, "_trigger_records", AsyncMock(return_value=records))
    assert await catalog._has_exact_triggers(object(), "source_schema")
    assert not catalog._has_candidate_triggers(records, "source_schema")
