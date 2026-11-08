# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Column-order compatibility preserves named schema and historical receipt semantics."""

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from db.models import CodeCatalog, CodeCrosswalk, CodeRelationship, CodeSynonym
from process import code_sets_result_archive as codes
from process import entity_address_snapshot_receipt as catalog
from process import ms_drg_result_generation as drg
from process import reference_family_archive as native
from process import scoped_catalog_binding as binding
from process import scoped_catalog_publication as publication


def _columns(model=CodeCatalog):
    return [
        (column.name, str(column.type.compile(dialect=postgresql.dialect())), -1, not column.nullable, "", "", None)
        for column in model.__table__.columns
    ]


def _appended_attribution(columns):
    return [column for column in columns if column[0] != "source_attribution"] + [
        column for column in columns if column[0] == "source_attribution"
    ]


def _result(columns):
    return SimpleNamespace(all=lambda: columns, scalars=lambda: SimpleNamespace(all=lambda: columns))


async def _signature(module, columns, primary=("code_system", "code")):
    session = SimpleNamespace(execute=AsyncMock(side_effect=[_result(columns), _result(list(primary))]))
    if module is codes:
        return await codes._column_signature(session, 7)
    return await drg._table_shape(session, 7, CodeCatalog)


@pytest.mark.asyncio
@pytest.mark.parametrize("module", [codes, drg])
async def test_appended_column_keeps_historical_receipt(module):
    columns = _columns()
    expected = (
        tuple(columns)
        if module is codes
        else hashlib.sha256(
            json.dumps([list(map(list, columns)), ["code_system", "code"]], default=str, separators=(",", ":")).encode()
        ).hexdigest()
    )
    assert await _signature(module, columns) == expected
    assert await _signature(module, _appended_attribution(columns)) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("module", [codes, drg])
@pytest.mark.parametrize("field,changed", [(1, "integer"), (2, 99), (3, True), (4, "s"), (5, "a"), (6, "'x'")])
async def test_receipts_keep_native_column_semantics(module, field, changed):
    columns = _appended_attribution(_columns())
    prior = await _signature(module, columns)
    changed_fields = list(columns[-1])
    changed_fields[field] = changed
    columns[-1] = tuple(changed_fields)
    assert await _signature(module, columns) != prior


@pytest.mark.asyncio
@pytest.mark.parametrize("module", [codes, drg])
@pytest.mark.parametrize("change", ["extra", "missing", "duplicate", "unknown", "key"])
async def test_column_order_rejects_schema_drift(module, change):
    columns = _appended_attribution(_columns())
    if change == "extra":
        columns.append(("extra", "text", -1, False, "", "", None))
    elif change == "missing":
        columns.pop()
    elif change == "duplicate":
        columns[-1] = columns[0]
    elif change == "unknown":
        columns[-1] = ("unknown", *columns[-1][1:])
    with pytest.raises(RuntimeError, match="columns differ|key differs"):
        await _signature(
            module, columns, primary=("code", "code_system") if change == "key" else ("code_system", "code")
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("model", [CodeCatalog, CodeCrosswalk, CodeSynonym, CodeRelationship])
@pytest.mark.parametrize("key_changed", [False, True])
async def test_reordered_binding_checks_native_key(model, key_changed):
    columns = _appended_attribution(_columns(model))
    binding_columns = [
        (name, kind, not_null, generated, identity, default is not None)
        for name, kind, _modifier, not_null, generated, identity, default in columns
    ]
    primary_columns = list(model.__table__.primary_key.columns.keys())
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _result(binding_columns),
                _result(columns),
                _result(list(reversed(primary_columns)) if key_changed else primary_columns),
            ]
        )
    )
    if key_changed:
        with pytest.raises(RuntimeError, match="key differs"):
            await binding._require_model_columns(session, 7, model)
    else:
        await binding._require_model_columns(session, 7, model)


def _catalog_shape(*, appended):
    columns = _appended_attribution(_columns()) if appended else _columns()
    native_columns = [
        dict(
            attnum=number,
            attname=column[0],
            type=column[1],
            attnotnull=column[3],
            attgenerated=column[4],
            attidentity=column[5],
            default_expression=column[6],
            collation_schema="pg_catalog",
            collation_name="default",
        )
        for number, column in enumerate(columns, 1)
    ]
    numbers_by_name = {column["attname"]: column["attnum"] for column in native_columns}
    attribution = numbers_by_name["source_attribution"]
    updated = numbers_by_name["updated_at"]
    constraints = [
        dict(
            contype=kind,
            condeferrable=False,
            condeferred=False,
            convalidated=True,
            key_columns=key,
            referenced_columns=None,
            referenced_table=None,
            referenced_in_archive_schema=None,
            check_expression=check,
        )
        for kind, key, check in (
            ("p", "{1,2}", None),
            ("c", "{" + str(attribution) + "}", "source_attribution IS NULL OR source_attribution <> ''"),
        )
    ]
    return native_columns, constraints, [_catalog_index(attribution, updated)]


def _catalog_index(attribution, updated):
    return dict(
        indisunique=False,
        indisprimary=False,
        indimmediate=True,
        indisvalid=True,
        indnkeyatts=1,
        indnatts=2,
        method="btree",
        predicate=None,
        expressions=None,
        keys=f"{attribution} {updated}",
        options="0",
        key_attributes=[
            dict(
                position=position,
                attribute_number=number,
                collation_schema="pg_catalog",
                collation_name="default",
                opclass_schema="pg_catalog",
                opclass_name="text_ops",
            )
            for position, number in enumerate((attribution, updated))
        ],
    )


def _schema_drift(columns, constraints, indexes, change):
    column_changes_by_field = {
        "type": "integer",
        "attnotnull": True,
        "attgenerated": "s",
        "attidentity": "a",
        "default_expression": "'x'",
        "collation_name": "C",
    }
    if change in column_changes_by_field:
        columns[-2][change] = column_changes_by_field[change]
        return
    if change == "check":
        constraints[1]["check_expression"] = "source_attribution IS NOT NULL"
        return
    if change in {"convalidated", "condeferrable", "condeferred"}:
        constraints[1][change] = not constraints[1][change]
        return
    if change == "key":
        constraints[0]["key_columns"] = "{2,1}"
        return
    if change == "foreign":
        constraints.append({**constraints[0], "contype": "f", "referenced_table": "foreign_table"})
        return
    if change == "index_key":
        indexes[0]["keys"] = "1 2"
        return
    if change == "opclass":
        indexes[0]["key_attributes"][0]["opclass_name"] = "text_pattern_ops"
        return
    if change == "predicate":
        indexes[0]["predicate"] = "source_attribution IS NOT NULL"
        return
    if change == "missing_index":
        indexes.clear()
        return
    if change == "extra_index":
        indexes.append({**indexes[0], "indisunique": True})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        None,
        "extra_index",
        "type",
        "attnotnull",
        "attgenerated",
        "attidentity",
        "default_expression",
        "collation_name",
        "check",
        "convalidated",
        "condeferrable",
        "condeferred",
        "key",
        "foreign",
        "index_key",
        "opclass",
        "predicate",
        "missing_index",
    ],
)
async def test_named_schema_preserves_native_boundaries(monkeypatch, change):
    before = _catalog_shape(appended=True)
    after = _catalog_shape(appended=False)
    _schema_drift(*after, change)
    for name, prior, candidate in zip(("columns", "constraints", "indexes"), before, after, strict=True):
        monkeypatch.setattr(catalog, "_catalog_" + name, AsyncMock(side_effect=[prior, candidate]))
    session = SimpleNamespace(in_transaction=lambda: True)
    if change in {None, "extra_index"}:
        await publication.require_preserved_catalog_schema(session, ("before", 7), ("after", 8))
    else:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="columns|constraints|index"):
            await publication.require_preserved_catalog_schema(session, ("before", 7), ("after", 8))
