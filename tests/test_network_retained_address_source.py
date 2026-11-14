# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained addresses resolve named generations and preserve actual column contracts."""

import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import network_custom_address_source as custom
from process import network_serving_read as serving
from process.network_address_projection import PinnedAddressSource
from tests.test_network_published_api_pin import published_entry as published_entry
from tests.test_registry_ptg_address_carry import _artifact

pytestmark = pytest.mark.asyncio


@pytest.fixture
def retained(published_entry):
    candidate = json.loads(published_entry["candidate_json"])
    candidate["validation_json"] = None
    published_entry["candidate_json"] = json.dumps(candidate)
    manifest = json.loads(published_entry["manifest_json"])
    source = PinnedAddressSource(candidate["schema_name"], "entity_address_unified", manifest["manifest_sha256"])
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value=manifest["generation_id"]),
        fetchrow=AsyncMock(return_value=published_entry),
    )
    return source, connection


async def test_retained_address_resolves_exact_historical_named_generation(retained):
    source, connection = retained
    manifest = await serving.resolve_retained_network_address_source(connection, source, control_schema="registry")
    assert manifest.schema_name == source.schema_name and manifest.manifest_sha256 == source.generation_id
    assert manifest.address_relation == serving.CompactAddressRelation(source.schema_name, source.table_name)
    assert connection.fetchval.await_args.args[1:] == (source.schema_name, source.generation_id)
    assert connection.fetchrow.await_args.args[1:] == (manifest.generation_id, True)
    assert "candidate.state='published'" in connection.fetchrow.await_args.args[0]


@pytest.mark.parametrize("damage", ["missing_generation", "missing_manifest", "candidate", "source", "revision"])
async def test_retained_address_refuses_missing_or_changed_logical_generation(retained, damage):
    source, connection = retained
    if damage == "missing_generation":
        connection.fetchval.return_value = None
    elif damage == "missing_manifest":
        connection.fetchrow.return_value = None
    else:
        entry = copy.deepcopy(connection.fetchrow.return_value)
        candidate = json.loads(entry["candidate_json"])
        candidate[
            {"candidate": "candidate_id", "source": "source_generations", "revision": "schema_revision"}[damage]
        ] = {
            "candidate": "changed",
            "source": {},
            "revision": 2,
        }[damage]
        entry["candidate_json"] = json.dumps(candidate)
        connection.fetchrow.return_value = entry
    with pytest.raises(serving.NetworkServingReadUnavailable):
        await serving.resolve_retained_network_address_source(connection, source, control_schema="registry")
    if damage == "missing_generation":
        connection.fetchrow.assert_not_awaited()


async def test_retained_address_requires_a_transaction_before_native_access(retained):
    source, connection = retained
    connection.is_in_transaction = lambda: False
    with pytest.raises(serving.NetworkServingReadUnavailable):
        await serving.resolve_retained_network_address_source(connection, source, control_schema="registry")
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("failure", [asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError])
async def test_retained_address_preserves_initial_native_lookup_error(retained, failure):
    source, connection = retained
    error = failure("refused")
    connection.fetchval.side_effect = error
    with pytest.raises(failure) as caught:
        await serving.resolve_retained_network_address_source(connection, source, control_schema="registry")
    assert caught.value is error


async def test_retained_address_rejects_invalid_control_scope_before_native_access(retained):
    source, connection = retained
    with pytest.raises(ValueError):
        await serving.resolve_retained_network_address_source(connection, source, control_schema='invalid"')
    connection.fetchval.assert_not_awaited()


def native_columns():
    return [
        {"attname": name, "type": "text", "atttypmod": -1, "attnotnull": False, "default_expr": None}
        for name in custom.EntityAddressUnified.__table__.columns.keys()
    ]


async def test_named_source_columns_keep_compacted_native_order(monkeypatch):
    columns = native_columns()
    connection = SimpleNamespace(fetch=AsyncMock(return_value=columns))
    projection = AsyncMock()
    monkeypatch.setattr(custom, "_projection_columns", projection)
    source = PinnedAddressSource("retained_addresses", "entity_address_unified", "edition-a")
    original = await custom._source_columns(connection, source, address=True)
    metadata_column = next(column for column in columns if column["attname"] == "source_record_ids")
    columns.remove(metadata_column)
    columns.append(metadata_column)
    compacted = await custom._source_columns(connection, source, address=True)
    assert compacted == tuple(columns) and compacted != original
    assert projection.await_args.args == (connection, '"retained_addresses"."entity_address_unified"')


@pytest.mark.parametrize("damage", ["missing", "extra", "duplicate", "renamed"])
async def test_named_source_columns_refuse_changed_column_membership(monkeypatch, damage):
    columns = native_columns()
    if damage == "missing":
        columns.pop()
    elif damage == "extra":
        columns.append({**columns[-1], "attname": "unexpected"})
    elif damage == "duplicate":
        columns.append(dict(columns[-1]))
    else:
        columns[-1]["attname"] = "renamed"
    projection = AsyncMock()
    monkeypatch.setattr(custom, "_projection_columns", projection)
    with pytest.raises(custom.NetworkCustomAddressSourceError, match="Retained unified address shape differs"):
        await custom._source_columns(
            SimpleNamespace(fetch=AsyncMock(return_value=columns)),
            PinnedAddressSource("retained_addresses", "entity_address_unified", "edition-a"),
            address=True,
        )
    projection.assert_not_awaited()


@pytest.fixture
def composition_verification(monkeypatch):
    receipt = _artifact()
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(receipt.as_dict())))
    monkeypatch.setattr(custom, "_require_transaction", AsyncMock())
    monkeypatch.setattr(
        custom,
        "_approved_identity",
        AsyncMock(
            return_value={
                "fingerprint": receipt.approved_records_sha256,
                "pairs": receipt.custom_pair_count,
            }
        ),
    )
    monkeypatch.setattr(custom, "verify_approved_site_sources", AsyncMock(return_value=()))
    monkeypatch.setattr(custom, "_source_alias_layout", AsyncMock(return_value=False))
    monkeypatch.setattr(custom, "_binding_shape", AsyncMock())
    monkeypatch.setattr(custom, "_verify_custom_rows", AsyncMock())
    monkeypatch.setattr(custom, "_office_lookup_shape", AsyncMock())
    return connection, receipt


@pytest.mark.parametrize(
    "field,changed",
    [
        ("type", "bigint"),
        ("atttypmod", 64),
        ("attnotnull", True),
        ("default_expr", "'changed'::text"),
    ],
)
async def test_composition_refuses_changed_column_attribute(composition_verification, monkeypatch, field, changed):
    connection, receipt = composition_verification
    columns = native_columns()
    output_columns = copy.deepcopy(columns)
    output_columns[0][field] = changed
    monkeypatch.setattr(custom, "_source_columns", AsyncMock(side_effect=[tuple(columns), tuple(output_columns)]))
    with pytest.raises(custom.NetworkCustomAddressSourceError, match="address shape differs"):
        await custom.verify_custom_address_source(connection, receipt)
    custom._verify_custom_rows.assert_not_awaited()


@pytest.mark.parametrize("retained", [False, True])
async def test_composition_keeps_named_recipe_and_current_or_retained_approval(
    composition_verification, monkeypatch, retained
):
    connection, receipt = composition_verification
    columns = AsyncMock(return_value=tuple(native_columns()))
    monkeypatch.setattr(custom, "_source_columns", columns)
    verify = custom.verify_retained_custom_address_source if retained else custom.verify_custom_address_source
    assert await verify(connection, receipt) == receipt
    assert custom._from_document(connection.fetchval.return_value) == receipt
    assert [call.args for call in columns.await_args_list] == [
        (connection, receipt.base_source),
        (connection, receipt.address_source),
    ]
    assert custom._approved_identity.await_args.kwargs == {"retained": retained}
    custom._binding_shape.assert_awaited_once()
    custom._verify_custom_rows.assert_awaited_once_with(connection, receipt)
    custom._office_lookup_shape.assert_awaited_once()
