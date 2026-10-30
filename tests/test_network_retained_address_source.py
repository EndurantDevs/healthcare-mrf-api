# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained addresses use the same immutable custody proof as serving reads."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from process import network_custom_address_source as custom
from process import network_serving_read as serving
from process.network_address_projection import PinnedAddressSource
from tests.test_registry_ptg_address_carry import _artifact


def source():
    return PinnedAddressSource("network_candidate_" + "1" * 32, "entity_address_unified", "a" * 64)


@pytest.mark.asyncio
async def test_retained_address_checks_full_custody_for_an_exact_historical_manifest(monkeypatch):
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value=7))
    manifest_by_name = {"candidate_json": '{"candidate_id":"example"}'}
    manifest = SimpleNamespace(schema_name=source().schema_name, manifest_sha256=source().generation_id)
    read = AsyncMock(return_value=manifest_by_name)
    custody = AsyncMock(return_value={"seal_sha256": "b" * 64})
    pin = Mock(return_value=manifest)
    monkeypatch.setattr(serving, "_read_manifest", read)
    monkeypatch.setattr(serving, "_candidate_custody", custody)
    monkeypatch.setattr(serving, "_pinned_manifest", pin)
    assert (
        await serving.resolve_retained_network_address_source(connection, source(), control_schema="registry")
        is manifest
    )
    read.assert_awaited_once_with(connection, '"registry"', 7, include_ineligible=True)
    custody.assert_awaited_once_with(connection, {"candidate_id": "example"}, "registry")
    pin.assert_called_once_with(manifest_by_name, custody=custody.return_value)
    assert connection.fetchval.await_args.args[1:] == (source().schema_name, source().generation_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("missing", "custody", "schema", "digest"))
async def test_retained_address_refuses_missing_or_changed_proof(monkeypatch, damage):
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value=7))
    read = AsyncMock(return_value={"candidate_json": "{}"})
    custody = AsyncMock(return_value={})
    manifest = SimpleNamespace(schema_name=source().schema_name, manifest_sha256=source().generation_id)
    if damage == "missing":
        connection.fetchval.return_value = None
    elif damage == "custody":
        custody.side_effect = ValueError("custody changed")
    elif damage == "schema":
        manifest.schema_name = "network_candidate_" + "2" * 32
    else:
        manifest.manifest_sha256 = "b" * 64
    monkeypatch.setattr(serving, "_read_manifest", read)
    monkeypatch.setattr(serving, "_candidate_custody", custody)
    monkeypatch.setattr(serving, "_pinned_manifest", Mock(return_value=manifest))
    with pytest.raises(serving.NetworkServingReadUnavailable):
        await serving.resolve_retained_network_address_source(connection, source(), control_schema="registry")
    if damage == "missing":
        read.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_address_requires_a_transaction_before_native_access():
    connection = SimpleNamespace(is_in_transaction=lambda: False, fetchval=AsyncMock())
    with pytest.raises(serving.NetworkServingReadUnavailable):
        await serving.resolve_retained_network_address_source(connection, source(), control_schema="registry")
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError))
async def test_retained_address_translates_initial_native_lookup_refusal(failure):
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(side_effect=failure("refused")))
    with pytest.raises(serving.NetworkServingReadUnavailable, match="verification failed"):
        await serving.resolve_retained_network_address_source(connection, source(), control_schema="registry")


@pytest.mark.asyncio
async def test_retained_address_rejects_invalid_control_scope_before_native_access():
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock())
    with pytest.raises(serving.NetworkServingReadUnavailable):
        await serving.resolve_retained_network_address_source(connection, source(), control_schema='invalid"')
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_custody_still_requires_the_existing_full_legacy_manifest_proof(monkeypatch):
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value=7))
    manifest_by_name = {"candidate_json": "{}"}
    monkeypatch.setattr(serving, "_read_manifest", AsyncMock(return_value=manifest_by_name))
    monkeypatch.setattr(serving, "_candidate_custody", AsyncMock(return_value=None))
    pin = Mock(side_effect=serving.NetworkServingReadUnavailable("legacy writer closure changed"))
    monkeypatch.setattr(serving, "_pinned_manifest", pin)
    with pytest.raises(serving.NetworkServingReadUnavailable, match="legacy writer closure changed"):
        await serving.resolve_retained_network_address_source(connection, source(), control_schema="registry")
    pin.assert_called_once_with(manifest_by_name, custody=None)


def catalog():
    return dict(
        schema_oid=1,
        table_oid=2,
        owner_oid=3,
        same_owner=True,
        relkind=b"r",
        relpersistence=b"p",
        relrowsecurity=False,
        relforcerowsecurity=False,
        **dict.fromkeys(
            ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"), False
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", (None, "oid", "custody"))
async def test_source_identity_uses_full_registered_custody_and_never_falls_back(monkeypatch, damage):
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=None),
        fetchrow=AsyncMock(return_value=catalog()),
        fetch=AsyncMock(
            return_value=[{"attname": name} for name in custom.EntityAddressUnified.__table__.columns.keys()]
        ),
    )
    resolve = AsyncMock(return_value=SimpleNamespace(address_table_oid=2 if damage is None else 9))
    if damage == "custody":
        resolve.side_effect = serving.NetworkServingReadUnavailable("changed")
    closed = AsyncMock(side_effect=AssertionError("registered source must use its sealed full census"))
    monkeypatch.setattr(serving, "resolve_retained_network_address_source", resolve)
    monkeypatch.setattr(custom, "_closed", closed)
    if damage is None:
        result = await custom._source_identity(
            connection, source(), ("reader",), address=True, control_schema="registry"
        )
        assert result[:3] == (1, 2, 3)
    else:
        with pytest.raises(custom.NetworkCustomAddressSourceError):
            await custom._source_identity(connection, source(), ("reader",), address=True, control_schema="registry")
    resolve.assert_awaited_once_with(connection, source(), control_schema="registry")
    closed.assert_not_awaited()


@pytest.mark.asyncio
async def test_ordinary_source_keeps_the_original_runtime_role_closure(monkeypatch):
    ordinary = PinnedAddressSource("retained_addresses", "entity_address_unified", "edition-a")
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=None),
        fetchrow=AsyncMock(return_value=catalog()),
        fetch=AsyncMock(
            return_value=[{"attname": name} for name in custom.EntityAddressUnified.__table__.columns.keys()]
        ),
    )
    closed = AsyncMock()
    resolve = AsyncMock(side_effect=AssertionError("ordinary source must retain its original closure"))
    monkeypatch.setattr(custom, "_closed", closed)
    monkeypatch.setattr(serving, "resolve_retained_network_address_source", resolve)
    await custom._source_identity(connection, ordinary, ("reader",), address=True, control_schema="registry")
    closed.assert_awaited_once_with(connection, 1, 3, ("reader",), ["entity_address_unified"])
    resolve.assert_not_awaited()


def native_columns():
    return [
        {"attname": name, "type": "text", "atttypmod": -1, "attnotnull": False, "default_expr": None}
        for name in custom.EntityAddressUnified.__table__.columns.keys()
    ]


@pytest.mark.asyncio
async def test_source_identity_accepts_compacted_metadata_and_pins_native_order(monkeypatch):
    ordinary = PinnedAddressSource("retained_addresses", "entity_address_unified", "edition-a")
    columns = native_columns()
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=None),
        fetchrow=AsyncMock(return_value=catalog()),
        fetch=AsyncMock(return_value=columns),
    )
    closed = AsyncMock()
    monkeypatch.setattr(custom, "_closed", closed)
    original = await custom._source_identity(connection, ordinary, ("reader",), address=True)
    # The real producer drops the old field and renames an appended replacement.
    metadata_column = next(column for column in columns if column["attname"] == "source_record_ids")
    columns.remove(metadata_column)
    columns.append(metadata_column)
    compacted = await custom._source_identity(connection, ordinary, ("reader",), address=True)
    assert compacted[:3] == original[:3] == (1, 2, 3)
    assert compacted[3] == custom._digest(columns)
    assert compacted[3] != original[3]
    assert closed.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("missing", "extra", "duplicate", "renamed"))
async def test_source_identity_refuses_changed_column_membership(monkeypatch, damage):
    columns = native_columns()
    if damage == "missing":
        columns.pop()
    elif damage == "extra":
        columns.append({**columns[-1], "attname": "unexpected"})
    elif damage == "duplicate":
        columns.append(dict(columns[-1]))
    else:
        columns[-1]["attname"] = "renamed"
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=None),
        fetchrow=AsyncMock(return_value=catalog()),
        fetch=AsyncMock(return_value=columns),
    )
    monkeypatch.setattr(custom, "_closed", AsyncMock())
    with pytest.raises(custom.NetworkCustomAddressSourceError, match="Retained unified address shape differs"):
        await custom._source_identity(connection, source(), ("reader",), address=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field_name,changed_value",
    (("type", "bigint"), ("atttypmod", 64), ("attnotnull", True), ("default_expr", "'changed'::text")),
)
async def test_source_identity_digest_keeps_every_native_column_attribute(monkeypatch, field_name, changed_value):
    columns = native_columns()
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=None),
        fetchrow=AsyncMock(return_value=catalog()),
        fetch=AsyncMock(return_value=columns),
    )
    monkeypatch.setattr(custom, "_closed", AsyncMock())
    original = await custom._source_identity(connection, source(), ("reader",), address=True)
    columns[0][field_name] = changed_value
    changed = await custom._source_identity(connection, source(), ("reader",), address=True)
    assert changed[:3] == original[:3]
    assert changed[3] != original[3]


@pytest.fixture
def composition_verification(monkeypatch):
    receipt = _artifact()
    generation = custom._generation(receipt)
    receipt = replace(
        receipt,
        generation_sha256=generation,
        address_source=PinnedAddressSource(
            "network_composition_" + receipt.composition_id.replace("-", ""), "entity_address_unified", generation
        ),
    )
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(receipt.as_dict())))
    monkeypatch.setattr(custom, "_require_transaction", AsyncMock())
    monkeypatch.setattr(
        custom,
        "_approved_identity",
        AsyncMock(return_value={"fingerprint": receipt.approved_records_sha256, "pairs": receipt.custom_pair_count}),
    )
    monkeypatch.setattr(custom, "verify_approved_site_sources", AsyncMock(return_value=()))
    monkeypatch.setattr(custom, "_source_alias_layout", AsyncMock(return_value=False))
    monkeypatch.setattr(
        custom,
        "_artifact_identity",
        AsyncMock(
            return_value=(
                receipt.schema_oid,
                receipt.address_table_oid,
                receipt.binding_table_oid,
                receipt.office_lookup_index_oid,
            )
        ),
    )
    return connection, receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("native_base", [False, True])
@pytest.mark.parametrize("damage", [None, "output_shape", "base_custody"])
async def test_composition_keeps_custody_and_column_shape_separate(
    composition_verification, monkeypatch, native_base, damage
):
    connection, receipt = composition_verification
    shape = "a" * 64 if not native_base else "d" * 64
    identity = receipt.source_oids if damage != "base_custody" else (99, *receipt.source_oids[1:])
    dependency = AsyncMock(return_value=identity)
    base_shape = AsyncMock(return_value=shape)
    output = AsyncMock(return_value=(11, 12, 15, "e" * 64 if damage == "output_shape" else shape))
    monkeypatch.setattr(custom, "_artifact_dependency_identity", dependency)
    monkeypatch.setattr(custom, "_source_shape", base_shape)
    monkeypatch.setattr(custom, "_source_identity", output)
    if damage:
        message = "address shape differs" if damage == "output_shape" else "source physical identity differs"
        with pytest.raises(custom.NetworkCustomAddressSourceError, match=message):
            await custom.verify_custom_address_source(connection, receipt)
    else:
        assert await custom.verify_custom_address_source(connection, receipt) == receipt
        assert custom._from_document(connection.fetchval.return_value) == receipt
    dependency.assert_awaited_once_with(
        connection, receipt.base_source, receipt.runtime_roles, address=True, control_schema=receipt.control_schema
    )
    if damage == "base_custody":
        base_shape.assert_not_awaited()
        output.assert_not_awaited()
    else:
        base_shape.assert_awaited_once_with(connection, receipt.source_oids[1], address=True)
        output.assert_awaited_once_with(connection, receipt.address_source, receipt.runtime_roles, address=True)
