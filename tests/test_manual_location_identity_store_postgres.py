# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Manual site evidence and native canonical identities on actual migrated tables."""

import asyncio
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from process import registry_record_store as store
from process.registry_record_store import (
    RegistryAddressUnavailable,
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)
from tests.test_manual_provider_identity_store_postgres import _actor, _apply, provider_db, serving_schema

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def location_db(provider_db):
    assert store._fast_module() is not None, "location tests require the actual compatible native canonicalizer"
    yield provider_db


def _address(**changes):
    return {
        "first_line": " 123 Example Street ",
        "second_line": " Suite 2 ",
        "city": " Sample City ",
        "state": " California ",
        "zip": " 90210-1234 ",
        "country": " US ",
        **changes,
    }


def _create(**changes):
    command = RegistryRecordCommand(
        "location",
        uuid4(),
        "create",
        0,
        {"display_name": " Example Site ", "aliases": [" B ", "A", "A"], "address_json": _address()},
        "Explicit site identity",
        uuid4().hex,
    )
    return replace(command, **changes)


async def test_native_address_evidence_and_separate_site_ids(location_db):
    connection, schema, sessions, _ = location_db
    actor, command = _actor(), _create()
    first = await _apply(sessions, schema, command, actor)
    equivalent = _create(fields={**command.fields, "address_json": _address(first_line="123 Example St", state="CA")})
    second = await _apply(sessions, schema, equivalent, actor)
    native = store._fast_module().canonicalize_batch(
        [("123 Example Street", "Suite 2", "Sample City", "California", "90210-1234", "US")]
    )[0]
    assert first["record"] == {
        "location_id": str(command.record_id),
        "display_name": "Example Site",
        "aliases": ["A", "B"],
        "address_json": {key: address_text.strip() for key, address_text in command.fields["address_json"].items()},
        "canonical_address_json": native,
        "archived": False,
        "revision": 1,
        "created_at": first["record"]["created_at"],
    }
    assert first["record_id"] != second["record_id"]
    assert first["record"]["canonical_address_json"] == second["record"]["canonical_address_json"]
    assert first["record"]["address_json"] != second["record"]["address_json"]
    assert set(native) == {
        "address_key",
        "identity_key",
        "premise_key",
        "premise_identity_key",
        "line1_norm",
        "unit_norm",
        "city_norm",
        "state_code",
        "zip5",
        "zip4",
        "country_code",
    }
    assert native["identity_key"] == "v2|123examplest|ste2||CA|90210|US|street"
    assert native["premise_key"] != native["address_key"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_location_registry') == 2


async def test_site_correction_archive_restore_and_immutable_retry(location_db, monkeypatch):
    connection, schema, sessions, _ = location_db
    actor, command = _actor(), _create()
    first = await _apply(sessions, schema, command, actor)
    corrected = replace(
        command,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields={**command.fields, "display_name": "Corrected Site", "address_json": _address(second_line=None)},
    )
    correction = await _apply(sessions, schema, corrected, actor)
    assert correction["revision"] == correction["custom_revision"] == 2
    assert correction["record"]["address_json"]["second_line"] is None
    assert correction["record"]["canonical_address_json"]["unit_norm"] == ""
    assert (
        correction["record"]["canonical_address_json"]["address_key"]
        != first["record"]["canonical_address_json"]["address_key"]
    )
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _apply(sessions, schema, replace(corrected, idempotency_key=uuid4().hex), actor)
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, replace(command, reason="Changed retry"), actor)
    monkeypatch.setattr(store, "_fast_module", lambda: None)
    assert await _apply(sessions, schema, command, actor) == first
    assert await _apply(sessions, schema, corrected, actor) == correction
    for operation, expected_revision, archived in [("archive", 2, True), ("restore", 3, False)]:
        change = replace(
            command, operation=operation, expected_revision=expected_revision, fields={}, idempotency_key=uuid4().hex
        )
        retained = await _apply(sessions, schema, change, actor)
        assert retained["record"]["location_id"] == str(command.record_id)
        assert retained["record"]["archived"] is archived
        assert retained["record"]["canonical_address_json"] == correction["record"]["canonical_address_json"]
        assert retained["record"]["address_json"] == correction["record"]["address_json"]
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (4, 0)


@pytest.mark.parametrize(
    "address",
    [
        None,
        [],
        {},
        {"first_line": "Incomplete"},
        _address(extra="unknown"),
        _address(first_line=None),
        _address(first_line=" "),
        _address(first_line="x" * 513),
        _address(second_line=""),
        _address(second_line="x" * 257),
        _address(city="x" * 129),
        _address(state="x" * 65),
        _address(zip="x" * 33),
        _address(country="x" * 65),
        _address(city=[]),
        _address(country=1),
        _address(first_line="line\0end"),
        _address(first_line="!@#"),
        _address(city="!@#"),
        _address(state="Unknown"),
        _address(zip="ab"),
        _address(country="GB"),
        _address(canonical_address_json={"address_key": "forged"}),
    ],
)
async def test_malformed_address_never_persists(location_db, address):
    connection, schema, sessions, _ = location_db
    command = _create()
    with pytest.raises(ValueError):
        await _apply(sessions, schema, replace(command, fields={**command.fields, "address_json": address}), _actor())
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_location_registry') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 0


async def test_server_canonical_fields_cannot_be_injected(location_db):
    connection, schema, sessions, _ = location_db
    command, actor = _create(), _actor()
    forged = replace(command, fields={**command.fields, "canonical_address_json": {"address_key": "forged"}})
    with pytest.raises(ValueError, match="editable_fields_invalid"):
        await _apply(sessions, schema, forged, actor)
    first = await _apply(sessions, schema, command, actor)
    corrected = replace(forged, operation="correct", expected_revision=1, idempotency_key=uuid4().hex)
    with pytest.raises(ValueError, match="editable_fields_invalid"):
        await _apply(sessions, schema, corrected, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 1
    async with sessions() as session:
        assert await get_registry_record(session, "location", command.record_id, schema=schema) == first["record"]


async def test_compatible_native_dependency_required_without_fallback(location_db, monkeypatch):
    connection, schema, sessions, _ = location_db
    monkeypatch.setattr(store, "_fast_module", lambda: None)
    async with sessions() as session, session.begin():
        with pytest.raises(RegistryAddressUnavailable, match="native_unavailable"):
            await apply_registry_record_command(session, _create(), _actor(), schema=schema)
        assert await session.scalar(text("SELECT 1")) == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_location_registry') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 0


async def test_site_read_list_bounds_and_uuid_identity(location_db):
    _, schema, sessions, _ = location_db
    actor, command = _actor(), _create()
    first = await _apply(sessions, schema, command, actor)
    await _apply(sessions, schema, _create(), actor)
    async with sessions() as session:
        assert await get_registry_record(session, "location", command.record_id, schema=schema) == first["record"]
        assert await get_registry_record(session, "location", uuid4(), schema=schema) is None
        assert len(await list_registry_records(session, "location", limit=1, schema=schema)) == 1
        assert len(await list_registry_records(session, "location", offset=1, schema=schema)) == 1
        assert await list_registry_records(session, "location", record_ids=[command.record_id], schema=schema) == [
            first["record"]
        ]
        for parameters in [
            {"limit": 101},
            {"offset": -1},
            {"record_ids": [uuid4()] * 101},
            {"record_ids": [command.record_id] * 2},
        ]:
            with pytest.raises(ValueError):
                await list_registry_records(session, "location", schema=schema, **parameters)
        with pytest.raises(ValueError, match="uuid_invalid"):
            await get_registry_record(session, "location", 1, schema=schema)
    with pytest.raises(ValueError, match="uuid_invalid"):
        await _apply(sessions, schema, replace(command, record_id=UUID(int=0)), actor)


async def test_concurrent_site_corrections_have_one_winner(location_db):
    connection, schema, sessions, _ = location_db
    actor, command = _actor(), _create()
    await _apply(sessions, schema, command, actor)
    correction = replace(command, operation="correct", expected_revision=1, idempotency_key=uuid4().hex)
    competing = replace(
        correction, idempotency_key=uuid4().hex, fields={**command.fields, "address_json": _address(second_line=None)}
    )
    outcomes = await asyncio.gather(
        _apply(sessions, schema, correction, actor), _apply(sessions, schema, competing, actor), return_exceptions=True
    )
    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, RegistryRecordConflict) for outcome in outcomes) == 1
    assert await connection.fetchval(f'SELECT revision FROM "{schema}".manual_location_registry') == 2
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 2


async def test_site_caller_rollback_preserves_evidence_and_history(location_db):
    connection, schema, sessions, _ = location_db
    actor, command = _actor(), _create()
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with sessions() as session, session.begin():
            await apply_registry_record_command(session, command, actor, schema=schema)
            raise RuntimeError("caller rollback")
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_location_registry') == 0
    first = await _apply(sessions, schema, command, actor)
    corrected = replace(
        command,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields={**command.fields, "address_json": _address(first_line="456 Example Road", second_line=None)},
    )
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with sessions() as session, session.begin():
            await apply_registry_record_command(session, corrected, actor, schema=schema)
            raise RuntimeError("caller rollback")
    async with sessions() as session:
        assert await get_registry_record(session, "location", command.record_id, schema=schema) == first["record"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 1
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (1, 0)
