# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Manual site evidence and native canonical identities on actual migrated tables."""

import asyncio
import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from process import registry_record_store as store
from process.network_custom_address_source import _APPROVED_CTE
from process.registry_record_store import (
    RegistryAddressUnavailable,
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)
from tests.test_manual_provider_identity_store_postgres import _actor, _apply, provider_db, serving_schema
from tests.test_manual_provider_identity_store_postgres import _create as _provider
from tests.test_registry_approval_store_postgres import _approve
from tests.test_registry_approval_store_postgres import _command as _approval
from tests.test_registry_record_store_postgres import _create as _network

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


def _membership(network_id, provider_id, sites, *, system="manual"):
    return RegistryRecordCommand(
        "membership",
        network_id,
        "create",
        0,
        {
            "memberships_json": [
                {
                    "network_id": network_id,
                    "provider_system": system,
                    "provider_id": provider_id,
                    "location_id": str(site),
                    "evidence_id": "example-review",
                }
                for site in sites
            ]
        },
        "Explicit provider offices",
        uuid4().hex,
    )


async def _pair_records(database):
    _, schema, sessions, _ = database
    actor = _actor()
    network = await _apply(sessions, schema, _network("network"), actor)
    provider = await _apply(sessions, schema, _provider(), actor)
    sites = [await _apply(sessions, schema, _create(), actor) for _ in range(2)]
    return actor, network, provider, sites


async def test_membership_retains_only_exact_pair_with_projection_key_and_immutable_replay(location_db, monkeypatch):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    command = _membership(network["record_id"], provider["record_id"], [sites[0]["record_id"]])
    retained = await _apply(sessions, schema, command, actor)
    bindings = await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding')
    assert len(bindings) == 1
    binding = bindings[0]
    assert (binding["provider_system"], binding["provider_id"], str(binding["location_id"])) == (
        "manual",
        provider["record_id"],
        sites[0]["record_id"],
    )
    assert (binding["entity_type"], binding["entity_id"], binding["archived"], binding["revision"]) == (
        "manual",
        provider["record_id"],
        False,
        1,
    )
    assert sites[0]["record"]["canonical_address_json"] == sites[1]["record"]["canonical_address_json"]
    approval = await _approve(
        connection, schema, await _approval(connection, schema, network, provider, sites[0], retained), actor
    )
    projected = await connection.fetch(
        _APPROVED_CTE.format(control=f'"{schema}"') + "SELECT * FROM custom", approval["approved_revision"]
    )
    assert len(projected) == 1 and projected[0]["location_key"] == binding["location_key"]
    monkeypatch.setattr(
        store, "partition_network_membership_commands", lambda _: pytest.fail("replay must precede native allocation")
    )
    assert await _apply(sessions, schema, command, actor) == retained
    assert await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding') == bindings
    history = await connection.fetchval(
        f"SELECT record_json FROM \"{schema}\".registry_record_history WHERE record_kind='membership'"
    )
    assert json.loads(history)["memberships_json"] == command.fields["memberships_json"]


async def test_npi_manual_office_retained_but_source_site_never_inserted(location_db):
    connection, schema, sessions, _ = location_db
    actor, network, _, sites = await _pair_records(location_db)
    await connection.execute(f'INSERT INTO "{schema}".npi VALUES(1000000004)')
    await connection.execute(
        f'INSERT INTO "{schema}".registry_site_binding '
        "(binding_id,source_generation,provider_system,provider_id,location_id,location_key,"
        "address_row_sha256,source_receipt_json) VALUES($1,1,'npi','1000000004',$2,$3,$3,'{}'::jsonb)",
        uuid4(),
        UUID(sites[1]["record_id"]),
        "a" * 64,
    )
    command = _membership(network["record_id"], "1000000004", [site["record_id"] for site in sites], system="npi")
    await _apply(sessions, schema, command, actor)
    bindings = await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding')
    assert len(bindings) == 1 and str(bindings[0]["location_id"]) == sites[0]["record_id"]
    assert (bindings[0]["entity_type"], bindings[0]["entity_id"]) == ("npi", "1000000004")


@pytest.mark.parametrize("conflict", ["location_key", "entity_type", "entity_id", "archived"])
async def test_conflicting_binding_rolls_back_all_new_pairs_and_membership(location_db, conflict):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    first = _membership(network["record_id"], provider["record_id"], [sites[0]["record_id"]])
    original = await _apply(sessions, schema, first, actor)
    assignment_by_field = {
        "location_key": "location_key=repeat('b',64)",
        "entity_type": "entity_type='other'",
        "entity_id": "entity_id='other'",
        "archived": "archived=true",
    }
    await connection.execute(f'UPDATE "{schema}".manual_provider_location_binding SET {assignment_by_field[conflict]}')
    before = await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding')
    revision = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    correction = replace(
        first,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields=_membership(network["record_id"], provider["record_id"], [site["record_id"] for site in sites]).fields,
    )
    with pytest.raises(RegistryRecordConflict, match="manual_location_binding_conflict"):
        await _apply(sessions, schema, correction, actor)
    assert await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding') == before
    async with sessions() as session:
        assert (
            await get_registry_record(session, "membership", network["record_id"], schema=schema) == original["record"]
        )
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == revision
    assert (
        await connection.fetchval(
            f"SELECT count(*) FROM \"{schema}\".registry_record_history WHERE record_kind='membership'"
        )
        == 1
    )


async def test_binding_and_new_location_roll_back_with_caller_transaction(location_db):
    connection, schema, sessions, _ = location_db
    actor, network, provider, _ = await _pair_records(location_db)
    revision = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    new_site = _create()
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with sessions() as session, session.begin():
            await apply_registry_record_command(session, new_site, actor, schema=schema, source_schema=schema)
            await apply_registry_record_command(
                session,
                _membership(network["record_id"], provider["record_id"], [new_site.record_id]),
                actor,
                schema=schema,
                source_schema=schema,
            )
            assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".manual_provider_location_binding')) == 1
            raise RuntimeError("caller rollback")
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_location_binding') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_location_registry') == 2
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == revision
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == revision


async def test_command_document_defaults_add_and_retains_only_added_exact_office(location_db):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    command = _membership(network["record_id"], provider["record_id"], [site["record_id"] for site in sites])
    rows = command.fields["memberships_json"]
    rows[1]["operation"] = "remove"
    retained = await _apply(sessions, schema, command, actor)
    assert retained["record"]["memberships_json"] == rows
    bindings = await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding')
    assert len(bindings) == 1 and str(bindings[0]["location_id"]) == sites[0]["record_id"]
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approved_record') == 0
    assert await _apply(sessions, schema, command, actor) == retained
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, replace(command, fields={"memberships_json": [rows[1]]}), actor)
    assert await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding') == bindings


async def test_removal_only_draft_archive_restore_does_not_create_a_manual_binding(location_db):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    command = _membership(network["record_id"], provider["record_id"], [sites[0]["record_id"]])
    command.fields["memberships_json"][0]["operation"] = "remove"
    retained = await _apply(sessions, schema, command, actor)
    for operation, expected_revision in (("archive", 1), ("restore", 2)):
        change = replace(
            command, operation=operation, expected_revision=expected_revision, fields={}, idempotency_key=uuid4().hex
        )
        restored = await _apply(sessions, schema, change, actor)
        assert restored["record"]["memberships_json"] == retained["record"]["memberships_json"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_location_binding') == 0
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


async def test_removal_correction_preserves_existing_binding_and_rejects_stale_cas(location_db):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    command = _membership(network["record_id"], provider["record_id"], [sites[0]["record_id"]])
    await _apply(sessions, schema, command, actor)
    bindings = await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding')
    removal = replace(
        command,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields={"memberships_json": [{**command.fields["memberships_json"][0], "operation": "remove"}]},
    )
    retained = await _apply(sessions, schema, removal, actor)
    assert retained["revision"] == 2
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _apply(sessions, schema, replace(removal, idempotency_key=uuid4().hex), actor)
    assert await connection.fetch(f'SELECT * FROM "{schema}".manual_provider_location_binding') == bindings
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


async def test_legacy_duplicates_preserve_evidence_copy_bytes_and_one_binding(location_db, monkeypatch):
    from process.network_membership_copy import _encode

    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    command = _membership(network["record_id"], provider["record_id"], [sites[0]["record_id"]])
    first = command.fields["memberships_json"][0]
    rows = [first, {**first, "evidence_id": "second-observation"}, dict(first)]
    command = replace(command, fields={"memberships_json": rows})
    original = store.retain_manual_location_bindings
    copied_batches = []

    async def capture(driver, copy_bytes, row_count, relations_by_name):
        copied_batches.append((copy_bytes, row_count))
        return await original(driver, copy_bytes, row_count, relations_by_name)

    monkeypatch.setattr(store, "retain_manual_location_bindings", capture)
    retained = await _apply(sessions, schema, command, actor)
    assert retained["record"]["memberships_json"] == rows
    assert copied_batches == [_encode(json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode())]
    assert copied_batches[0][1] == 3
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_location_binding') == 1
    history = await connection.fetchval(
        f"SELECT record_json FROM \"{schema}\".registry_record_history WHERE record_kind='membership'"
    )
    assert json.loads(history)["memberships_json"] == rows


@pytest.mark.parametrize("variant", ["same_add", "add_remove", "late_explicit", "different_evidence"])
async def test_explicit_command_mode_refuses_duplicate_exact_unit_atomically(location_db, variant):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    command = _membership(network["record_id"], provider["record_id"], [sites[0]["record_id"]])
    first = command.fields["memberships_json"][0]
    if variant == "late_explicit":
        rows = [first, dict(first), {**first, "location_id": sites[1]["record_id"], "operation": "add"}]
    else:
        rows = [{**first, "operation": "add"}, {**first, "operation": "remove" if variant == "add_remove" else "add"}]
        if variant == "different_evidence":
            rows[1]["evidence_id"] = "different-evidence"
    before = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    with pytest.raises(ValueError, match="membership_rows_invalid"):
        await _apply(sessions, schema, replace(command, fields={"memberships_json": rows}), actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_location_binding') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == before


@pytest.mark.parametrize("operation", [None, "legacy", "unknown", True, {}])
async def test_malformed_membership_operation_never_persists(location_db, operation):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    command = _membership(network["record_id"], provider["record_id"], [sites[0]["record_id"]])
    command.fields["memberships_json"][0]["operation"] = operation
    with pytest.raises(ValueError, match="membership_rows_invalid"):
        await _apply(sessions, schema, command, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_location_binding') == 0


@pytest.mark.parametrize("operation", ["add", "remove"])
@pytest.mark.parametrize(
    "invalid_target", ["network", "provider", "location", "unknown_provider", "unknown_location", "wrong_network"]
)
async def test_both_operations_require_active_exact_targets(location_db, operation, invalid_target):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    records_by_kind = {"network": network, "provider": provider, "location": sites[0]}
    if invalid_target in records_by_kind:
        target = records_by_kind[invalid_target]
        identity = target["record_id"] if invalid_target == "network" else UUID(target["record_id"])
        await _apply(
            sessions,
            schema,
            RegistryRecordCommand(invalid_target, identity, "archive", 1, {}, "Retire target", uuid4().hex),
            actor,
        )
    command = _membership(network["record_id"], provider["record_id"], [sites[0]["record_id"]])
    row = command.fields["memberships_json"][0]
    row["operation"] = operation
    if invalid_target == "unknown_provider":
        row["provider_id"] = str(uuid4())
    elif invalid_target == "unknown_location":
        row["location_id"] = str(uuid4())
    elif invalid_target == "wrong_network":
        row["network_id"] += 1
    before = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    with pytest.raises(ValueError, match="membership_target_invalid"):
        await _apply(sessions, schema, command, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_location_binding') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == before


async def test_mixed_command_caller_rollback_preserves_bindings_draft_and_history(location_db):
    connection, schema, sessions, _ = location_db
    actor, network, provider, sites = await _pair_records(location_db)
    command = _membership(network["record_id"], provider["record_id"], [site["record_id"] for site in sites])
    command.fields["memberships_json"][1]["operation"] = "remove"
    before = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with sessions() as session, session.begin():
            await apply_registry_record_command(session, command, actor, schema=schema, source_schema=schema)
            raise RuntimeError("caller rollback")
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_location_binding') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == before
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == before
