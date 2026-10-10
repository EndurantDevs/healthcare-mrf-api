# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persisted draft CAS, replay and rollback on an explicitly isolated native DB."""

import asyncio
import importlib.util
import json
import os
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.registry_record_store import (
    RegistryActor,
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)


@pytest.fixture
async def record_db():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated test database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    schema = "registry_record_test_" + uuid4().hex
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://"))
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        for filename in [
            "20261007010000_managed_network_registry.py",
            "20261007020000_registry_revision_history.py",
            "20261007060000_registry_approved_selection.py",
            "20261007070000_registry_company_links.py",
            "20261007080000_manual_directory_registry.py",
            "20261007090000_network_membership_drafts.py",
            "20261007110000_registry_site_bindings.py",
            "20261007120000_company_network_assertions.py",
            "20261007130000_registry_network_bindings.py",
            "20261009020000_company_registry_assertions.py",
            "20261009030000_network_catalog_evidence.py",
        ]:
            path = Path(__file__).parents[1] / "alembic/versions" / filename
            spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
            migration = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration)
            for statement in migration._ddl(schema):
                await connection.execute(statement)
        yield connection, schema, async_sessionmaker(engine)
    finally:
        await engine.dispose()
        try:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        finally:
            await connection.close()


def _actor():
    return RegistryActor("platform_admin", uuid4(), "client_example")


def _create(kind="group", **changes):
    fields_by_name = {"display_name": " Example Unicode Réseau ", "aliases": [" Alias B ", "Alias A", "Alias A"]}
    if kind == "group":
        fields_by_name["group_kind"] = "corporate_parent"
    if kind == "company":
        fields_by_name["roles"] = ["network_operator", "employer", "employer"]
    command = RegistryRecordCommand(
        kind,
        None if kind == "network" else uuid4(),
        "create",
        0,
        fields_by_name,
        "Example creation",
        uuid4().hex,
        uuid4() if kind == "network" else None,
    )
    return replace(command, **changes)


async def _apply(sessions, schema, command, actor):
    async with sessions() as session, session.begin():
        return await apply_registry_record_command(session, command, actor, schema=schema)


@pytest.mark.asyncio
async def test_group_correction_replay_and_archive_restore(record_db):
    connection, schema, sessions = record_db
    command, actor = _create(), _actor()
    created = await _apply(sessions, schema, command, actor)
    assert created["record"]["display_name"] == "Example Unicode Réseau"
    assert created["record"]["aliases"] == ["Alias A", "Alias B"]
    corrected = replace(
        command,
        operation="correct",
        expected_revision=1,
        fields={**command.fields, "display_name": "Corrected Example"},
        idempotency_key=uuid4().hex,
    )
    correction = await _apply(sessions, schema, corrected, actor)
    assert correction["revision"] == correction["custom_revision"] == 2
    assert await _apply(sessions, schema, command, actor) == created
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _apply(sessions, schema, replace(corrected, idempotency_key=uuid4().hex), actor)
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, replace(command, reason="Different body"), actor)
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, command, replace(actor, impersonator_id=uuid4()))
    for operation, revision, archived in [("archive", 2, True), ("restore", 3, False)]:
        change = replace(
            command, operation=operation, expected_revision=revision, fields={}, idempotency_key=uuid4().hex
        )
        change_result = await _apply(sessions, schema, change, actor)
        assert change_result["record"]["group_id"] == str(command.record_id)
        assert change_result["record"]["archived"] is archived
        assert change_result["record"]["display_name"] == "Corrected Example"
        assert change_result["record"]["aliases"] == ["Alias A", "Alias B"]
    async with sessions() as session:
        assert (await get_registry_record(session, "group", command.record_id, schema=schema))["revision"] == 4
        assert len(await list_registry_records(session, "group", limit=100, schema=schema)) == 1
        with pytest.raises(ValueError, match="page_invalid"):
            await list_registry_records(session, "group", limit=101, schema=schema)
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (4, 0)
    history = await connection.fetch(
        f'SELECT record_json,actor_json FROM "{schema}".registry_record_history ORDER BY revision'
    )
    assert len(history) == 4
    assert "Corrected Example" not in history[0]["record_json"]
    assert str(actor.user_id) in history[0]["actor_json"]


@pytest.mark.asyncio
async def test_network_allocation_retry_and_archive_preserve_source_aliases(record_db):
    connection, schema, sessions = record_db
    command, actor = _create("network"), _actor()
    created = await _apply(sessions, schema, command, actor)
    network_id = created["record_id"]
    assert type(network_id) is int and 0 < network_id <= 2147483647
    sequence_value = await connection.fetchval(
        f'SELECT last_value FROM "{schema}".network_registry_identity_network_id_seq'
    )
    assert await _apply(sessions, schema, command, actor) == created
    assert (
        await connection.fetchval(f'SELECT last_value FROM "{schema}".network_registry_identity_network_id_seq')
        == sequence_value
    )
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, replace(command, reason="Different create"), actor)
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_alias VALUES ($1,$2,$3,$4,$5,$6,$7,now())',
        "example-source",
        "source-one",
        "legacy_uuid",
        str(uuid4()),
        "medical/example",
        network_id,
        "example-evidence",
    )
    archived = replace(
        command,
        record_id=network_id,
        allocation_key=None,
        operation="archive",
        expected_revision=1,
        fields={},
        idempotency_key=uuid4().hex,
    )
    await _apply(sessions, schema, archived, actor)
    assert await _apply(sessions, schema, command, actor) == created
    restored = replace(archived, operation="restore", expected_revision=2, idempotency_key=uuid4().hex)
    assert (await _apply(sessions, schema, restored, actor))["record_id"] == network_id
    other = await _apply(sessions, schema, _create("network"), actor)
    assert other["record_id"] != network_id
    assert (
        await connection.fetchval(
            f'SELECT count(*) FROM "{schema}".network_registry_alias WHERE network_id=$1', network_id
        )
        == 1
    )
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


@pytest.mark.asyncio
async def test_concurrent_creation_and_correction_have_one_committed_revision(record_db):
    connection, schema, sessions = record_db
    command, actor = _create("network"), _actor()
    results = await asyncio.gather(*[_apply(sessions, schema, command, actor) for _ in range(3)])
    assert results[0] == results[1] == results[2]
    correction = replace(
        command,
        record_id=results[0]["record_id"],
        allocation_key=None,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
    )
    competing = replace(
        correction, idempotency_key=uuid4().hex, fields={**correction.fields, "display_name": "Other correction"}
    )
    outcomes = await asyncio.gather(
        _apply(sessions, schema, correction, actor), _apply(sessions, schema, competing, actor), return_exceptions=True
    )
    assert sum(isinstance(outcome, RegistryRecordConflict) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 2
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (2, 0)


@pytest.mark.asyncio
async def test_caller_rollback_and_caught_native_failure_leave_heads_and_history_atomic(record_db):
    connection, schema, sessions = record_db
    command, actor = _create("network"), _actor()
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with sessions() as session, session.begin():
            await apply_registry_record_command(session, command, actor, schema=schema)
            raise RuntimeError("caller rollback")
    for table in ["network_registry_identity", "network_registry_record", "registry_record_history"]:
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".{table}') == 0
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (0, 0)
    created = await _apply(sessions, schema, command, actor)
    await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=9223372036854775807')
    correction = replace(
        command,
        record_id=created["record_id"],
        allocation_key=None,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
    )
    async with sessions() as session, session.begin():
        with pytest.raises(DBAPIError):
            await apply_registry_record_command(session, correction, actor, schema=schema)
        assert await session.scalar(text("SELECT 1")) == 1
    assert await connection.fetchval(f'SELECT revision FROM "{schema}".network_registry_record') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 1
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


@pytest.mark.asyncio
async def test_distinct_group_kinds_and_company_roles_are_not_name_identity(record_db):
    _, schema, sessions = record_db
    actor = _actor()
    corporate = _create()
    regulatory = _create(fields={**corporate.fields, "group_kind": "naic_group"})
    first = await _apply(sessions, schema, corporate, actor)
    second = await _apply(sessions, schema, regulatory, actor)
    assert first["record_id"] != second["record_id"]
    assert first["record"]["display_name"] == second["record"]["display_name"]
    company = await _apply(sessions, schema, _create("company"), actor)
    assert company["record"]["roles"] == ["employer", "network_operator"]


@pytest.mark.asyncio
async def test_invalid_commands_fail_before_persisting_any_head(record_db):
    connection, schema, sessions = record_db
    actor = _actor()
    command = _create()
    invalid_commands = [
        replace(command, fields={"display_name": "Incomplete"}),
        replace(command, fields={**command.fields, "aliases": [" "]}),
        replace(command, fields={**command.fields, "aliases": ["alias"] * 101}),
        replace(command, reason=" "),
        replace(command, expected_revision=True),
        replace(command, record_id=UUID(int=0)),
        replace(command, fields={**command.fields, "group_kind": []}),
        replace(_create("network"), record_id=True, allocation_key=None, operation="correct", expected_revision=1),
        replace(_create("company"), fields={"display_name": "Example", "roles": [{}], "aliases": []}),
    ]
    async with sessions() as session, session.begin():
        for candidate in invalid_commands:
            with pytest.raises(ValueError):
                await apply_registry_record_command(session, candidate, actor, schema=schema)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 0


def _network_evidence(network_id, revision, **changes):
    return {
        "network_id": network_id,
        "expected_record_revision": revision,
        "pricing_refs": None,
        "benefit_refs": [],
        **changes,
    }


def _require_evidence_native():
    import inspect

    from process.registry_record_store import _fast_module

    native = _fast_module()
    assert native is not None, "The source-bound native evidence parser is required"
    assert inspect.isbuiltin(getattr(native, "parse_registry_network_evidence", None))
    return native


@pytest.mark.asyncio
async def test_network_evidence_native_syntax_refuses_unknown_and_typed_malformed(record_db):
    import json

    native = _require_evidence_native()
    evidence = _network_evidence(7, 1)
    assert json.loads(native.parse_registry_network_evidence(json.dumps(evidence).encode())) == evidence
    invalid_documents = [
        {**evidence, "unknown": None},
        {**evidence, "network_id": True},
        {**evidence, "expected_record_revision": 0},
        {**evidence, "pricing_refs": [{}]},
        {**evidence, "benefit_refs": [None]},
    ]
    for document in invalid_documents:
        with pytest.raises(ValueError, match="registry_network_evidence_invalid"):
            native.parse_registry_network_evidence(json.dumps(document).encode())
    duplicate = b'{"network_id":7,"network_id":7,"expected_record_revision":1,"pricing_refs":null,"benefit_refs":[]}'
    with pytest.raises(ValueError, match="registry_network_evidence_invalid"):
        native.parse_registry_network_evidence(duplicate)


@pytest.mark.asyncio
async def test_network_evidence_create_refusal_precedes_identity_allocation(record_db):
    connection, schema, sessions = record_db
    command = _create("network")
    before = await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_identity')
    for evidence in [None, _network_evidence(7, 1)]:
        invalid = replace(command, fields={**command.fields, "catalog_evidence_json": evidence})
        with pytest.raises(ValueError, match="create_evidence_forbidden"):
            await _apply(sessions, schema, invalid, _actor())
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_identity') == before
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 0


@pytest.mark.asyncio
async def test_network_evidence_retention_replay_and_archive(record_db):
    import json

    _require_evidence_native()
    connection, schema, sessions = record_db
    create, actor = _create("network"), _actor()
    first = await _apply(sessions, schema, create, actor)
    assert first["record"]["catalog_evidence_json"] is None
    evidence = _network_evidence(first["record_id"], 1)
    correction = replace(
        create,
        record_id=first["record_id"],
        allocation_key=None,
        operation="correct",
        expected_revision=1,
        fields={**create.fields, "catalog_evidence_json": evidence},
        idempotency_key=uuid4().hex,
    )
    retained = await _apply(sessions, schema, correction, actor)
    omitted = replace(
        correction,
        expected_revision=2,
        fields={**create.fields, "display_name": "Later name"},
        idempotency_key=uuid4().hex,
    )
    later = await _apply(sessions, schema, omitted, actor)
    assert later["record"]["catalog_evidence_json"] == evidence
    archive = replace(omitted, operation="archive", expected_revision=3, fields={}, idempotency_key=uuid4().hex)
    archived = await _apply(sessions, schema, archive, actor)
    restored = await _apply(
        sessions, schema, replace(archive, operation="restore", expected_revision=4, idempotency_key=uuid4().hex), actor
    )
    assert archived["record"]["catalog_evidence_json"] == restored["record"]["catalog_evidence_json"] == evidence
    clear = replace(
        omitted,
        expected_revision=5,
        fields={**omitted.fields, "catalog_evidence_json": None},
        idempotency_key=uuid4().hex,
    )
    cleared = await _apply(sessions, schema, clear, actor)
    assert cleared["record"]["catalog_evidence_json"] is None
    assert (await _apply(sessions, schema, correction, actor))["record"] == retained["record"]
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(
            sessions,
            schema,
            replace(
                correction, fields={**correction.fields, "catalog_evidence_json": {**evidence, "pricing_refs": []}}
            ),
            actor,
        )


@pytest.mark.asyncio
async def test_network_evidence_caller_rollback_preserves_history(record_db):
    import json

    _require_evidence_native()
    connection, schema, sessions = record_db
    create, actor = _create("network"), _actor()
    first = await _apply(sessions, schema, create, actor)
    evidence = _network_evidence(first["record_id"], 1)
    correction = replace(
        create,
        record_id=first["record_id"],
        allocation_key=None,
        operation="correct",
        expected_revision=1,
        fields={**create.fields, "catalog_evidence_json": evidence},
        idempotency_key=uuid4().hex,
    )
    await _apply(sessions, schema, correction, actor)
    before_history = await connection.fetchval(
        f'SELECT jsonb_agg(record_json ORDER BY revision)::text FROM "{schema}".registry_record_history'
    )
    async with sessions() as session:
        transaction = await session.begin()
        rollback = replace(
            correction,
            expected_revision=2,
            fields={**correction.fields, "catalog_evidence_json": _network_evidence(first["record_id"], 2)},
            idempotency_key=uuid4().hex,
        )
        await apply_registry_record_command(session, rollback, actor, schema=schema)
        await transaction.rollback()
    assert (
        await connection.fetchval(
            f'SELECT jsonb_agg(record_json ORDER BY revision)::text FROM "{schema}".registry_record_history'
        )
        == before_history
    )
    assert (
        json.loads(
            await connection.fetchval(
                f'SELECT record_json::text FROM "{schema}".registry_record_history WHERE revision=2'
            )
        )["catalog_evidence_json"]
        == evidence
    )


@pytest.mark.asyncio
async def test_network_evidence_legacy_approval_and_old_approved_bytes_remain(record_db):
    import json

    from process.registry_approval_store import approve_registry_records
    from tests.test_registry_approval_store_postgres import _command as approval_command

    _require_evidence_native()
    connection, schema, sessions = record_db
    create, actor = _create("network"), _actor()
    first = await _apply(sessions, schema, create, actor)
    # Prepare an authentic old-version JSON shape before the protected approval.
    await connection.execute(
        f"UPDATE \"{schema}\".registry_record_history SET record_json=record_json-'catalog_evidence_json' WHERE record_kind='network'"
    )
    legacy_bytes = await connection.fetchval(f'SELECT record_json::text FROM "{schema}".registry_record_history')
    command = await approval_command(connection, schema, first)
    async with connection.transaction():
        approved = await approve_registry_records(connection, command, actor, control_schema=schema)
    assert (
        await connection.fetchval(f'SELECT record_json::text FROM "{schema}".registry_record_history') == legacy_bytes
    )
    old_approved = await connection.fetchval(
        f'SELECT record_json::text FROM "{schema}".registry_approved_record WHERE approved_revision=$1',
        approved["approved_revision"],
    )
    correction = replace(
        create,
        record_id=first["record_id"],
        operation="correct",
        allocation_key=None,
        expected_revision=1,
        fields={**create.fields, "catalog_evidence_json": _network_evidence(first["record_id"], 1)},
        idempotency_key=uuid4().hex,
    )
    second = await _apply(sessions, schema, correction, actor)
    command = await approval_command(connection, schema, second)
    async with connection.transaction():
        current = await approve_registry_records(connection, command, actor, control_schema=schema)
    assert (
        await connection.fetchval(
            f'SELECT record_json::text FROM "{schema}".registry_approved_record WHERE approved_revision=$1',
            approved["approved_revision"],
        )
        == old_approved
    )
    retained = json.loads(
        await connection.fetchval(
            f'SELECT record_json::text FROM "{schema}".registry_approved_record WHERE approved_revision=$1',
            current["approved_revision"],
        )
    )
    assert retained["catalog_evidence_json"] == second["record"]["catalog_evidence_json"]


@pytest.mark.asyncio
async def test_network_evidence_historical_undo_rebinds_without_history_edits(record_db):
    from process.registry_manual_undo import RegistryManualUndoCommand, prepare_registry_manual_undo

    _require_evidence_native()
    connection, schema, sessions = record_db
    create, actor = _create("network"), _actor()
    first = await _apply(sessions, schema, create, actor)
    correction = replace(
        create,
        record_id=first["record_id"],
        allocation_key=None,
        operation="correct",
        expected_revision=1,
        fields={**create.fields, "catalog_evidence_json": _network_evidence(first["record_id"], 1)},
        idempotency_key=uuid4().hex,
    )
    await _apply(sessions, schema, correction, actor)
    await _apply(
        sessions,
        schema,
        replace(
            correction,
            expected_revision=2,
            fields={**create.fields, "catalog_evidence_json": None},
            idempotency_key=uuid4().hex,
        ),
        actor,
    )
    historical = await connection.fetchval(
        f'SELECT record_json::text FROM "{schema}".registry_record_history WHERE revision=2'
    )
    undo = RegistryManualUndoCommand("network", first["record_id"], 3, 2, "Reviewed historical evidence", uuid4().hex)
    async with sessions() as session, session.begin():
        preparation = await prepare_registry_manual_undo(session, undo, actor, schema=schema)
        receipt = await apply_registry_record_command(session, preparation.command, actor, schema=schema)
    assert receipt["record"]["catalog_evidence_json"] == _network_evidence(first["record_id"], 3)
    assert (
        await connection.fetchval(f'SELECT record_json::text FROM "{schema}".registry_record_history WHERE revision=2')
        == historical
    )
    legacy_undo = replace(undo, expected_revision=4, target_revision=1, idempotency_key=uuid4().hex)
    async with sessions() as session, session.begin():
        preparation = await prepare_registry_manual_undo(session, legacy_undo, actor, schema=schema)
        receipt = await apply_registry_record_command(session, preparation.command, actor, schema=schema)
    assert receipt["record"]["catalog_evidence_json"] is None


def _near_limit_network_evidence(network_id):
    import json

    pricing_refs = [
        {
            "healthporta_plan_id": "hpplan_" + "0" * 26,
            "plan_release_id": "hprelease_" + "A" * 26,
            "serving_revision_id": "hpserve_" + "Z" * 26,
            "role": "in_network",
            "ordinal": ordinal,
            "snapshot_id": "s" * 96,
        }
        for ordinal in range(16)
    ]
    benefit_refs = [
        {
            "healthporta_plan_id": "hpplan_" + "0" * 26,
            "plan_version_id": "hpversion_" + "A" * 26,
            "observation_id": "hpobs_" + f"{ordinal:032d}",
            "alias_binding_id": "hpbinding_" + "b" * 32,
            "provenance_id": "hpprov_" + "C" * 32,
            "semantic_digest": "a" * 64,
            "observation_digest": "0" * 64,
            "document_sha256": "f" * 64,
        }
        for ordinal in range(16)
    ]
    evidence = _network_evidence(network_id, 1, pricing_refs=pricing_refs, benefit_refs=benefit_refs)
    encode = lambda: json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    for reference in pricing_refs:
        for position in range(1, 95):
            snapshot_id = reference["snapshot_id"]
            reference["snapshot_id"] = snapshot_id[:position] + "\x01" + snapshot_id[position + 1 :]
            excess = len(encode()) - 16384
            if excess >= 0:
                if excess:
                    reference["snapshot_id"] = reference["snapshot_id"][:-excess]
                assert len(encode()) == 16384
                return evidence
    raise AssertionError("The valid reference generator did not reach the parser boundary")


@pytest.mark.asyncio
async def test_network_evidence_native_and_jsonb_storage_boundaries(record_db):
    import json

    _require_evidence_native()
    connection, schema, sessions = record_db
    create, actor = _create("network"), _actor()
    first = await _apply(sessions, schema, create, actor)
    evidence = _near_limit_network_evidence(first["record_id"])
    correction = replace(
        create,
        record_id=first["record_id"],
        allocation_key=None,
        operation="correct",
        expected_revision=1,
        fields={**create.fields, "catalog_evidence_json": evidence},
        idempotency_key=uuid4().hex,
    )
    retained = await _apply(sessions, schema, correction, actor)
    assert retained["record"]["catalog_evidence_json"] == evidence
    stored_bytes = await connection.fetchval(
        f'SELECT octet_length(catalog_evidence_json::text) FROM "{schema}".network_registry_record'
    )
    assert 16384 < stored_bytes <= 32768
    for invalid_json in ["null", "[]", json.dumps({"oversized": "x" * 32768})]:
        with pytest.raises(asyncpg.CheckViolationError):
            async with connection.transaction():
                await connection.execute(
                    f'UPDATE "{schema}".network_registry_record SET catalog_evidence_json=$1::jsonb', invalid_json
                )
    assert (
        await connection.fetchval(
            f'SELECT octet_length(catalog_evidence_json::text) FROM "{schema}".network_registry_record'
        )
        == stored_bytes
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["group", "company", "network"])
async def test_legacy_name_correction(record_db, kind):
    connection, schema, sessions = record_db
    command, actor = _create(kind), _actor()
    created = await _apply(sessions, schema, command, actor)
    table, identity = {
        "group": ("company_group_registry", "group_id"),
        "company": ("company_registry", "company_id"),
        "network": ("network_registry_record", "network_id"),
    }[kind]
    record_id = created["record_id"] if kind == "network" else UUID(created["record_id"])
    await connection.execute(
        f'UPDATE "{schema}"."{table}" SET display_name=$1,aliases=$2 WHERE "{identity}"=$3',
        "Legacy\nname",
        json.dumps(["Legacy\u0085alias"]),
        record_id,
    )
    async with sessions() as session:
        legacy = await get_registry_record(session, kind, record_id, schema=schema)
    assert legacy["display_name"] == "Legacy\nname"
    assert legacy["aliases"] == ["Legacy\u0085alias"]
    correction = replace(
        command,
        record_id=record_id,
        allocation_key=None,
        operation="correct",
        expected_revision=1,
        fields={**command.fields, "display_name": "Corrected Réseau", "aliases": ["😀" * 512]},
        idempotency_key=uuid4().hex,
    )
    corrected = await _apply(sessions, schema, correction, actor)
    assert corrected["revision"] == 2
    assert corrected["record"]["display_name"] == "Corrected Réseau"
    assert corrected["record"]["aliases"] == ["😀" * 512]
