# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical undo preparation and actual draft correction on isolated native SQL."""

import json
from dataclasses import FrozenInstanceError, replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.models.company_group_registry import CompanyGroupRegistry
from process.network_serving_read import NetworkServingReadUnavailable
from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_manual_undo import (
    MAX_UNDO_SNAPSHOT_BYTES,
    RegistryManualUndoCommand,
    prepare_registry_manual_undo,
)
from process.registry_record_store import RegistryRecordConflict, apply_registry_record_command
from process.registry_retained_site_adoption import RetainedSiteAdoptionError
from tests.test_manual_location_identity_store_postgres import _create as _location
from tests.test_manual_provider_identity_store_postgres import _create as _provider
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_membership_draft_store_postgres import (
    _command as _membership_command,
)
from tests.test_network_membership_draft_store_postgres import (
    _seed as _membership_seed,
)
from tests.test_network_membership_draft_store_postgres import (
    membership_db,
)
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _command as _approval_command
from tests.test_registry_company_links_postgres import _assertion, _links
from tests.test_registry_record_store_postgres import _actor, _create, record_db
from tests.test_registry_retained_site_adoption_postgres import retained_db

pytestmark = pytest.mark.asyncio


async def _apply(sessions, schema, command, actor, **kwargs):
    async with sessions() as session, session.begin():
        return await apply_registry_record_command(session, command, actor, schema=schema, **kwargs)


async def _prepare(sessions, schema, command, actor):
    async with sessions() as session, session.begin():
        return await prepare_registry_manual_undo(session, command, actor, schema=schema)


async def _seed(record_db, kind="group"):
    _, schema, sessions = record_db
    command = _provider() if kind == "provider" else _location() if kind == "location" else _create(kind)
    actor = _actor()
    first = await _apply(sessions, schema, command, actor)
    corrected = replace(
        command,
        record_id=first["record_id"] if kind == "network" else command.record_id,
        allocation_key=None,
        operation="correct",
        expected_revision=1,
        fields={**command.fields, "display_name": "Later content"},
        idempotency_key=uuid4().hex,
    )
    await _apply(sessions, schema, corrected, actor)
    undo = RegistryManualUndoCommand(kind, corrected.record_id, 2, 1, "Reviewed historical correction", uuid4().hex)
    return actor, first, corrected, undo


@pytest.mark.parametrize("kind", ["group", "company", "network", "provider", "location"])
async def test_historical_content_creates_new_unapproved_draft_with_durable_provenance(record_db, kind):
    connection, schema, sessions = record_db
    actor, first, _, undo = await _seed(record_db, kind)
    before = await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history')
    preparation = await _prepare(sessions, schema, undo, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == before
    assert preparation.command.operation == "correct" and preparation.command.expected_revision == 2
    assert preparation.provenance.target_custom_revision == 1
    assert preparation.provenance.target_revision == 1 and preparation.provenance.current_revision == 2
    assert preparation.provenance.target_archived is False
    assert not {"created_at", "archived", "revision", "canonical_address_json"} & preparation.command.fields.keys()
    with pytest.raises(FrozenInstanceError):
        preparation.provenance.target_revision = 2
    result = await _apply(sessions, schema, preparation.command, actor)
    assert result["revision"] == result["custom_revision"] == 3
    assert result["record"]["display_name"] == first["record"]["display_name"]
    assert result["record"]["created_at"] == first["record"]["created_at"]
    row = await connection.fetchrow(
        f'SELECT reason,request_sha256 FROM "{schema}".registry_record_history WHERE revision=3'
    )
    target_sha = await connection.fetchval(
        f'SELECT request_sha256 FROM "{schema}".registry_record_history WHERE revision=1'
    )
    assert row["reason"] == f"Undo revision 1 [{target_sha}]: {undo.reason}"
    assert row["request_sha256"] != target_sha
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (3, 0)


async def test_retry_after_later_edit_replays_and_changed_target_reason_actor_conflict(record_db):
    connection, schema, sessions = record_db
    actor, _, corrected, undo = await _seed(record_db)
    prepared = await _prepare(sessions, schema, undo, actor)
    result = await _apply(sessions, schema, prepared.command, actor)
    later = replace(corrected, expected_revision=3, idempotency_key=uuid4().hex)
    await _apply(sessions, schema, later, actor)
    retry = await _prepare(sessions, schema, undo, actor)
    assert retry.command == prepared.command and retry.provenance.current_revision == 4
    assert await _apply(sessions, schema, retry.command, actor) == result
    for change in [replace(undo, reason="Changed reason"), replace(undo, expected_revision=3, target_revision=2)]:
        changed = await _prepare(sessions, schema, change, actor)
        with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
            await _apply(sessions, schema, changed.command, actor)
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, retry.command, replace(actor, user_id=uuid4()))
    stale = await _prepare(sessions, schema, replace(undo, idempotency_key=uuid4().hex), actor)
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _apply(sessions, schema, stale.command, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 4


async def test_same_content_at_different_targets_still_has_distinct_audit_request(record_db):
    _, schema, sessions = record_db
    actor, _, corrected, undo = await _seed(record_db)
    identical = replace(corrected, expected_revision=2, idempotency_key=uuid4().hex)
    await _apply(sessions, schema, identical, actor)
    await _apply(sessions, schema, replace(identical, expected_revision=3, idempotency_key=uuid4().hex), actor)
    first = await _prepare(sessions, schema, replace(undo, expected_revision=4, target_revision=2), actor)
    result = await _apply(sessions, schema, first.command, actor)
    different = await _prepare(sessions, schema, replace(undo, expected_revision=4, target_revision=3), actor)
    assert first.command.fields == different.command.fields
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, different.command, actor)
    assert result["revision"] == 5


async def test_archive_state_is_separate_and_preparation_does_not_restore(record_db):
    _, schema, sessions = record_db
    actor, _, corrected, undo = await _seed(record_db)
    archived = replace(corrected, operation="archive", expected_revision=2, fields={}, idempotency_key=uuid4().hex)
    await _apply(sessions, schema, archived, actor)
    prepared = await _prepare(sessions, schema, replace(undo, expected_revision=3), actor)
    assert prepared.provenance.target_archived is False
    result = await _apply(sessions, schema, prepared.command, actor)
    assert result["record"]["archived"] is True


@pytest.mark.parametrize(
    "changes",
    [
        {"target_revision": 0},
        {"target_revision": True},
        {"target_revision": 2},
        {"target_revision": 3},
        {"target_revision": 1.0},
        {"target_revision": "1"},
        {"expected_revision": True},
        {"expected_revision": 1},
        {"expected_revision": 9223372036854775807},
        {"expected_revision": "2"},
        {"record_id": UUID(int=0)},
        {"record_id": "not-an-id"},
        {"record_kind": "network_binding"},
        {"record_kind": "unknown"},
        {"record_kind": False},
        {"reason": " "},
        {"reason": "x" * 1001},
        {"reason": "bad\0reason"},
        {"idempotency_key": " bad"},
        {"idempotency_key": "x" * 129},
    ],
)
async def test_invalid_boundary_does_not_issue_sql(record_db, changes):
    _, schema, sessions = record_db
    command = RegistryManualUndoCommand("group", uuid4(), 2, 1, "Reviewed correction", uuid4().hex)
    async with sessions() as session, session.begin():
        calls = []

        def count(*args):
            calls.append(args[2])

        event.listen(session.bind.sync_engine, "before_cursor_execute", count)
        try:
            with pytest.raises(ValueError):
                await prepare_registry_manual_undo(session, replace(command, **changes), _actor(), schema=schema)
        finally:
            event.remove(session.bind.sync_engine, "before_cursor_execute", count)
        assert calls == []


async def test_invalid_actor_caller_transaction_missing_history_and_foreign_identity(record_db):
    _, schema, sessions = record_db
    actor, _, _, undo = await _seed(record_db)
    async with sessions() as session:
        with pytest.raises(ValueError, match="caller_transaction"):
            await prepare_registry_manual_undo(session, undo, actor, schema=schema)
    for changes in [{"kind": "client_user"}, {"user_id": UUID(int=0)}, {"impersonator_id": UUID(int=0)}]:
        with pytest.raises(ValueError):
            await _prepare(sessions, schema, undo, replace(actor, **changes))
    for changes in [{"record_id": uuid4()}, {"expected_revision": 3}, {"record_kind": "company"}]:
        with pytest.raises(RegistryRecordConflict, match="history_conflict"):
            await _prepare(sessions, schema, replace(undo, **changes), actor)


async def test_pending_orm_write_rejected_without_flush(record_db):
    _, schema, sessions = record_db
    actor, _, _, undo = await _seed(record_db)
    async with sessions() as session, session.begin():
        pending = CompanyGroupRegistry(group_id=uuid4(), group_kind="corporate_parent", display_name="Pending group")
        session.add(pending)
        with pytest.raises(ValueError, match="clean_caller_transaction"):
            await prepare_registry_manual_undo(session, undo, actor, schema=schema)
        assert pending in session.new
        session.expunge(pending)


async def test_undo_draft_requires_separate_current_revision_preview(serving_schema):
    connection, schema, engine = serving_schema
    sessions = async_sessionmaker(engine)
    actor, original, _, undo = await _seed((connection, schema, sessions))
    prepared = await _prepare(sessions, schema, undo, actor)
    correction = await _apply(sessions, schema, prepared.command, actor)
    old_selection = await _approval_command(connection, schema, original)
    async with connection.transaction():
        with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
            await preview_registry_approval(connection, old_selection, actor, control_schema=schema)
    new_selection = await _approval_command(connection, schema, correction)
    async with connection.transaction():
        preview = await preview_registry_approval(connection, new_selection, actor, control_schema=schema)
    assert preview["selected_count"] == 1 and preview["records"][0]["record_revision"] == 3
    assert preview["records"][0]["after"]["display_name"] == original["record"]["display_name"]
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


async def test_reason_exact_bound_and_overflow_is_rejected_without_truncation(record_db):
    _, schema, sessions = record_db
    actor, _, _, undo = await _seed(record_db)
    prepared = await _prepare(sessions, schema, undo, actor)
    prefix_size = len(prepared.command.reason) - len(undo.reason)
    exact = await _prepare(sessions, schema, replace(undo, reason="x" * (1000 - prefix_size)), actor)
    assert len(exact.command.reason) == 1000
    with pytest.raises(ValueError, match="undo_reason_invalid"):
        await _prepare(sessions, schema, replace(undo, reason="x" * (1001 - prefix_size)), actor)


@pytest.mark.parametrize("damage", ["identity", "revision", "archived", "fields", "bound", "expected_identity"])
async def test_malformed_historical_snapshot_fails_with_static_error(record_db, damage):
    connection, schema, sessions = record_db
    actor, first, _, undo = await _seed(record_db)
    snapshot = first["record"].copy()
    damage_by_name = {
        "identity": {"group_id": str(uuid4())},
        "revision": {"revision": True},
        "archived": {"archived": "false"},
        "bound": {"ignored": "x" * MAX_UNDO_SNAPSHOT_BYTES},
        "expected_identity": {"revision": 2, "group_id": str(uuid4())},
    }
    if damage == "fields":
        del snapshot["aliases"]
    else:
        snapshot.update(damage_by_name[damage])
    await connection.execute(
        f'UPDATE "{schema}".registry_record_history SET record_json=$1::jsonb WHERE revision=$2',
        json.dumps(snapshot),
        2 if damage == "expected_identity" else 1,
    )
    with pytest.raises(ValueError, match="^registry_manual_undo_history_(invalid|conflict)$"):
        await _prepare(sessions, schema, undo, actor)


async def test_preparation_uses_one_query(record_db):
    _, schema, sessions = record_db
    actor, _, _, undo = await _seed(record_db)
    async with sessions() as session, session.begin():
        counts = []
        engine = session.bind.sync_engine

        def count(*args):
            counts.append(args[2])

        event.listen(engine, "before_cursor_execute", count)
        try:
            await prepare_registry_manual_undo(session, undo, actor, schema=schema)
        finally:
            event.remove(engine, "before_cursor_execute", count)
        assert len(counts) == 1 and counts[0].lstrip().startswith("SELECT")


@pytest.mark.parametrize("legacy", [False, True])
async def test_company_relationship_history_restores_explicit_assertions(record_db, legacy):
    connection, schema, sessions = record_db
    actor = _actor()
    company = await _apply(sessions, schema, _create("company"), actor)
    network = await _apply(sessions, schema, _create("network"), actor)
    command = _links(company, [network])
    if not legacy:
        command = replace(command, fields={**command.fields, "network_assertions": [_assertion(company, network)]})
    original = await _apply(sessions, schema, command, actor)
    if legacy:
        await connection.execute(
            f"UPDATE \"{schema}\".registry_record_history SET record_json=record_json-'network_assertions' "
            "WHERE record_kind='company_links'"
        )
    clear = replace(
        command,
        operation="correct",
        expected_revision=1,
        fields={"network_ids": [], "group_id": None, "network_assertions": []},
        idempotency_key=uuid4().hex,
    )
    await _apply(sessions, schema, clear, actor)
    undo = RegistryManualUndoCommand("company_links", command.record_id, 2, 1, "Reviewed relationships", uuid4().hex)
    prepared = await _prepare(sessions, schema, undo, actor)
    assert prepared.command.fields["network_assertions"] == original["record"]["network_assertions"]
    correction = await _apply(sessions, schema, prepared.command, actor)
    assert correction["record"]["network_ids"] == original["record"]["network_ids"]
    assert correction["record"]["network_assertions"] == original["record"]["network_assertions"]
    assert correction["revision"] == 3


@pytest.mark.parametrize("invalidate_site", [False, True])
async def test_membership_history_revalidates_actual_office_targets(membership_db, invalidate_site):
    connection, schema, sessions, source_schema, _ = membership_db
    actor, network_id, provider_id, sites = await _membership_seed(membership_db)
    original = _membership_command(
        network_id,
        [
            {
                "network_id": network_id,
                "provider_system": "manual",
                "provider_id": provider_id,
                "location_id": sites[0],
                "evidence_id": "explicit-review",
            }
        ],
    )
    first = await _apply(sessions, schema, original, actor, source_schema=source_schema)
    await _apply(
        sessions,
        schema,
        replace(
            original,
            operation="correct",
            expected_revision=1,
            fields={"memberships_json": []},
            idempotency_key=uuid4().hex,
        ),
        actor,
        source_schema=source_schema,
    )
    undo = RegistryManualUndoCommand("membership", network_id, 2, 1, "Reviewed earlier office", uuid4().hex)
    prepared = await _prepare(sessions, schema, undo, actor)
    assert prepared.command.fields == original.fields
    if invalidate_site:
        await connection.execute(
            f'UPDATE "{schema}".manual_location_registry SET archived=true WHERE location_id=$1', UUID(sites[0])
        )
        with pytest.raises(ValueError, match="membership_target_invalid"):
            await _apply(sessions, schema, prepared.command, actor, source_schema=source_schema)
        assert await connection.fetchval(f'SELECT revision FROM "{schema}".network_membership_draft') == 2
    else:
        correction = await _apply(sessions, schema, prepared.command, actor, source_schema=source_schema)
        assert correction["record"]["memberships_json"] == first["record"]["memberships_json"]
        assert correction["revision"] == 3


async def test_numeric_identity_rejects_boolean_history(record_db):
    connection, schema, sessions = record_db
    actor, first, _, undo = await _seed(record_db, "network")
    assert first["record_id"] == 1
    for revision, damage in [(1, True), (2, "1")]:
        await connection.execute(
            f'UPDATE "{schema}".registry_record_history SET record_json='
            "jsonb_set(record_json,'{network_id}',$1::jsonb) WHERE revision=$2",
            json.dumps(damage),
            revision,
        )
        with pytest.raises(ValueError, match="^registry_manual_undo_history_(invalid|conflict)$"):
            await _prepare(sessions, schema, undo, actor)


async def test_snapshot_exact_byte_bound_and_overflow(record_db):
    connection, schema, sessions = record_db
    actor, _, _, undo = await _seed(record_db)
    await connection.execute(
        f'UPDATE "{schema}".registry_record_history SET record_json='
        "record_json||jsonb_build_object('ignored','') WHERE revision=1"
    )
    baseline = await connection.fetchval(
        f'SELECT octet_length(record_json::text) FROM "{schema}".registry_record_history WHERE revision=1'
    )
    for extra in [0, 1]:
        await connection.execute(
            f'UPDATE "{schema}".registry_record_history SET record_json='
            "jsonb_set(record_json,'{ignored}',to_jsonb(repeat('x',$1::integer))) WHERE revision=1",
            MAX_UNDO_SNAPSHOT_BYTES - baseline + extra,
        )
        if extra:
            with pytest.raises(ValueError, match="undo_history_invalid"):
                await _prepare(sessions, schema, undo, actor)
        else:
            prepared = await _prepare(sessions, schema, undo, actor)
            assert "ignored" not in prepared.command.fields


async def test_retained_site_revalidation_and_replay(retained_db):
    fixture = retained_db
    sessions = async_sessionmaker(fixture.engine)
    actor = _actor()
    from process.registry_record_store import RegistryRecordCommand

    fields_by_name = {"source_generation": fixture.source.generation_id, **fixture.rows[0]}
    create = RegistryRecordCommand("site_binding", uuid4(), "create", 0, fields_by_name, "Reviewed office", uuid4().hex)

    async def apply(command):
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            return await apply_registry_record_command(session, command, actor, schema=fixture.control_schema)

    await apply(create)
    later_fields_by_name = {"source_generation": fixture.source.generation_id, **fixture.rows[1]}
    await apply(
        replace(
            create, operation="correct", expected_revision=1, fields=later_fields_by_name, idempotency_key=uuid4().hex
        )
    )
    undo = RegistryManualUndoCommand("site_binding", create.record_id, 2, 1, "Reviewed earlier office", uuid4().hex)
    prepared = await _prepare(sessions, fixture.control_schema, undo, actor)
    assert set(prepared.command.fields) == set(fields_by_name)
    correction_receipt = await apply(prepared.command)
    assert correction_receipt["record"]["location_id"] == fields_by_name["location_id"]
    await fixture.connection.execute(f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false')
    retry = await _prepare(sessions, fixture.control_schema, undo, actor)
    assert await apply(retry.command) == correction_receipt
    fresh = await _prepare(
        sessions,
        fixture.control_schema,
        replace(
            undo,
            expected_revision=3,
            idempotency_key=uuid4().hex,
        ),
        actor,
    )
    with pytest.raises((NetworkServingReadUnavailable, RetainedSiteAdoptionError)):
        await apply(fresh.command)
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM \"{fixture.control_schema}\".registry_record_history WHERE record_kind='site_binding'"
        )
        == 3
    )
