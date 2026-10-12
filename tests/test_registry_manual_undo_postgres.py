# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical undo preparation and actual draft correction on isolated native SQL."""

import importlib.util
import json
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.models.company_group_registry import CompanyGroupRegistry
from db.models.registry_revision import RegistryRecordHistory
from process.network_serving_read import NetworkServingReadUnavailable
from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_custom_history import read_registry_custom_history
from process.registry_manual_undo import (
    MAX_UNDO_SNAPSHOT_BYTES,
    RegistryManualUndoCommand,
    _prepare_correction,
    prepare_registry_manual_undo,
)
from process.registry_provider_batch import apply_registry_provider_batch
from process.registry_publication_queue import (
    claim_registry_publication_request,
    enqueue_registry_publication,
    finish_registry_publication_request,
)
from process.registry_record_store import RegistryRecordConflict, apply_registry_record_command
from process.registry_retained_site_adoption import RetainedSiteAdoptionError
from tests.test_company_registry_assertions_postgres import _ACTOR as _assertion_actor
from tests.test_company_registry_assertions_postgres import _create as _assertion_company
from tests.test_company_registry_assertions_postgres import _draft as _assertion_draft
from tests.test_company_registry_assertions_postgres import _identifier as _assertion_identifier
from tests.test_company_registry_assertions_postgres import assertion_db as assertion_db
from tests.test_manual_location_identity_store_postgres import _create as _location
from tests.test_manual_provider_identity_store_postgres import _create as _provider
from tests.test_manual_provider_identity_store_postgres import provider_db as provider_db
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_membership_draft_store_postgres import (
    _command as _membership_command,
)
from tests.test_network_membership_draft_store_postgres import (
    _seed as _membership_seed,
)
from tests.test_network_membership_draft_store_postgres import membership_db as membership_db
from tests.test_network_serving_schema_postgres import _candidate
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_network_source_binding_store_postgres import _apply as binding_apply
from tests.test_network_source_binding_store_postgres import _command as binding_command
from tests.test_network_source_binding_store_postgres import _row as binding_row
from tests.test_network_source_binding_store_postgres import _seed as binding_seed
from tests.test_registry_approval_store_postgres import _approve, _command, _draft
from tests.test_registry_approval_store_postgres import _command as _approval_command
from tests.test_registry_company_links_postgres import _assertion, _links
from tests.test_registry_management_permissions_postgres import _install
from tests.test_registry_management_permissions_postgres import permissions_db as permissions_db
from tests.test_registry_record_store_postgres import _actor, _create
from tests.test_registry_record_store_postgres import record_db as record_db
from tests.test_registry_retained_site_adoption_postgres import retained_db as retained_db

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
    result = await _apply(sessions, schema, preparation.command, actor, undo_provenance=preparation.provenance)
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
    result = await _apply(sessions, schema, prepared.command, actor, undo_provenance=prepared.provenance)
    later = replace(corrected, expected_revision=3, idempotency_key=uuid4().hex)
    await _apply(sessions, schema, later, actor)
    retry = await _prepare(sessions, schema, undo, actor)
    assert retry.command == prepared.command and retry.provenance.current_revision == 4
    assert await _apply(sessions, schema, retry.command, actor, undo_provenance=retry.provenance) == result
    for change in [replace(undo, reason="Changed reason"), replace(undo, expected_revision=3, target_revision=2)]:
        changed = await _prepare(sessions, schema, change, actor)
        with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
            await _apply(sessions, schema, changed.command, actor, undo_provenance=changed.provenance)
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, retry.command, replace(actor, user_id=uuid4()), undo_provenance=retry.provenance)
    stale = await _prepare(sessions, schema, replace(undo, idempotency_key=uuid4().hex), actor)
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _apply(sessions, schema, stale.command, actor, undo_provenance=stale.provenance)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 4


async def test_same_content_at_different_targets_still_has_distinct_audit_request(record_db):
    _, schema, sessions = record_db
    actor, _, corrected, undo = await _seed(record_db)
    identical = replace(corrected, expected_revision=2, idempotency_key=uuid4().hex)
    await _apply(sessions, schema, identical, actor)
    await _apply(sessions, schema, replace(identical, expected_revision=3, idempotency_key=uuid4().hex), actor)
    first = await _prepare(sessions, schema, replace(undo, expected_revision=4, target_revision=2), actor)
    result = await _apply(sessions, schema, first.command, actor, undo_provenance=first.provenance)
    different = await _prepare(sessions, schema, replace(undo, expected_revision=4, target_revision=3), actor)
    assert first.command.fields == different.command.fields
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, different.command, actor, undo_provenance=different.provenance)
    assert result["revision"] == 5


async def test_archive_state_is_separate_and_preparation_does_not_restore(record_db):
    _, schema, sessions = record_db
    actor, _, corrected, undo = await _seed(record_db)
    archived = replace(corrected, operation="archive", expected_revision=2, fields={}, idempotency_key=uuid4().hex)
    await _apply(sessions, schema, archived, actor)
    prepared = await _prepare(sessions, schema, replace(undo, expected_revision=3), actor)
    assert prepared.provenance.target_archived is False
    result = await _apply(sessions, schema, prepared.command, actor, undo_provenance=prepared.provenance)
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
    correction = await _apply(sessions, schema, prepared.command, actor, undo_provenance=prepared.provenance)
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
    correction = await _apply(sessions, schema, prepared.command, actor, undo_provenance=prepared.provenance)
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
            await _apply(
                sessions,
                schema,
                prepared.command,
                actor,
                undo_provenance=prepared.provenance,
                source_schema=source_schema,
            )
        assert await connection.fetchval(f'SELECT revision FROM "{schema}".network_membership_draft') == 2
    else:
        correction = await _apply(
            sessions, schema, prepared.command, actor, undo_provenance=prepared.provenance, source_schema=source_schema
        )
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

    async def apply(command, *, undo_provenance=None):
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            return await apply_registry_record_command(
                session, command, actor, schema=fixture.control_schema, undo_provenance=undo_provenance
            )

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
    correction_receipt = await apply(prepared.command, undo_provenance=prepared.provenance)
    assert correction_receipt["record"]["location_id"] == fields_by_name["location_id"]
    await fixture.connection.execute(f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false')
    retry = await _prepare(sessions, fixture.control_schema, undo, actor)
    assert await apply(retry.command, undo_provenance=retry.provenance) == correction_receipt
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
        await apply(fresh.command, undo_provenance=fresh.provenance)
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM \"{fixture.control_schema}\".registry_record_history WHERE record_kind='site_binding'"
        )
        == 3
    )


def _undo_group_assertion(company_id, **changes):
    return {
        "assertion_id": str(uuid4()),
        "company_id": str(company_id),
        "group_id": str(uuid4()),
        "group_kind": "corporate_parent",
        "relationship_kind": "reported_affiliation",
        "valid_from": "2025-06-30",
        "valid_to": "2025-06-30",
        "provenance": {
            "kind": "manual_reference",
            "evidence_ref": "Reviewed group reference",
            "snapshot_id": None,
            "source_record_key": None,
        },
    } | changes


def _company_undo_target():
    original = _assertion_company(identifiers=[_assertion_identifier()])
    snapshot_by_field = {
        "company_id": str(original.record_id),
        "revision": 1,
        "archived": False,
        **{key: original.fields[key] for key in ("display_name", "roles", "aliases")},
        **{key: original.fields["assertions"][key] for key in ("role_assertions", "identifier_assertions")},
    }
    history_by_field = {
        "record_json": snapshot_by_field,
        "request_sha256": "a" * 64,
        "custom_revision": 7,
        "current_revision": 2,
    }
    undo = RegistryManualUndoCommand("company", original.record_id, 2, 1, "Reviewed target", uuid4().hex)
    return undo, history_by_field


@pytest.mark.parametrize("groups", ["legacy", "empty", "manual_nullable", "source_nullable"])
async def test_company_undo_preparation_restores_exact_target_group_family(groups):
    undo, history_by_field = _company_undo_target()
    snapshot = history_by_field["record_json"]
    if groups != "legacy":
        snapshot["group_assertions"] = []
    if groups == "manual_nullable":
        snapshot["group_assertions"] = [_undo_group_assertion(undo.record_id, valid_from=None)]
    if groups == "source_nullable":
        snapshot["group_assertions"] = [
            _undo_group_assertion(
                undo.record_id,
                group_id=None,
                valid_from=None,
                valid_to=None,
                provenance={
                    "kind": "source_reference",
                    "evidence_ref": "Retained group observation",
                    "snapshot_id": str(uuid4()),
                    "source_record_key": "company-observation-17",
                },
            )
        ]
    before = deepcopy(history_by_field)
    prepared = _prepare_correction(undo, history_by_field, "company_id")
    assertions = prepared.command.fields["assertions"]
    assert "group_assertions" in assertions
    assert assertions["group_assertions"] == snapshot.get("group_assertions", [])
    for family in ("role_assertions", "identifier_assertions"):
        assert assertions[family] == snapshot[family]
    assert assertions["expected_revision"] == prepared.command.expected_revision == 2
    assert assertions["company_id"] == str(undo.record_id)
    assert prepared.command.idempotency_key == undo.idempotency_key
    assert prepared.provenance.target_custom_revision == 7
    assert prepared.provenance.target_request_sha256 == "a" * 64
    assert prepared.provenance.target_archived is False
    assert history_by_field == before


@pytest.mark.parametrize("malformation", ["null", "relationship", "company", "extra_key", "period"])
async def test_company_undo_preparation_rejects_explicit_malformed_group_history(malformation):
    undo, history_by_field = _company_undo_target()
    assertion = _undo_group_assertion(undo.record_id)
    changes_by_case = {
        "relationship": {"relationship_kind": "unsupported_relationship"},
        "company": {"company_id": str(uuid4())},
        "extra_key": {"unreviewed": True},
        "period": {"valid_to": "2025-06-29"},
    }
    history_by_field["record_json"]["group_assertions"] = (
        None if malformation == "null" else [assertion | changes_by_case[malformation]]
    )
    before = deepcopy(history_by_field)
    with pytest.raises(ValueError, match="company_registry_assertion_values_invalid"):
        _prepare_correction(undo, history_by_field, "company_id")
    assert history_by_field == before


@pytest.mark.parametrize("target_groups", ["legacy", "empty", "populated"])
async def test_company_group_undo_round_trip_preserves_history_and_replay(assertion_db, target_groups):
    fixture = assertion_db
    group = _create(
        "group", fields={"display_name": "Reviewed parent", "group_kind": "corporate_parent", "aliases": []}
    )
    await _assertion_draft(fixture, group)
    original = _assertion_company(identifiers=[_assertion_identifier()])
    assertion = _undo_group_assertion(original.record_id, group_id=str(group.record_id))
    if target_groups != "legacy":
        original.fields["assertions"]["group_assertions"] = [assertion] if target_groups == "populated" else []
    first = await _assertion_draft(fixture, original)
    later_fields = deepcopy(original.fields)
    later_fields["display_name"] = "Later company"
    later_fields["assertions"].update(
        expected_revision=1, group_assertions=[] if target_groups == "populated" else [assertion]
    )
    later = replace(
        original, operation="correct", expected_revision=1, fields=later_fields, idempotency_key=uuid4().hex
    )
    await _assertion_draft(fixture, later)
    undo = RegistryManualUndoCommand("company", original.record_id, 2, 1, "Reviewed historical groups", uuid4().hex)
    preparation = await _prepare(fixture.sessions, fixture.schema, undo, _assertion_actor)
    assert preparation.command.fields["assertions"]["group_assertions"] == first["record"]["group_assertions"]
    restored = await _assertion_draft(fixture, preparation.command, undo_provenance=preparation.provenance)
    assert restored["revision"] == 3
    for family in ("roles", "role_assertions", "identifier_assertions", "group_assertions"):
        assert restored["record"][family] == first["record"][family]
    assert await _assertion_draft(fixture, preparation.command, undo_provenance=preparation.provenance) == restored
    historical_record_by_field = json.loads(
        await fixture.connection.fetchval(
            f"SELECT record_json::text FROM {fixture.namespace}.registry_record_history "
            "WHERE record_kind='company' AND record_key=$1 AND revision=1",
            str(original.record_id),
        )
    )
    assert historical_record_by_field == first["record"]
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _assertion_draft(
            fixture, replace(preparation.command, idempotency_key=uuid4().hex), undo_provenance=preparation.provenance
        )


async def _audit(connection, schema, kind, identity, **kwargs):
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        return await read_registry_custom_history(connection, kind, identity, control_schema=schema, **kwargs)


async def test_exact_operations_undo_reference_replay_and_missing_predecessor(serving_schema):
    connection, schema, engine = serving_schema
    sessions = async_sessionmaker(engine)
    actor, creation = _actor(), _create()
    await _apply(sessions, schema, creation, actor)
    correction = replace(
        creation,
        operation="correct",
        expected_revision=1,
        fields={**creation.fields, "display_name": "Reviewed group"},
        idempotency_key=uuid4().hex,
    )
    await _apply(sessions, schema, correction, actor)
    undo = RegistryManualUndoCommand("group", creation.record_id, 2, 1, "Review earlier content", uuid4().hex)
    async with sessions() as session, session.begin():
        prepared = await prepare_registry_manual_undo(session, undo, actor, schema=schema)
        receipt = await apply_registry_record_command(
            session, prepared.command, actor, schema=schema, undo_provenance=prepared.provenance
        )
    async with sessions() as session, session.begin():
        assert (
            await apply_registry_record_command(
                session, prepared.command, actor, schema=schema, undo_provenance=prepared.provenance
            )
            == receipt
        )
        with pytest.raises(ValueError, match="registry_manual_undo_provenance_required"):
            await apply_registry_record_command(session, prepared.command, actor, schema=schema)
    page = await _audit(connection, schema, "group", creation.record_id, limit=2)
    assert page["has_more"] and [event["record_revision"] for event in page["events"]] == [3, 2]
    first = page["events"][0]
    assert first["operation"] == "correct" and first["draft_event"]["before_revision"] == 2
    assert first["draft_event"]["operation_context"]["operation"] == "undo"
    assert first["draft_event"]["operation_context"]["target_revision"] == 1
    assert first["before_snapshot_retained"] is True
    assert not any("before_record" in event["draft_event"] for event in page["events"])
    await connection.execute(f'DELETE FROM "{schema}".registry_record_history WHERE revision=2')
    missing = await _audit(connection, schema, "group", creation.record_id, limit=1)
    assert missing["events"][0]["draft_event"]["before_revision"] == 2
    assert missing["events"][0]["before_snapshot_retained"] is False
    await connection.execute(f'UPDATE "{schema}".registry_record_history SET event_json=NULL WHERE revision=1')
    legacy = await _audit(connection, schema, "group", creation.record_id, offset=1)
    assert legacy["events"][0]["operation"] is None and legacy["events"][0]["draft_event"] is None
    assert legacy["events"][0]["before_snapshot_retained"] is None


async def test_late_history_refusal_rolls_back_head_control_and_preserves_caller_work(record_db):
    connection, schema, sessions = record_db
    actor, creation = _actor(), _create()
    await _apply(sessions, schema, creation, actor)
    await connection.execute(
        f'ALTER TABLE "{schema}".registry_record_history ADD CONSTRAINT refuse_archive '
        "CHECK(event_json IS NULL OR event_json->>'operation'<>'archive')"
    )
    archive = replace(creation, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex)
    prior_id = uuid4()
    async with sessions() as session, session.begin():
        await session.execute(
            text(
                f'INSERT INTO "{schema}".company_group_registry '
                "(group_id,group_kind,display_name) VALUES (:identity,'corporate_parent','Earlier work')"
            ),
            {"identity": prior_id},
        )
        with pytest.raises(DBAPIError):
            await apply_registry_record_command(session, archive, actor, schema=schema)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_group_registry') == 2
    assert tuple(
        await connection.fetchrow(
            f'SELECT revision,archived FROM "{schema}".company_group_registry WHERE group_id=$1', creation.record_id
        )
    ) == (1, False)
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (1, 0)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 1


async def test_actual_approval_selections_exclude_carried_forward_equal_typed_ids(serving_schema):
    connection, schema, engine = serving_schema
    actor, shared_id = _actor(), uuid4()
    group = await _draft(engine, schema, replace(_create(), record_id=shared_id), actor)
    company = await _draft(engine, schema, replace(_create("company"), record_id=shared_id), actor)
    first = await _approve(connection, schema, await _command(connection, schema, group), actor)
    await _approve(connection, schema, await _command(connection, schema, company), actor)
    group_page = await _audit(connection, schema, "group", shared_id)
    company_page = await _audit(connection, schema, "company", shared_id)
    approvals = [event for event in group_page["events"] if event["lifecycle"] == "approved"]
    assert len(approvals) == 1 and approvals[0]["event_revision"] == first["approved_revision"]
    assert all(event["record"]["group_id"] == str(shared_id) for event in group_page["events"])
    assert all(event["record"]["company_id"] == str(shared_id) for event in company_page["events"])
    first_page = await _audit(connection, schema, "group", shared_id, limit=1)
    second_page = await _audit(connection, schema, "group", shared_id, limit=1, offset=1)
    assert first_page["has_more"] and not second_page["has_more"]
    assert first_page["events"] + second_page["events"] == group_page["events"]
    assert (await _audit(connection, schema, "group", shared_id, maximum_revision=0))["events"] == []


async def test_additive_migration_preserves_existing_data_and_exact_model_columns(record_db):
    connection, schema, sessions = record_db
    creation = _create()
    await _apply(sessions, schema, creation, _actor())
    legacy_schema = "history_legacy_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{legacy_schema}"')
        await connection.execute(
            f'CREATE TABLE "{legacy_schema}".registry_record_history '
            f'(LIKE "{schema}".registry_record_history INCLUDING ALL)'
        )
        await connection.execute(
            f'ALTER TABLE "{legacy_schema}".registry_record_history DROP CONSTRAINT registry_history_event'
        )
        await connection.execute(f'ALTER TABLE "{legacy_schema}".registry_record_history DROP COLUMN event_json')
        columns = [column for column in RegistryRecordHistory.__table__.columns.keys() if column != "event_json"]
        await connection.execute(
            f'INSERT INTO "{legacy_schema}".registry_record_history ({",".join(columns)}) '
            f'SELECT {",".join(columns)} FROM "{schema}".registry_record_history'
        )
        before = await connection.fetchval(
            f'SELECT to_jsonb(history)::text FROM "{legacy_schema}".registry_record_history history'
        )
        path = Path(__file__).parents[1] / "alembic/versions/20261013010000_registry_custom_history_events.py"
        spec = importlib.util.spec_from_file_location("history_event_migration", path)
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        for statement in migration._ddl(legacy_schema):
            await connection.execute(statement)
        after = await connection.fetchval(
            f"SELECT (to_jsonb(history)-'event_json')::text FROM \"{legacy_schema}\".registry_record_history history"
        )
        assert before == after
        assert await connection.fetchval(f'SELECT event_json IS NULL FROM "{legacy_schema}".registry_record_history')
        actual = await connection.fetch(
            "SELECT attname::text FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
            f'"{legacy_schema}".registry_record_history',
        )
        assert [column["attname"] for column in actual] == list(RegistryRecordHistory.__table__.columns.keys())
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal)",
            f'"{legacy_schema}".registry_record_history',
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{legacy_schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", legacy_schema) is None


async def test_derived_history_insert_grant_keeps_38_tables_and_denies_mutation(permissions_db):
    fixture = permissions_db
    installed = await _install(fixture)
    assert len(installed["table_oids"]) == 38
    table = f'"{fixture.schema}".registry_record_history'
    role = fixture.roles_by_kind["api"]
    assert await fixture.connection.fetchval("SELECT has_column_privilege($1,$2,'event_json','INSERT')", role, table)
    assert not await fixture.connection.fetchval(
        "SELECT has_column_privilege($1,$2,'event_json','UPDATE')", role, table
    )
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{role}"')
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with fixture.connection.transaction():
                await fixture.connection.execute(f"UPDATE {table} SET event_json=NULL")


@pytest.mark.parametrize(
    "kind,identity,kwargs",
    [
        ("network", True, {}),
        ("network", uuid4(), {}),
        ("group", 1, {}),
        ("network", 1, {"limit": 51}),
        ("network", 1, {"offset": -1}),
        ("network", 1, {"minimum_revision": True}),
        ("network", 1, {"maximum_revision": -1}),
    ],
)
async def test_invalid_audit_bounds_stop_before_sql(record_db, kind, identity, kwargs):
    connection, schema, _ = record_db
    with pytest.raises(ValueError):
        async with connection.transaction():
            await read_registry_custom_history(connection, kind, identity, control_schema=schema, **kwargs)


async def test_audit_output_overflow_refuses_without_returning_or_mutating_history(serving_schema):
    connection, schema, engine = serving_schema
    creation = _create()
    await _draft(engine, schema, creation, _actor())
    await connection.execute(
        f'UPDATE "{schema}".registry_record_history SET record_json='
        "jsonb_set(record_json,'{aliases}',jsonb_build_array(repeat('a',1048576)))"
    )
    before = await connection.fetchval(f'SELECT md5(record_json::text) FROM "{schema}".registry_record_history')
    with pytest.raises(ValueError, match="output_limit"):
        await _audit(connection, schema, "group", creation.record_id)
    assert await connection.fetchval(f'SELECT md5(record_json::text) FROM "{schema}".registry_record_history') == before


@pytest.mark.parametrize(
    "event",
    [
        {},
        {"operation": "guessed", "before_revision": 0, "operation_context": {}},
        {"operation": "create", "before_revision": False, "operation_context": {}},
        {"operation": "create", "before_revision": 1, "operation_context": {}},
        {"operation": "create", "before_revision": 0, "operation_context": None},
        {"operation": "create", "before_revision": 0, "operation_context": {}, "extra": True},
        {"operation": "create", "before_revision": 0, "operation_context": {"padding": "a" * 16384}},
    ],
)
async def test_native_metadata_shape_cannot_accept_guessed_or_mismatched_events(record_db, event):
    connection, schema, sessions = record_db
    await _apply(sessions, schema, _create(), _actor())
    with pytest.raises(asyncpg.CheckViolationError):
        async with connection.transaction():
            await connection.execute(
                f'UPDATE "{schema}".registry_record_history SET event_json=$1::jsonb', json.dumps(event)
            )
    assert (
        json.loads(await connection.fetchval(f'SELECT event_json::text FROM "{schema}".registry_record_history'))[
            "operation"
        ]
        == "create"
    )


async def test_network_lifecycle_preserves_integer_subject_and_refuses_wrong_snapshot_type(serving_schema):
    connection, schema, engine = serving_schema
    actor, command = _actor(), _create("network")
    current = await _draft(engine, schema, command, actor)
    identity = current["record_id"]
    for operation in ("correct", "archive", "restore"):
        command = replace(
            command,
            record_id=identity,
            allocation_key=None,
            operation=operation,
            expected_revision=current["revision"],
            idempotency_key=uuid4().hex,
            fields={"display_name": "Reviewed Network", "aliases": []} if operation == "correct" else {},
        )
        current = await _draft(engine, schema, command, actor)
    audit = await _audit(connection, schema, "network", identity)
    assert type(audit["record_id"]) is int
    assert [event["operation"] for event in audit["events"]] == ["restore", "archive", "correct", "create"]
    assert [event["draft_event"]["before_revision"] for event in audit["events"]] == [3, 2, 1, 0]
    await connection.execute(
        f"UPDATE \"{schema}\".registry_record_history SET record_json=jsonb_set(record_json,'{{network_id}}','true') WHERE revision=4"
    )
    with pytest.raises(ValueError, match="snapshot_invalid"):
        await _audit(connection, schema, "network", identity)


async def test_specialized_binding_is_auditable_without_enabling_undo(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    networks = await binding_seed(connection, schema)
    row = binding_row(networks[0])
    await binding_apply(connection, schema, binding_command([row]), actor)
    identity = UUID(row["binding_id"])
    audit = await _audit(connection, schema, "network_binding", identity)
    assert audit["events"][0]["operation"] == "bind"
    assert audit["events"][0]["record"]["evidence_sha256"] == row["evidence_sha256"]
    async with async_sessionmaker(engine)() as session, session.begin():
        with pytest.raises(ValueError, match="undo_request_invalid"):
            await prepare_registry_manual_undo(
                session,
                RegistryManualUndoCommand("network_binding", identity, 2, 1, "Review", uuid4().hex),
                actor,
                schema=schema,
            )


async def test_completed_queue_receipt_requires_exact_manifest_and_custom_revision(serving_schema):
    """Component fixture uses real queue stores, without claiming serving pipeline execution."""
    connection, schema, engine = serving_schema
    actor = _actor()
    draft = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, draft)
    async with connection.transaction():
        queued = await enqueue_registry_publication(
            connection,
            command,
            actor,
            session_token_sha256="a" * 64,
            expected_serving_generation=0,
            control_schema=schema,
        )
        lease = await claim_registry_publication_request(
            connection, operation_family="publication", control_schema=schema
        )
    approval = await _approve(connection, schema, command, actor)
    candidate_id = uuid4()
    await _candidate(connection, schema, candidate_id)
    generation = await connection.fetchval(
        f'INSERT INTO "{schema}".network_serving_manifest '
        "(candidate_id,schema_revision,source_generations,approved_custom_revision,manifest_sha256) VALUES($1,1,'{}',$2,$3) RETURNING generation_id",
        candidate_id,
        approval["approved_revision"],
        "b" * 64,
    )
    identity = UUID(draft["record_id"])
    assert not any(
        event["lifecycle"] == "publication" for event in (await _audit(connection, schema, "group", identity))["events"]
    )
    async with connection.transaction():
        await finish_registry_publication_request(
            connection,
            UUID(queued["request_id"]),
            lease["lease_id"],
            state="completed",
            result={
                **{key: approval[key] for key in ("approved_revision", "previous_approved_revision", "selected_count")},
                "serving_generation": generation,
                "policy_revision": 7,
            },
            control_schema=schema,
        )
    events = (await _audit(connection, schema, "group", identity))["events"]
    published_events = [event for event in events if event["lifecycle"] == "publication"]
    assert len(published_events) == 1 and published_events[0]["publication"]["generation_id"] == generation
    await connection.execute(
        f"UPDATE \"{schema}\".registry_publication_request SET result_json=jsonb_set(result_json,'{{approved_revision}}','999999')"
    )
    assert not any(
        event["lifecycle"] == "publication" for event in (await _audit(connection, schema, "group", identity))["events"]
    )


async def test_installed_legacy_grants_migrate_and_reconcile_without_expanding_custody(permissions_db):
    """Existing column grants stay closed until the normal reconciler adds the new insert column."""
    fixture = permissions_db
    installed = await _install(fixture)
    connection, schema = fixture.connection, fixture.schema
    table = f'"{schema}".registry_record_history'
    api_role = fixture.roles_by_kind["api"]
    await connection.execute(f"ALTER TABLE {table} DROP CONSTRAINT registry_history_event")
    await connection.execute(f"ALTER TABLE {table} DROP COLUMN event_json")
    assert await connection.fetchval("SELECT has_column_privilege($1,$2,'revision','INSERT')", api_role, table)
    path = Path(__file__).parents[1] / "alembic/versions/20261013010000_registry_custom_history_events.py"
    spec = importlib.util.spec_from_file_location("history_grant_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    for statement in migration._ddl(schema):
        await connection.execute(statement)
    assert not await connection.fetchval("SELECT has_column_privilege($1,$2,'event_json','INSERT')", api_role, table)
    reconciled = await _install(fixture)
    assert reconciled["table_oids"] == installed["table_oids"] and len(reconciled["table_oids"]) == 38
    assert await connection.fetchval("SELECT has_column_privilege($1,$2,'event_json','INSERT')", api_role, table)
    assert not await connection.fetchval("SELECT has_column_privilege($1,$2,'event_json','UPDATE')", api_role, table)


async def test_first_undo_apply_requires_provenance_before_sql_and_preserves_all_state(record_db):
    connection, schema, sessions = record_db
    actor, _, corrected, undo = await _seed(record_db)
    prepared = await _prepare(sessions, schema, undo, actor)
    assert prepared.command._undo_provenance_required is True

    async def retained_state():
        return (
            await connection.fetchrow(
                f'SELECT * FROM "{schema}".company_group_registry WHERE group_id=$1', corrected.record_id
            ),
            await connection.fetch(f'SELECT * FROM "{schema}".registry_revision_control ORDER BY id'),
            await connection.fetch(
                f'SELECT * FROM "{schema}".registry_record_history ORDER BY custom_revision, revision'
            ),
        )

    before = await retained_state()
    statements = []

    def count(_connection, _cursor, statement, *_args):
        statements.append(statement)

    async with sessions() as session, session.begin():
        engine = session.bind.sync_engine
        event.listen(engine, "before_cursor_execute", count)
        try:
            with pytest.raises(ValueError, match="registry_manual_undo_provenance_required"):
                await apply_registry_record_command(session, prepared.command, actor, schema=schema)
        finally:
            event.remove(engine, "before_cursor_execute", count)
    assert statements == []
    assert await retained_state() == before
    receipt = await _apply(sessions, schema, prepared.command, actor, undo_provenance=prepared.provenance)
    committed = await retained_state()
    assert receipt["revision"] == 3 and receipt["custom_revision"] == 3
    assert await _apply(sessions, schema, prepared.command, actor, undo_provenance=prepared.provenance) == receipt
    assert await retained_state() == committed
    event_document = json.loads(
        await connection.fetchval(f'SELECT event_json::text FROM "{schema}".registry_record_history WHERE revision=3')
    )
    assert event_document["operation_context"]["operation"] == "undo"
    assert event_document["operation_context"]["target_revision"] == 1


async def test_provider_batch_refuses_prepared_undo_before_database(record_db):
    connection, schema, sessions = record_db
    actor, _, corrected, undo = await _seed(record_db, "provider")
    prepared = await _prepare(sessions, schema, undo, actor)
    assert prepared.command._undo_provenance_required is True
    snapshot_query = (
        f"SELECT to_jsonb(provider) AS provider,"
        f'(SELECT jsonb_agg(history ORDER BY custom_revision) FROM "{schema}".registry_record_history history) AS history,'
        f'(SELECT to_jsonb(control) FROM "{schema}".registry_revision_control control WHERE id=1) AS control '
        f'FROM "{schema}".manual_provider_registry provider WHERE provider_id=$1'
    )
    before = await connection.fetchrow(snapshot_query, corrected.record_id)
    with pytest.raises(ValueError, match="registry_manual_undo_provenance_required"):
        await apply_registry_provider_batch(
            object(), [prepared.command], [], actor, control_schema=schema, source_schema=schema
        )
    assert await connection.fetchrow(snapshot_query, corrected.record_id) == before
