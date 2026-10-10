# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preview exact manual selections without publishing or changing durable data."""

import asyncio
import copy
import json
from contextlib import AsyncExitStack, aclosing
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from process import registry_imported_selection_preview as imported_preview
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import compose_registry_membership_candidate
from process.registry_imported_selection_preview import RegistryImportedSelectionUnavailable
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests import cms_registry_recipe_postgres_support as cms_recipes
from tests import test_network_initial_source_office_bindings_postgres as office_tests
from tests import test_registry_source_recipe_composition_postgres as recipe_tests
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_fhir_membership_source_postgres import fhir_source, reviewed_fhir_source
from tests.test_network_initial_source_office_bindings_postgres import office_db
from tests.test_network_legacy_membership_source_postgres import _aca_binding, _write_aca_bindings
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import (
    _actor,
    _approve,
    _bulk_manual_versions,
    _command,
    _CountedConnection,
    _create,
    _draft,
)
from tests.test_registry_candidate_composition_postgres import (
    _initial_arguments,
    _remove_candidates,
    _roles,
    initial_composition,
)
from tests.test_registry_source_recipe_composition_postgres import _publish_raw_recipe

pytestmark = pytest.mark.asyncio
_DURABLE_TABLES = (
    "registry_revision_control",
    "registry_record_history",
    "registry_approval_history",
    "registry_approved_record",
    "company_group_registry",
    "company_registry",
    "network_registry_identity",
    "network_registry_record",
    "registry_network_binding",
    "registry_network_binding_batch",
    "network_membership_candidate",
    "network_serving_control",
    "network_serving_manifest",
)


async def _preview(connection, schema, command, actor):
    async with connection.transaction():
        return await preview_registry_approval(connection, command, actor, control_schema=schema)


async def _durable_state(connection, schema):
    return {
        table: await connection.fetchval(
            f"SELECT coalesce(jsonb_agg(to_jsonb(state) ORDER BY to_jsonb(state)::text),'[]'::jsonb)::text "
            f'FROM "{schema}".{table} state'
        )
        for table in _DURABLE_TABLES
    }


async def _assert_staging_removed(connection):
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_approval_%'"
        )
        == 0
    )


async def test_preview_selects_only_requested_pending_records(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected = await _draft(engine, schema, _create("network"), actor)
    await _draft(engine, schema, _create("group"), _actor("client_other"))
    command = await _command(connection, schema, selected)
    initial_state = await _durable_state(connection, schema)
    preview = await _preview(connection, schema, command, actor)
    assert preview == {
        "expected_draft_revision": 2,
        "expected_approved_revision": 0,
        "selected_count": 1,
        "additions_count": 1,
        "corrections_count": 0,
        "archives_count": 0,
        "restores_count": 0,
        "unresolved_count": 0,
        "records": [
            {
                "record_kind": "network",
                "record_id": selected["record_id"],
                "record_revision": 1,
                "before": None,
                "after": selected["record"],
            }
        ],
    }
    assert type(preview["records"][0]["record_id"]) is int
    assert await _preview(connection, schema, command, actor) == preview
    assert await _durable_state(connection, schema) == initial_state
    await _assert_staging_removed(connection)


async def test_preview_before_is_prior_approval_and_after_exact_history(serving_schema):
    connection, schema, engine = serving_schema
    actor, creation = _actor(), _create()
    original = await _draft(engine, schema, creation, actor)
    await _approve(connection, schema, await _command(connection, schema, original), actor)
    correction = replace(
        creation,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields={**creation.fields, "display_name": "Intermediate pending"},
    )
    await _draft(engine, schema, correction, actor)
    latest = await _draft(
        engine,
        schema,
        replace(
            correction,
            expected_revision=2,
            idempotency_key=uuid4().hex,
            fields={**creation.fields, "display_name": "Selected correction"},
        ),
        actor,
    )
    initial_state = await _durable_state(connection, schema)
    preview = await _preview(connection, schema, await _command(connection, schema, latest), actor)
    assert preview["records"][0] == {
        "record_kind": "group",
        "record_id": original["record_id"],
        "record_revision": 3,
        "before": original["record"],
        "after": latest["record"],
    }
    assert preview["corrections_count"] == preview["selected_count"] == 1
    assert preview["additions_count"] == preview["archives_count"] == preview["restores_count"] == 0
    assert await _durable_state(connection, schema) == initial_state


async def _classified_versions(connection, schema, engine, actor):
    commands = [_create(kind) for kind in ("group", "company", "network")]
    original_versions = [await _draft(engine, schema, command, actor) for command in commands]
    archived = await _draft(
        engine,
        schema,
        replace(commands[0], operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex),
        actor,
    )
    await _approve(connection, schema, await _command(connection, schema, archived, *original_versions[1:]), actor)
    restored = await _draft(
        engine,
        schema,
        replace(commands[0], operation="restore", expected_revision=2, fields={}, idempotency_key=uuid4().hex),
        actor,
    )
    company_archive = await _draft(
        engine,
        schema,
        replace(commands[1], operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex),
        actor,
    )
    network_correction = await _draft(
        engine,
        schema,
        replace(
            commands[2],
            record_id=original_versions[2]["record_id"],
            allocation_key=None,
            operation="correct",
            expected_revision=1,
            idempotency_key=uuid4().hex,
            fields={**commands[2].fields, "display_name": "Corrected network"},
        ),
        actor,
    )
    addition = await _draft(engine, schema, _create(), actor)
    return restored, company_archive, network_correction, addition


async def test_archive_restore_correction_and_addition_counts_are_exclusive(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected_versions = await _classified_versions(connection, schema, engine, actor)
    command = await _command(connection, schema, *selected_versions)
    preview = await _preview(connection, schema, command, actor)
    assert preview["selected_count"] == 4
    assert [preview[key] for key in ("additions_count", "corrections_count", "archives_count", "restores_count")] == [
        1
    ] * 4
    assert preview["unresolved_count"] == 0
    assert sorted(record["record_kind"] for record in preview["records"]) == ["company", "group", "group", "network"]
    assert (
        await _preview(connection, schema, replace(command, selection=tuple(reversed(command.selection))), actor)
        == preview
    )


@pytest.mark.parametrize("change", [{"expected_draft_revision": 0}, {"expected_approved_revision": 1}])
async def test_live_control_cas_is_required_for_preview(serving_schema, change):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, selected, **change)
    with pytest.raises(RegistryApprovalConflict, match="revision_conflict"):
        await _preview(connection, schema, command, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0
    await _assert_staging_removed(connection)


async def test_missing_control_and_old_approval_command_are_rejected(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, selected)
    await _approve(connection, schema, command, actor)
    with pytest.raises(RegistryApprovalConflict, match="revision_conflict"):
        await _preview(connection, schema, command, actor)
    command = await _command(connection, schema, selected)
    await connection.execute(f'DELETE FROM "{schema}".registry_revision_control')
    with pytest.raises(RegistryApprovalConflict, match="control_unavailable"):
        await _preview(connection, schema, command, actor)


@pytest.mark.parametrize("missing", ["head", "history"])
async def test_missing_selected_head_or_history_is_rejected(serving_schema, missing):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, selected)
    table = "company_group_registry" if missing == "head" else "registry_record_history"
    await connection.execute(f'DELETE FROM "{schema}".{table}')
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _preview(connection, schema, command, actor)
    await _assert_staging_removed(connection)


async def test_stale_selected_revision_and_changed_history_are_rejected(serving_schema):
    connection, schema, engine = serving_schema
    actor, creation = _actor(), _create()
    original = await _draft(engine, schema, creation, actor)
    corrected = await _draft(
        engine, schema, replace(creation, operation="correct", expected_revision=1, idempotency_key=uuid4().hex), actor
    )
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _preview(connection, schema, await _command(connection, schema, original), actor)
    await connection.execute(
        f'UPDATE "{schema}".registry_record_history '
        "SET record_json=jsonb_set(record_json,'{display_name}','\"Changed\"') WHERE revision=2"
    )
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _preview(connection, schema, await _command(connection, schema, corrected), actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0


async def test_preview_requires_actor_shape_namespace_and_caller_transaction(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, selected)
    with pytest.raises(ValueError, match="caller_transaction"):
        await preview_registry_approval(connection, command, actor, control_schema=schema)
    for invalid_actor in ({}, replace(actor, user_id=UUID(int=0)), replace(actor, client_id=" invalid ")):
        with pytest.raises(ValueError):
            await _preview(connection, schema, command, invalid_actor)
    with pytest.raises(ValueError):
        await _preview(connection, 'invalid";DROP SCHEMA public;', command, actor)
    with pytest.raises(ValueError, match="selection_invalid"):
        await _preview(connection, schema, replace(command, selection=({"record_kind": "group"},)), actor)


async def test_caller_rollback_and_caught_failure_preserve_durable_state(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, selected)
    initial_state = await _durable_state(connection, schema)
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with connection.transaction():
            await preview_registry_approval(connection, command, actor, control_schema=schema)
            raise RuntimeError("caller rollback")
    async with connection.transaction():
        await connection.execute("CREATE TEMP TABLE caller_preview_work(value int) ON COMMIT DROP")
        await connection.execute("INSERT INTO caller_preview_work VALUES(8)")
        with pytest.raises(RegistryApprovalConflict, match="revision_conflict"):
            await preview_registry_approval(
                connection, replace(command, expected_draft_revision=0), actor, control_schema=schema
            )
        assert await connection.fetchval("SELECT value FROM caller_preview_work") == 8
        await _assert_staging_removed(connection)
    assert await _durable_state(connection, schema) == initial_state


async def test_serialized_unicode_response_overflow_is_rejected(serving_schema):
    connection, schema, engine = serving_schema
    actor, creation = _actor(), _create()
    aliases = ["\U0001f539" * 509 + f"{number:03}" for number in range(100)]
    creation = replace(creation, fields={**creation.fields, "aliases": aliases})
    original = await _draft(engine, schema, creation, actor)
    await _approve(connection, schema, await _command(connection, schema, original), actor)
    corrected = await _draft(
        engine, schema, replace(creation, operation="correct", expected_revision=1, idempotency_key=uuid4().hex), actor
    )
    assert len(json.dumps({"before": original["record"], "after": corrected["record"]}).encode()) > 1048576
    initial_state = await _durable_state(connection, schema)
    command = await _command(connection, schema, corrected)
    async with connection.transaction():
        await connection.execute("CREATE TEMP TABLE caller_overflow_work(value int) ON COMMIT DROP")
        await connection.execute("INSERT INTO caller_overflow_work VALUES(9)")
        with pytest.raises(ValueError, match="preview_too_large"):
            await preview_registry_approval(connection, command, actor, control_schema=schema)
        assert await connection.fetchval("SELECT value FROM caller_overflow_work") == 9
        await _assert_staging_removed(connection)
    assert await _durable_state(connection, schema) == initial_state


async def test_full_response_size_and_sql_count_are_bounded_at_five_thousand(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    seed = await _draft(engine, schema, _create(), actor)
    await _bulk_manual_versions(connection, schema, seed)
    counted = _CountedConnection(connection)
    preview = await _preview(counted, schema, await _command(connection, schema, seed), actor)
    one_count = counted.statements
    assert len(json.dumps(preview, sort_keys=True).encode()) < 1048576
    selected_versions = tuple(
        dict(row)
        for row in await connection.fetch(
            f'SELECT record_kind,record_key AS record_id,revision FROM "{schema}".registry_record_history'
        )
    )
    counted.statements = 0
    command = await _command(connection, schema, selection=selected_versions)
    with pytest.raises(ValueError, match="preview_too_large"):
        await _preview(counted, schema, command, actor)
    assert counted.statements == one_count == 7  # Includes one prospective membership anti-join.
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (5000, 0)
    await _assert_staging_removed(connection)


@pytest.fixture
async def retained_aca_preview(initial_composition):
    """Publish real exact offices and retain their sealed raw recipe, with exact cleanup."""
    fixture = initial_composition
    copy_targets = []
    _, arguments = await _initial_arguments(fixture)
    try:
        target, addresses = await compose_registry_membership_candidate(fixture.connection, **arguments)
        copy_targets.append(target)
        await prepare_and_publish_network_candidate(
            fixture.connection, target, addresses, **_roles(fixture), control_schema=fixture.control_schema
        )
        yield fixture
    finally:
        await _remove_candidates(fixture, copy_targets, arguments["request_id"])


@pytest.fixture
async def expanded_aca_preview(serving_schema, monkeypatch):
    """Publish all 10000 raw facts, keeping only the fixture's office sample small."""
    original_reader = office_tests._reviewed_batch

    async def read_office_sample(fixture):
        original_count = fixture.count
        fixture.count = 2
        try:
            return await original_reader(fixture)
        finally:
            fixture.count = original_count

    monkeypatch.setattr(office_tests, "_reviewed_batch", read_office_sample)
    async with aclosing(office_db.__wrapped__(serving_schema, SimpleNamespace(param=10000))) as office_fixtures:
        offices = await anext(office_fixtures)
        async with aclosing(initial_composition.__wrapped__(offices, serving_schema)) as composition_fixtures:
            composed = await anext(composition_fixtures)
            async with aclosing(retained_aca_preview.__wrapped__(composed)) as retained_fixtures:
                yield await anext(retained_fixtures)


async def _pending_aca_operation(fixture, operation, count=1):
    connection, schema = fixture.connection, fixture.control_schema
    stored_heads = await connection.fetch(
        f'SELECT to_jsonb(head)::text FROM "{schema}".registry_network_binding head ORDER BY source_key'
    )
    network = None
    if operation == "rebind":
        network = await _draft(fixture.engine, schema, _create("network"), _actor())
    commands = []
    for index, stored in enumerate(stored_heads[:count], 1):
        head = json.loads(stored[0])
        commands.append(
            _aca_binding(
                fixture.recipes[0].binding_coordinates,
                network["record_id"] if network else head["network_id"],
                index,
                binding_id=head["binding_id"],
                operation=operation,
                expected_revision=head["revision"],
                expected_network_id=head["network_id"],
            )
        )
    receipt = await _write_aca_bindings(connection, schema, _actor(), commands)
    selected = receipt["records"] + ([network] if network else [])
    return await _command(connection, schema, *selected)


async def _repeatable_preview(fixture, command):
    async with fixture.connection.transaction(isolation="repeatable_read"):
        return await preview_registry_approval(
            fixture.connection, command, _actor(), control_schema=fixture.control_schema
        )


@pytest.mark.parametrize(
    "operation,count,mapped,omitted", [("rebind", 1, 2, 0), ("close", 1, 1, 1), ("close", 2, 0, 2)]
)
async def test_retained_imported_impact_uses_pending_history_without_approval(
    retained_aca_preview, operation, count, mapped, omitted
):
    fixture = retained_aca_preview
    command = await _pending_aca_operation(fixture, operation, count)
    initial_state = await _durable_state(fixture.connection, fixture.control_schema)
    preview = await _repeatable_preview(fixture, command)
    assert preview["source_selection"] == {
        "status": "available",
        "mapped_rows": mapped,
        "omitted_rows": omitted,
        "unresolved_rows": 0,
    }
    assert preview["unresolved_count"] == 0
    assert await _repeatable_preview(fixture, command) == preview
    assert await _durable_state(fixture.connection, fixture.control_schema) == initial_state
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest')
        == 1
    )
    await _assert_staging_removed(fixture.connection)


async def test_unselected_pending_close_never_enters_imported_preview(retained_aca_preview):
    fixture = retained_aca_preview
    await _pending_aca_operation(fixture, "close")
    unrelated = await _draft(fixture.engine, fixture.control_schema, _create(), _actor())
    command = await _command(fixture.connection, fixture.control_schema, unrelated)
    preview = await _repeatable_preview(fixture, command)
    assert preview["source_selection"] == {
        "status": "available",
        "mapped_rows": 2,
        "omitted_rows": 0,
        "unresolved_rows": 0,
    }


async def test_writable_retained_source_is_explicitly_unavailable(retained_aca_preview):
    fixture = retained_aca_preview
    command = await _pending_aca_operation(fixture, "close")
    source = fixture.recipes[0].source_pin
    schema, reader = source.prepared.ownership.schema_name, source.runtime_roles[0]
    await fixture.connection.execute(f'GRANT UPDATE ON "{schema}".mrf_address_evidence TO "{reader}"')
    try:
        preview = await _repeatable_preview(fixture, command)
        assert preview["source_selection"] == {"status": "unavailable"}
    finally:
        await fixture.connection.execute(f'REVOKE UPDATE ON "{schema}".mrf_address_evidence FROM "{reader}"')


async def test_bad_mapped_office_counts_unresolved_before_approval(retained_aca_preview):
    fixture = retained_aca_preview
    source = fixture.recipes[0].source_pin
    await fixture.connection.execute(
        f'UPDATE "{source.prepared.ownership.schema_name}".mrf_address_evidence SET address_key=NULL WHERE evidence_checksum=1'
    )
    unrelated = await _draft(fixture.engine, fixture.control_schema, _create(), _actor())
    preview = await _repeatable_preview(fixture, await _command(fixture.connection, fixture.control_schema, unrelated))
    assert preview["source_selection"] == {
        "status": "available",
        "mapped_rows": 1,
        "omitted_rows": 0,
        "unresolved_rows": 1,
    }
    assert preview["unresolved_count"] == 1


async def test_imported_preview_requires_repeatable_snapshot(retained_aca_preview):
    fixture = retained_aca_preview
    command = await _pending_aca_operation(fixture, "close")
    with pytest.raises(RegistryImportedSelectionUnavailable, match="requires_repeatable_read"):
        await _preview(fixture.connection, fixture.control_schema, command, _actor())
    await _assert_staging_removed(fixture.connection)


async def test_imported_preview_deadline_never_returns_zero(retained_aca_preview, monkeypatch):
    fixture = retained_aca_preview
    command = await _pending_aca_operation(fixture, "close")
    actual_sql = imported_preview._impact_sql

    def delayed_aggregate(*arguments):
        return actual_sql(*arguments).replace(
            "SELECT (SELECT count(*) FROM page)",
            "SELECT (SELECT 0 FROM pg_sleep(1)) AS delayed, (SELECT count(*) FROM page)",
        )

    monkeypatch.setattr(imported_preview, "_impact_sql", delayed_aggregate)
    monkeypatch.setattr(imported_preview, "PREVIEW_DEADLINE_SECONDS", 0.1)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute("CREATE TEMP TABLE caller_deadline_work(value int) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO caller_deadline_work VALUES(9)")
        with pytest.raises(RegistryImportedSelectionUnavailable, match="selection_unavailable"):
            await preview_registry_approval(
                fixture.connection, command, _actor(), control_schema=fixture.control_schema
            )
        assert await fixture.connection.fetchval("SELECT value FROM caller_deadline_work") == 9
        await _assert_staging_removed(fixture.connection)


async def test_imported_preview_rejects_unrecognized_paging(retained_aca_preview, monkeypatch):
    fixture = retained_aca_preview
    command = await _pending_aca_operation(fixture, "close")
    original_sql = imported_preview.aca_reviewed_sql
    monkeypatch.setattr(
        imported_preview,
        "aca_reviewed_sql",
        lambda *arguments: original_sql(*arguments).replace("LIMIT 5001", "LIMIT 5002"),
    )
    with pytest.raises(RegistryImportedSelectionUnavailable, match="selection_unavailable"):
        await _repeatable_preview(fixture, command)
    await _assert_staging_removed(fixture.connection)


@pytest.mark.parametrize(
    "operation,count,mapped,omitted", [("rebind", 1, 10000, 0), ("close", 1, 5000, 5000), ("close", 2, 0, 10000)]
)
async def test_complete_imported_counts_above_copy_page(expanded_aca_preview, operation, count, mapped, omitted):
    fixture = expanded_aca_preview
    command = await _pending_aca_operation(fixture, operation, count)
    initial_state = await _durable_state(fixture.connection, fixture.control_schema)
    counted = _CountedConnection(fixture.connection)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        preview = await preview_registry_approval(counted, command, _actor(), control_schema=fixture.control_schema)
    assert preview["source_selection"] == {
        "status": "available",
        "mapped_rows": mapped,
        "omitted_rows": omitted,
        "unresolved_rows": 0,
    }
    assert counted.statements == 16
    assert preview["unresolved_count"] == 0
    assert await _durable_state(fixture.connection, fixture.control_schema) == initial_state
    await _assert_staging_removed(fixture.connection)


@pytest.mark.parametrize("office_db", [1, 100], indirect=True)
async def test_imported_preview_query_count_is_fixed_per_recipe(retained_aca_preview, office_db):
    fixture = retained_aca_preview
    unrelated = await _draft(fixture.engine, fixture.control_schema, _create(), _actor())
    command = await _command(fixture.connection, fixture.control_schema, unrelated)
    counted = _CountedConnection(fixture.connection)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        preview = await preview_registry_approval(counted, command, _actor(), control_schema=fixture.control_schema)
    assert preview["source_selection"] == {
        "status": "available",
        "mapped_rows": office_db.count,
        "omitted_rows": 0,
        "unresolved_rows": 0,
    }
    assert counted.statements == 16


async def test_bootstrap_binding_preview_has_no_invented_source_count(office_db):
    connection, schema = office_db.connection, office_db.control
    heads = await connection.fetch(
        f'SELECT binding_id::text AS record_id,revision FROM "{schema}".registry_network_binding'
    )
    selected_records = tuple({"record_kind": "network_binding", **dict(head)} for head in heads)
    command = await _command(connection, schema, selection=selected_records)
    preview = await _preview(connection, schema, command, _actor())
    assert preview["source_selection"] == {"status": "unavailable"}


async def test_retained_fhir_preview_requires_complete_custody(custom_db, reviewed_fhir_source):
    fixture = custom_db
    copy_targets = []
    try:
        await _publish_raw_recipe(fixture, reviewed_fhir_source, copy_targets)
        unrelated = await _draft(fixture.engine, fixture.control_schema, _create(), _actor())
        command = await _command(fixture.connection, fixture.control_schema, unrelated)
        initial_state = await _durable_state(fixture.connection, fixture.control_schema)
        preview = await _repeatable_preview(fixture, command)
        assert preview["source_selection"] == {"status": "unavailable"}
        assert await _durable_state(fixture.connection, fixture.control_schema) == initial_state
    finally:
        await _remove_candidates(fixture, copy_targets, uuid4())


async def test_mixed_retained_recipe_preview_never_reports_partial_aca_counts(
    office_db, custom_db, reviewed_fhir_source, monkeypatch
):
    fixture = custom_db
    copy_targets = []
    actual_create = recipe_tests.create_network_candidate

    async def create_mixed(connection, target, **options):
        # Provision two actual raw pins through the ordinary candidate codec;
        # publication and manifest custody remain the real protected pipeline.
        options["source_recipes"] += (
            RegistrySourceMembershipRecipe(office_db.source, office_db.batch.binding_coordinates),
        )
        return await actual_create(connection, target, **options)

    monkeypatch.setattr(recipe_tests, "create_network_candidate", create_mixed)
    try:
        await _publish_raw_recipe(fixture, reviewed_fhir_source, copy_targets)
        unrelated = await _draft(fixture.engine, fixture.control_schema, _create(), _actor())
        command = await _command(fixture.connection, fixture.control_schema, unrelated)
        preview = await _repeatable_preview(fixture, command)
        assert preview["source_selection"] == {"status": "unavailable"}
        assert (
            await fixture.connection.fetchval(
                f'SELECT jsonb_array_length(source_recipes_json) FROM "{fixture.control_schema}".network_membership_candidate WHERE state=$1',
                "published",
            )
            == 2
        )
    finally:
        await _remove_candidates(fixture, copy_targets, uuid4())


async def test_imported_preview_checks_the_aggregate_limit(retained_aca_preview, monkeypatch):
    fixture = retained_aca_preview
    command = await _pending_aca_operation(fixture, "close")
    monkeypatch.setattr(imported_preview, "MAX_PREVIEW_COUNT", 1)
    with pytest.raises(RegistryImportedSelectionUnavailable, match="selection_unavailable"):
        await _repeatable_preview(fixture, command)
    await _assert_staging_removed(fixture.connection)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def cms_preview_template():
    """Use the shared actual CMS migration template without replacing legacy fixtures."""
    async with aclosing(cms_recipes.cms_recipe_template.__wrapped__()) as templates:
        yield await anext(templates)


async def _fixture_value(stack, generator):
    """Register exact fixture cleanup before advancing any owned resource setup."""
    await stack.enter_async_context(aclosing(generator))
    return await anext(generator)


def _expand_cms_roles(monkeypatch, count):
    """Declare all membership rows before raw artifacts, witnesses and custody are sealed."""
    original = cms_recipes._membership_resource_rows

    def resource_rows(factory, revision):
        resources_by_kind = original(factory, revision)
        role = resources_by_kind["PractitionerRole"][0]
        resources_by_kind["PractitionerRole"] = [
            {**copy.deepcopy(role), "id": "preview-role-" + str(index)} for index in range(count)
        ]
        return resources_by_kind

    monkeypatch.setattr(cms_recipes, "_membership_resource_rows", resource_rows)


def _add_aca_recipe(monkeypatch, offices):
    """Persist both actual immutable pins through ordinary candidate creation."""
    actual_create = recipe_tests.create_network_candidate

    async def create_mixed(connection, target, **options):
        options["source_recipes"] += (
            RegistrySourceMembershipRecipe(offices.source, offices.batch.binding_coordinates),
        )
        return await actual_create(connection, target, **options)

    monkeypatch.setattr(recipe_tests, "create_network_candidate", create_mixed)


@pytest.fixture
async def retained_cms_preview(cms_preview_template, monkeypatch, tmp_path, request):
    """Publish closed CMS custody and optional ACA recipes, retaining exact cleanup."""
    options_by_kind = getattr(request, "param", {})
    if options_by_kind.get("rows"):
        _expand_cms_roles(monkeypatch, options_by_kind["rows"])
    async with AsyncExitStack() as stack:
        database = await _fixture_value(stack, cms_recipes.cms_recipe_database.__wrapped__(monkeypatch))
        registry = await _fixture_value(stack, cms_recipes.serving_schema.__wrapped__(database, monkeypatch))
        fixture = await _fixture_value(stack, custom_db.__wrapped__(registry))
        if options_by_kind.get("mixed"):
            offices = await _fixture_value(stack, office_db.__wrapped__(registry, SimpleNamespace(param=2)))
            _add_aca_recipe(monkeypatch, offices)
        reviewed = await _fixture_value(
            stack, cms_recipes.reviewed_cms_source.__wrapped__(database, registry, monkeypatch, tmp_path)
        )
        copy_targets = []
        try:
            await _publish_raw_recipe(fixture, reviewed, copy_targets)
            yield fixture, reviewed
        finally:
            await _remove_candidates(fixture, copy_targets, uuid4())


async def _pending_cms_command(fixture, reviewed, operation):
    """Create pending history through the native writer, leaving approval untouched."""
    _, actor, networks, binding, _, _ = reviewed
    selected = await recipe_tests._review_source_operation(
        fixture.connection, fixture.control_schema, actor, binding, operation, networks
    )
    return await _command(fixture.connection, fixture.control_schema, selected)


@pytest.mark.parametrize("operation,mapped,omitted", [("rebind", 1, 0), ("close", 0, 1)])
async def test_cms_preview_uses_pending_bindings_with_closed_custody(retained_cms_preview, operation, mapped, omitted):
    """An exact prospective rebind or close changes counts before durable approval."""
    fixture, reviewed = retained_cms_preview
    command = await _pending_cms_command(fixture, reviewed, operation)
    initial_state = await _durable_state(fixture.connection, fixture.control_schema)
    preview = await _repeatable_preview(fixture, command)
    assert preview["source_selection"] == {
        "status": "available",
        "mapped_rows": mapped,
        "omitted_rows": omitted,
        "unresolved_rows": 0,
    }
    assert preview["unresolved_count"] == 0
    assert await _durable_state(fixture.connection, fixture.control_schema) == initial_state
    await _assert_staging_removed(fixture.connection)


@pytest.mark.parametrize("retained_cms_preview", [{"mixed": True}], indirect=True)
async def test_mixed_closed_aca_and_cms_preview_counts_complete_recipes(retained_cms_preview):
    """Both source families are counted against one prospective immutable history map."""
    fixture, reviewed = retained_cms_preview
    command = await _pending_cms_command(fixture, reviewed, "close")
    preview = await _repeatable_preview(fixture, command)
    assert preview["source_selection"] == {
        "status": "available",
        "mapped_rows": 2,
        "omitted_rows": 1,
        "unresolved_rows": 0,
    }


class _CMSCountedConnection(_CountedConnection):
    """Include native catalog set queries in the complete preview call count."""

    async def fetch(self, *arguments):
        self.statements += 1
        return await self.connection.fetch(*arguments)


@pytest.mark.parametrize("retained_cms_preview", [{}, {"rows": 5002}], indirect=True)
@pytest.mark.parametrize("operation", ["rebind", "close"])
async def test_cms_preview_counts_all_source_rows_above_both_native_page_limits(retained_cms_preview, operation):
    """Retained source facts beyond keyset and expansion pages remain fully accounted."""
    fixture, reviewed = retained_cms_preview
    command = await _pending_cms_command(fixture, reviewed, operation)
    source = reviewed[0][2]
    count = await fixture.connection.fetchval(
        "SELECT count(*) FROM mrf.provider_directory_dataset_resource WHERE dataset_id=$1 AND resource_type='PractitionerRole'",
        source.dataset_id,
    )
    counted = _CMSCountedConnection(fixture.connection)
    original_jit = await fixture.connection.fetchval("SHOW jit")
    async with fixture.connection.transaction(isolation="repeatable_read"):
        preview = await preview_registry_approval(counted, command, _actor(), control_schema=fixture.control_schema)
        assert await fixture.connection.fetchval("SHOW jit") == "off"
    assert await fixture.connection.fetchval("SHOW jit") == original_jit
    assert preview["source_selection"] == {
        "status": "available",
        "mapped_rows": count if operation == "rebind" else 0,
        "omitted_rows": count if operation == "close" else 0,
        "unresolved_rows": 0,
    }
    assert counted.statements == 26


@pytest.mark.parametrize("retained_cms_preview", [{"mixed": True}], indirect=True)
async def test_cms_custody_drift_makes_entire_mixed_preview_unavailable(retained_cms_preview):
    """A lost protected source owner cannot expose partial ACA counts as complete."""
    fixture, reviewed = retained_cms_preview
    command = await _pending_cms_command(fixture, reviewed, "close")
    owner = reviewed[0][2].custody_owner_role
    try:
        await fixture.connection.execute(f'ALTER ROLE "{owner}" LOGIN')
        preview = await _repeatable_preview(fixture, command)
        assert preview["source_selection"] == {"status": "unavailable"}
    finally:
        await fixture.connection.execute(f'ALTER ROLE "{owner}" NOLOGIN')
    await _assert_staging_removed(fixture.connection)


@pytest.mark.parametrize("changed", ["source", "expanded", "remaining_cursor"])
async def test_cms_preview_denies_unrecognized_extraction_shapes(retained_cms_preview, monkeypatch, changed):
    """Exact SQL guards deny partial aggregation when the reviewed reader changes."""
    fixture, reviewed = retained_cms_preview
    command = await _pending_cms_command(fixture, reviewed, "close")
    original = imported_preview.fhir_extraction_sql

    def changed_sql(*arguments, **options):
        sql = original(*arguments, **options)
        if changed == "source":
            return sql.replace("LIMIT $11", "LIMIT $11::bigint")
        if changed == "expanded":
            return sql.replace("LIMIT 5001", "LIMIT 5002")
        return sql.replace("FROM page", "FROM page WHERE $9::text IS NULL")

    monkeypatch.setattr(imported_preview, "fhir_extraction_sql", changed_sql)
    with pytest.raises(RegistryImportedSelectionUnavailable, match="selection_unavailable"):
        await _repeatable_preview(fixture, command)
    await _assert_staging_removed(fixture.connection)


async def test_cms_preview_cancel_preserves_caller_work_and_durable_state(retained_cms_preview, monkeypatch):
    """Cancellation unwinds preview staging without changing any accepted source or head."""
    fixture, reviewed = retained_cms_preview
    command = await _pending_cms_command(fixture, reviewed, "close")
    initial_state = await _durable_state(fixture.connection, fixture.control_schema)
    entered = asyncio.Event()
    actual_require = imported_preview._require_recipe_source

    async def blocked_require(connection, recipe):
        await actual_require(connection, recipe)
        entered.set()
        await asyncio.Future()

    monkeypatch.setattr(imported_preview, "_require_recipe_source", blocked_require)
    task = asyncio.create_task(_repeatable_preview(fixture, command))
    await asyncio.wait_for(entered.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await _durable_state(fixture.connection, fixture.control_schema) == initial_state
    await _assert_staging_removed(fixture.connection)
