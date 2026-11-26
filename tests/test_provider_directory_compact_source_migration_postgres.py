# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained observations migrate through real bounded owner transactions."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from db.plan_release_storage import storage_relation
from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_source_migration as migration
from process import provider_directory_generic_candidate as candidate_rows
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_partition_successor as partition_successor
from process.provider_directory_source_storage_binding import (
    CompactV2FHIRSourceStorage,
    NamedFHIRSourceStorage,
    SharedBodyFHIRSourceStorage,
)
from tests.test_provider_directory_compact_intake_postgres import (
    _append_checkpoint,
    _binding,
    _prepare,
    _rows,
    directory,
)
from tests.test_provider_directory_compact_intake_postgres import (
    intake_case as intake_case,
)

pytestmark = pytest.mark.asyncio


async def _legacy(case):
    case.directory.anthem = directory.anthem
    case.directory._stable_source_id = directory._stable_source_id
    case.directory._last_updated_partition_fingerprint_resource_type = (
        directory._last_updated_partition_fingerprint_resource_type
    )
    await _prepare(case)
    for family in directory.anthem.RESOURCES:
        rows = _rows(case, family, 7 if family == "PractitionerRole" else 1)
        for row in rows:
            row["payload_json"].update(numbers=[-0.0, 1.0], padding="x" * 800)
            row["payload_hash"] = hashlib.sha256(compact._json(row["payload_json"]).encode()).hexdigest()
        await _append_checkpoint(case, family, rows)
    async with source.source_transaction(case.database):
        await storage.reset_generic_source_family(case.database, case.directory, case.candidate, "PractitionerRole")
    await _append_checkpoint(case, "PractitionerRole", _rows(case, "PractitionerRole", 3))
    with pytest.raises(DBAPIError):
        await _append_checkpoint(case, "PractitionerRole", _rows(case, "PractitionerRole"), fail=True)
    binding = await _binding(case)
    return binding, await _observations(case, binding), await _checkpoints(case)


async def _observations(case, binding):
    relation = storage_relation("source_schema", binding["tables"]["resource_input"]["name"])
    records = await case.connection.fetch(f"SELECT * FROM {relation} ORDER BY batch_ordinal,row_ordinal")
    return [{**dict(row), "payload_json": compact._json(json.loads(row["payload_json"]))} for row in records]


async def _checkpoints(case):
    return await case.connection.fetch(
        "SELECT to_jsonb(c) AS checkpoint FROM source_schema.provider_directory_pagination_checkpoint c ORDER BY resource_type"
    )


async def test_companion_successor_atomically_preserves_ordinary_inputs_and_fences_rollback(intake_case):
    """Exercise the checkpoint conversion and conflict rollback with PostgreSQL."""
    case = intake_case
    await _prepare(case)
    family = "InsurancePlan"
    await _append_checkpoint(case, family, _rows(case, family))
    binding = await _binding(case)
    input_rows = await _observations(case, binding)
    connection = case.connection
    table = storage_relation("source_schema", "provider_directory_pagination_checkpoint")
    base = directory.highmark.API_ROOT + "/HM"
    cursor = base + "?_getpages=example&_getpagesoffset=500"
    predecessor = await connection.fetchrow(
        f"UPDATE {table} SET canonical_api_base=$1,next_url=$2,pages_processed=1,rows_processed=1 "
        "WHERE resource_type=$3 RETURNING *",
        base,
        cursor,
        family,
    )
    settings = directory.highmark.companion_partition_settings(base, family)
    config = directory._build_partition_config(settings)
    initialization = directory._last_updated_partition_initialization(family, config, base)
    siblings = await connection.fetch(
        f"SELECT * FROM {table} WHERE canonical_api_base<>$1 ORDER BY resource_type", base
    )

    async with connection.transaction(isolation="serializable"):
        await partition_successor.start_verified_companion_partition(
            connection, "source_schema", predecessor, initialization
        )
    original, successor = await connection.fetch(
        f"SELECT * FROM {table} WHERE canonical_api_base=$1 ORDER BY resource_type", base
    )
    assert original["state"] == "superseded" and original["next_url"] is None and original["rows_processed"] == 1
    assert json.loads(original["completeness_json"])[directory.highmark.RETIRED_CURSOR_KEY] == cursor
    assert successor["resource_type"] == initialization.checkpoint_resource_type and successor["rows_processed"] == 0
    assert successor["next_url"] == initialization.checkpoint_payload
    assert dict(original)["source_scope_hash"] == dict(successor)["source_scope_hash"]
    with pytest.raises(candidate_rows.GenericCandidateError, match="transition_companion_checkpoint_changed"):
        async with connection.transaction(isolation="serializable"):
            await partition_successor.start_verified_companion_partition(
                connection, "source_schema", predecessor, initialization
            )
    assert (
        await connection.fetch(f"SELECT * FROM {table} WHERE canonical_api_base<>$1 ORDER BY resource_type", base)
        == siblings
    )
    assert await _observations(case, binding) == input_rows


async def _migrate(case, **kwargs):
    return await migration.migrate_source_candidate(case.database, case.directory, case.candidate, **kwargs)


def _fingerprints(case, resource_type):
    return directory._partition_fingerprint_rows(
        SimpleNamespace(dataset_id=case.candidate.dataset_id),
        resource_type,
        "window",
        "c" * 64,
        {"same": "d" * 64},
        {"same": "e" * 64},
    )


async def test_partition_fingerprints_resume_with_both_proof_passes(intake_case):
    case = intake_case
    await _legacy(case)
    async with source.source_transaction(case.database):
        for number in (1, 2):
            resource_type = directory._last_updated_partition_fingerprint_resource_type("PractitionerRole", number)
            await storage.append_generic_source_batch(
                case.database, case.directory, case.candidate, _fingerprints(case, resource_type)
            )
    before = await _binding(case)
    observations, checkpoints = await _observations(case, before), await _checkpoints(case)

    async def pause_after_segment():
        progress = (await _binding(case)).get(migration.KEY)
        if progress and progress["rows"]:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _migrate(case, check_cancelled=pause_after_segment)
    pending = await _binding(case)
    assert 0 < pending[migration.KEY]["rows"] < len(observations)
    after = await _migrate(case)
    assert after["ordinal"] == before["ordinal"] and after["resets"] == before["resets"]
    assert await _observations(case, after) == observations
    assert await _checkpoints(case) == checkpoints


@pytest.mark.parametrize(
    "resource_type", ["LU:PractitionerRole:pass:0", "LU:PractitionerRole:pass:3", "LU:Specimen:pass:1"]
)
async def test_partition_fingerprints_refuse_invalid_pass_or_unselected_family(intake_case, resource_type):
    case = intake_case
    await _legacy(case)
    async with source.source_transaction(case.database):
        await storage.append_generic_source_batch(
            case.database, case.directory, case.candidate, _fingerprints(case, resource_type)
        )
    before = await _binding(case)
    observations, checkpoints = await _observations(case, before), await _checkpoints(case)
    with pytest.raises(ValueError, match="payload_conversion_input_changed"):
        await _migrate(case)
    assert await _binding(case) == before
    assert await _observations(case, before) == observations and await _checkpoints(case) == checkpoints


async def test_segmented_prefix_preserves_history(intake_case, monkeypatch):
    case = intake_case
    await _legacy(case)
    nested_values = "x" * 100
    for _ in range(180):
        nested_values = [nested_values]
    resources = _rows(case, "PractitionerRole", 3)
    for resource in resources:
        resource["payload_json"]["nested_values"] = nested_values
        resource["payload_hash"] = hashlib.sha256(compact._json(resource["payload_json"]).encode()).hexdigest()
    await _append_checkpoint(case, "PractitionerRole", resources)
    with pytest.raises(DBAPIError):
        await _append_checkpoint(case, "PractitionerRole", _rows(case, "PractitionerRole"), fail=True)
    before = await _binding(case)
    observations, checkpoints = await _observations(case, before), await _checkpoints(case)
    aliases, _, _ = compact.prepare_compact_payload_batch(
        resources, dataset_id=case.candidate.dataset_id, batch_ordinal=before["ordinal"], format_id=compact.FORMAT
    )
    read_charge = aliases[0][compact._ALIAS_COLUMNS.index("read_bytes")]
    monkeypatch.setattr(candidate_rows, "COPY_MAX_BYTES", candidate_rows.COPY_FRAME_BYTES + 2 * read_charge - 1)
    segments, read_pages = [], []
    original = migration._copy_segment
    original_read = compact.read_compact_payload_keys

    async def tracked(connection, policy, binding, keys):
        segments.append([(key["batch_ordinal"], key["row_ordinal"]) for key in keys])
        return await original(connection, policy, binding, keys)

    async def tracked_read(connection, tables, dataset_id, selected):
        read_pages.append([(key["batch_ordinal"], key["row_ordinal"]) for key in selected])
        return await original_read(connection, tables, dataset_id, selected)

    monkeypatch.setattr(migration, "_copy_segment", tracked)
    monkeypatch.setattr(compact, "read_compact_payload_keys", tracked_read)
    after = await _migrate(case)
    assert after["contract"] == storage.NAMED_COMPACT_CONTRACT
    assert after["identity"] == before["identity"] and after["ordinal"] == before["ordinal"]
    assert after["resets"] == before["resets"] and after[migration.KEY]["state"] == "retained"
    assert await _observations(case, after) == observations and await _checkpoints(case) == checkpoints
    assert len({part[0][0] for part in segments}) < len(segments)
    assert any(part[0][1] > 0 for part in segments)
    assert any(len(part) == 2 and part[0][0] == before["ordinal"] for part in segments)
    assert all(len(part) == 1 for part in read_pages if part[0][0] == before["ordinal"])
    for table in before["tables"].values():
        assert await case.connection.fetchval(
            "SELECT to_regclass($1)::text IS NOT NULL", storage_relation("source_schema", table["name"])
        )
    assert await _observations(case, before) == observations
    sequence_highwater = after[migration.KEY]["sequence"][0]
    assert sequence_highwater > before["ordinal"]
    await _append_checkpoint(case, "PractitionerRole", _rows(case, "PractitionerRole"))
    assert (await _binding(case))["ordinal"] == sequence_highwater + 1
    assert await _migrate(case) == await _binding(case)


@pytest.mark.parametrize("payload_format", [compact.FORMAT, compact.SHARED_BODY_FORMAT])
async def test_cancelled_conversion_preserves_prefix(intake_case, monkeypatch, payload_format):
    case = intake_case
    before, observations, checkpoints = await _legacy(case)
    if payload_format == compact.SHARED_BODY_FORMAT:
        with pytest.raises(ValueError, match="payload_format_changed"):
            await _migrate(case, payload_format=payload_format)
        monkeypatch.setattr(
            case.factory, "storage_binding", SharedBodyFHIRSourceStorage(**asdict(case.factory.storage_binding))
        )
    original = compact.append_compact_payload_segment

    async def interrupted(*args, **kwargs):
        await original(*args, **kwargs)
        raise asyncio.CancelledError

    with monkeypatch.context() as patch:
        patch.setattr(compact, "append_compact_payload_segment", interrupted)
        with pytest.raises(asyncio.CancelledError):
            await migration.prepare_retained_prefix(case.directory, case.candidate, SimpleNamespace(check=AsyncMock()))
    pending = await _binding(case)
    assert pending["tables"] == before["tables"] and pending[migration.KEY]["rows"] == 0
    compact_aliases = storage_relation(
        "source_schema", pending[migration.KEY]["compact_tables"]["resource_aliases"]["name"]
    )
    assert await case.connection.fetchval(f"SELECT count(*) FROM {compact_aliases}") == 0
    assert await _observations(case, before) == observations and await _checkpoints(case) == checkpoints
    after = await migration.prepare_retained_prefix(case.directory, case.candidate, SimpleNamespace(check=AsyncMock()))
    assert after["payload_format"] == payload_format
    assert await _observations(case, after) == observations
    assert await _checkpoints(case) == checkpoints
    if payload_format == compact.SHARED_BODY_FORMAT:
        with monkeypatch.context() as unconfigured:
            unconfigured.setattr(
                case.factory, "storage_binding", NamedFHIRSourceStorage(**asdict(case.factory.storage_binding))
            )
            with pytest.raises(ValueError, match="payload_format_changed"):
                await _append_checkpoint(case, "PractitionerRole", _rows(case, "PractitionerRole"))
        assert await _checkpoints(case) == checkpoints
    await _append_checkpoint(case, "PractitionerRole", _rows(case, "PractitionerRole"))
    assert (await _binding(case))["ordinal"] > after["ordinal"]


async def test_ambiguous_commit_resumes_conversion(intake_case, monkeypatch):
    case = intake_case
    _before, observations, checkpoints = await _legacy(case)
    monkeypatch.setattr(candidate_rows, "COPY_MAX_BYTES", 4096)
    original = source.source_transaction
    state_by_kind = {"has_raised": False}

    @asynccontextmanager
    async def ambiguous(database, **kwargs):
        async with original(database, **kwargs) as session:
            yield session
        progress = (await _binding(case)).get(migration.KEY)
        if progress and progress["state"] == "copying" and progress["rows"] and not state_by_kind["has_raised"]:
            state_by_kind["has_raised"] = True
            raise TimeoutError("synthetic commit acknowledgement unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(source, "source_transaction", ambiguous)
        with pytest.raises(TimeoutError, match="commit acknowledgement"):
            await _migrate(case)
    pending = await _binding(case)
    assert state_by_kind["has_raised"] and pending[migration.KEY]["rows"] > 0
    assert pending[migration.KEY]["rows"] < pending[migration.KEY]["total_rows"]
    after = await _migrate(case)
    assert after[migration.KEY]["rows"] == len(observations)
    assert await _observations(case, after) == observations and await _checkpoints(case) == checkpoints


async def test_pending_conversion_blocks_writers(intake_case):
    case = intake_case
    before, observations, checkpoints = await _legacy(case)
    cancel_checks = []

    async def pause():
        cancel_checks.append(True)
        if len(cancel_checks) == 2:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _migrate(case, check_cancelled=pause)
    pending = await _binding(case)
    for operation in (
        lambda: storage.append_generic_source_batch(
            case.database, case.directory, case.candidate, _rows(case, "PractitionerRole")
        ),
        lambda: storage.reset_generic_source_family(case.database, case.directory, case.candidate, "PractitionerRole"),
        lambda: completion.complete_generic_source_candidate(case.database, case.directory, case.candidate),
    ):
        with pytest.raises(ValueError, match="payload_conversion_incomplete"):
            async with source.source_transaction(case.database):
                await operation()
    assert await _binding(case) == pending
    assert await _observations(case, before) == observations and await _checkpoints(case) == checkpoints
    await case.connection.execute("UPDATE source_schema.provider_directory_endpoint_dataset SET status='failed'")
    async with source.source_transaction(case.database):
        await storage.discard_generic_source_inputs(case.database, case.directory, case.candidate, None)
    disposed = await _binding(case)
    assert disposed["phase"] == "disposed" and migration.KEY not in disposed
    for table in [*before["tables"].values(), *pending[migration.KEY]["compact_tables"].values()]:
        assert not await case.connection.fetchval(
            "SELECT to_regclass($1)::text IS NOT NULL", storage_relation("source_schema", table["name"])
        )
    assert await _checkpoints(case) == checkpoints


async def _transition_census(case, current):
    """Prepare real completed checkpoints for the retained active resource census."""
    relation = storage_relation("source_schema", current["tables"]["resource_input"]["name"])
    active = await case.connection.fetch(
        f"SELECT resource_type,count(*) AS count FROM {relation} WHERE batch_ordinal>"
        "greatest($1::bigint,coalesce(($2::jsonb->>resource_type)::bigint,0)) GROUP BY resource_type",
        current["resets"].get("*", 0),
        json.dumps(current["resets"]),
    )
    counts_by_resource = {observation["resource_type"]: observation["count"] for observation in active}
    await case.connection.execute(
        "CREATE TABLE source_schema.provider_directory_bulk_acquisition_checkpoint("
        "checkpoint_id text,canonical_api_base text,resource_type text,source_scope_hash text,strategy_version text,"
        "acquisition_root_run_id text,owner_run_id text,endpoint_id text,dataset_id text,start_url_hash text,"
        "manifest_hash text,manifest_json jsonb,state text,rows_written bigint,completed_at timestamp,failed_at timestamp,error text)"
    )
    for resource, count in counts_by_resource.items():
        await case.connection.execute(
            "UPDATE source_schema.provider_directory_pagination_checkpoint SET canonical_api_base=$1,"
            "source_scope_hash=$2,start_url_hash=$3,state='complete',completed_at=now(),pages_processed=1,rows_processed=$4 "
            "WHERE resource_type=$5",
            directory.anthem.API_BASE,
            "a" * 64,
            hashlib.sha256(directory.anthem.start_url(resource, 500).encode()).hexdigest(),
            count,
            resource,
        )
    identity_by_field = {
        "dataset_id": "dataset",
        "endpoint_id": "endpoint",
        "acquisition_root_run_id": "root",
        "failed_run_id": "run",
    }
    return counts_by_resource, identity_by_field


@pytest.mark.parametrize("payload_format", [compact.FORMAT, compact.SHARED_BODY_FORMAT])
async def test_successor_preserves_reset_census(intake_case, monkeypatch, payload_format):
    case = intake_case
    _before, _observed, checkpoints = await _legacy(case)
    if payload_format == compact.SHARED_BODY_FORMAT:
        monkeypatch.setattr(
            case.factory, "storage_binding", SharedBodyFHIRSourceStorage(**asdict(case.factory.storage_binding))
        )
    current = await _migrate(case, payload_format=payload_format)
    old_storage_by_field = {**current["storage"], "producer": ["a" * 40, "sha256:" + "b" * 64]}
    new_storage_by_field = {**current["storage"], "producer": ["c" * 40, "sha256:" + "d" * 64]}
    _raw, transitioned = storage._transition_candidate_document(current, old_storage_by_field, new_storage_by_field)
    assert transitioned == current
    counts_by_resource, identity_by_field = await _transition_census(case, current)
    stopped_run_by_field = {"status": "failed", "ancestor_run_ids": ["root"]}
    async with source.source_transaction(case.database):
        publisher = source.bound_publisher()
        await storage._verify_transition_inputs(
            publisher.connection, publisher.policy, current, identity_by_field, counts_by_resource, stopped_run_by_field
        )
        corrupt_binding_by_field = {**current, "resets": {"PractitionerRole": 0}}
        with pytest.raises(ValueError, match="transition_input_changed"):
            await storage._verify_transition_inputs(
                publisher.connection,
                publisher.policy,
                corrupt_binding_by_field,
                identity_by_field,
                counts_by_resource,
                stopped_run_by_field,
            )
        aliases = storage_relation("source_schema", current["tables"]["resource_aliases"]["name"])
        await publisher.connection.execute(
            f"UPDATE {aliases} SET batch_ordinal=$1 WHERE resource_type='Location'", current["ordinal"] + 1
        )
        with pytest.raises(ValueError, match="transition_input_changed|compact_alias_changed"):
            await storage._verify_transition_inputs(
                publisher.connection,
                publisher.policy,
                current,
                identity_by_field,
                counts_by_resource,
                stopped_run_by_field,
            )
    # This data-only successor check does not issue a manifest or authenticate a run.
    assert len(checkpoints) == 7


async def test_successful_release_rolls_back_with_handoff(intake_case):
    from tests.test_provider_directory_compact_completion_postgres import _append, _candidate, _complete, _payloads

    case = await _candidate(intake_case, "release")
    case.directory.anthem = directory.anthem
    case.directory._stable_source_id = directory._stable_source_id
    case.directory._last_updated_partition_fingerprint_resource_type = (
        directory._last_updated_partition_fingerprint_resource_type
    )
    original = await _prepare(case)
    await _append(case, _payloads())
    converted = await _migrate(case, payload_format=compact.FORMAT)
    with pytest.raises(ValueError):
        async with source.source_transaction(case.database):
            publisher = source.bound_publisher()
            await migration.release_conversion_inputs(publisher.connection, publisher.policy, converted)
    prepared = await _complete(case)
    observed = await _observations(case, prepared)
    with pytest.raises(RuntimeError, match="later handoff failure"):
        async with source.source_transaction(case.database):
            publisher = source.bound_publisher()
            released = await migration.release_conversion_inputs(publisher.connection, publisher.policy, prepared)
            assert migration.KEY not in released
            for table in original["tables"].values():
                assert not await publisher.connection.fetchval(
                    "SELECT to_regclass($1)::text IS NOT NULL", storage_relation("source_schema", table["name"])
                )
            raise RuntimeError("synthetic later handoff failure")
    assert await _binding_for(case) == prepared
    for table in original["tables"].values():
        assert await case.connection.fetchval(
            "SELECT to_regclass($1)::text IS NOT NULL", storage_relation("source_schema", table["name"])
        )
    async with source.source_transaction(case.database):
        publisher = source.bound_publisher()
        released = await migration.release_conversion_inputs(publisher.connection, publisher.policy, prepared)
    assert migration.KEY not in released and await _binding_for(case) == released
    assert await _observations(case, released) == observed
    for table in original["tables"].values():
        assert not await case.connection.fetchval(
            "SELECT to_regclass($1)::text IS NOT NULL", storage_relation("source_schema", table["name"])
        )


async def test_shared_body_conversion_atomically_reclaims_legacy_tables_on_finish(intake_case, monkeypatch):
    from tests.test_provider_directory_compact_completion_postgres import _append, _candidate, _complete, _payloads

    case = await _candidate(intake_case, "shared_release")
    case.directory.anthem = directory.anthem
    case.directory._stable_source_id = directory._stable_source_id
    case.directory._last_updated_partition_fingerprint_resource_type = (
        directory._last_updated_partition_fingerprint_resource_type
    )
    original = await _prepare(case)
    await _append(case, _payloads())
    observed_before = await _observations(case, original)
    monkeypatch.setattr(
        case.factory, "storage_binding", SharedBodyFHIRSourceStorage(**asdict(case.factory.storage_binding))
    )
    original_store = storage._store
    failure_by_stage = {"should_fail": True}

    async def fail_finish_store(connection, policy, identity, previous, binding_by_field, **kwargs):
        if failure_by_stage["should_fail"] and binding_by_field["contract"] == storage.NAMED_COMPACT_CONTRACT:
            failure_by_stage["should_fail"] = False
            raise RuntimeError("synthetic finish commit failure")
        return await original_store(connection, policy, identity, previous, binding_by_field, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(storage, "_store", fail_finish_store)
        with pytest.raises(RuntimeError, match="synthetic finish commit failure"):
            await _migrate(case, payload_format=compact.SHARED_BODY_FORMAT)
    pending = await _binding_for(case)
    assert pending["contract"] == storage.NAMED_ISOLATED_CONTRACT and pending[migration.KEY]["state"] == "copying"
    for table in original["tables"].values():
        assert await case.connection.fetchval(
            "SELECT to_regclass($1)::text IS NOT NULL", storage_relation("source_schema", table["name"])
        )
    converted = await _migrate(case, payload_format=compact.SHARED_BODY_FORMAT)
    assert converted["contract"] == storage.NAMED_COMPACT_CONTRACT and migration.KEY not in converted
    assert await _binding_for(case) == converted
    for table in original["tables"].values():
        assert not await case.connection.fetchval(
            "SELECT to_regclass($1)::text IS NOT NULL", storage_relation("source_schema", table["name"])
        )
    assert await _observations(case, converted) == observed_before
    tokens_table = storage_relation("source_schema", converted["tables"]["resource_reference_tokens"]["name"])
    assert await case.connection.fetchval(f"SELECT count(*) FROM {tokens_table}") == 1
    aliases_name = converted["tables"]["resource_aliases"]["name"]
    index_count = await case.connection.fetchval(
        "SELECT count(*) FROM pg_indexes WHERE schemaname='source_schema' AND tablename=$1", aliases_name
    )
    assert index_count == 3
    prepared = await _complete(case)
    assert await _observations(case, prepared) == observed_before
    await case.connection.execute(f"UPDATE {tokens_table} SET reference_text=reference_text||'-tampered'")
    with pytest.raises(ValueError, match="compact_shared_body_changed"):
        await _complete(case)


async def test_compact_v2_conversion_reclaims_legacy_tables_and_completes(intake_case, monkeypatch):
    from tests.test_provider_directory_compact_completion_postgres import _append, _candidate, _complete, _payloads

    case = await _candidate(intake_case, "compact_v2_release")
    case.directory.anthem = directory.anthem
    case.directory._stable_source_id = directory._stable_source_id
    case.directory._last_updated_partition_fingerprint_resource_type = (
        directory._last_updated_partition_fingerprint_resource_type
    )
    original = await _prepare(case)
    await _append(case, _payloads())
    observed_before = await _observations(case, original)
    monkeypatch.setattr(
        case.factory, "storage_binding", CompactV2FHIRSourceStorage(**asdict(case.factory.storage_binding))
    )
    converted = await _migrate(case, payload_format=compact.COMPACT_V2_FORMAT)
    assert converted["contract"] == storage.NAMED_COMPACT_CONTRACT and migration.KEY not in converted
    assert converted["payload_format"] == compact.COMPACT_V2_FORMAT
    for table in original["tables"].values():
        assert not await case.connection.fetchval(
            "SELECT to_regclass($1)::text IS NOT NULL", storage_relation("source_schema", table["name"])
        )
    assert await _observations(case, converted) == observed_before
    # Append a fresh live batch directly into compact-v2 after migration, then complete candidate
    await _append(case, _payloads())
    observed_after_live = await _observations(case, await _binding_for(case))
    assert len(observed_after_live) == len(observed_before) * 2
    prepared = await _complete(case)
    assert await _observations(case, prepared) == observed_after_live
    tokens_table = storage_relation("source_schema", converted["tables"]["resource_reference_tokens"]["name"])
    await case.connection.execute(f"UPDATE {tokens_table} SET reference_text=reference_text||'-tampered'")
    with pytest.raises(ValueError, match="compact_reference_changed"):
        await _complete(case)


async def test_shared_body_chunked_validation(intake_case, monkeypatch):
    """Verify keyset-chunked shared-body validation covers every chunk and rejects tampered hashes."""
    from process import provider_directory_compact_source_completion as completion
    from tests.test_provider_directory_compact_completion_postgres import _append, _candidate, _complete, _payloads

    case = await _candidate(intake_case, "shared_chunked_release")
    case.directory.anthem = directory.anthem
    case.directory._stable_source_id = directory._stable_source_id
    case.directory._last_updated_partition_fingerprint_resource_type = (
        directory._last_updated_partition_fingerprint_resource_type
    )
    monkeypatch.setattr(completion, "_SHARED_BODY_VALIDATION_CHUNK_ROWS", 1)
    monkeypatch.setattr(
        case.factory,
        "storage_binding",
        SharedBodyFHIRSourceStorage(**asdict(case.factory.storage_binding)),
    )
    await _prepare(case)
    await _append(case, _payloads())
    converted = await _migrate(case, payload_format=compact.SHARED_BODY_FORMAT)
    assert converted["payload_format"] == compact.SHARED_BODY_FORMAT
    shared_table = storage_relation("source_schema", converted["tables"]["resource_shared_bodies"]["name"])
    last_hash = await case.connection.fetchval(f"SELECT max(body_hash) FROM {shared_table}")
    await case.connection.execute(
        f"UPDATE {shared_table} SET payload='{{}}'::json WHERE body_hash=$1",
        last_hash,
    )
    with pytest.raises(ValueError, match="compact_shared_body_changed"):
        await _complete(case)


@pytest.mark.parametrize("column", ["network_set_id", "service_set_id"])
async def test_compact_v2_swapped_set_rejected(intake_case, monkeypatch, column):
    """Fail compact-v2 finish before dropping legacy tables if aliases swap valid ref sets."""
    from tests.test_provider_directory_compact_completion_postgres import _append, _candidate, _payloads

    case = await _candidate(intake_case, f"compact_v2_swapped_{column}")
    case.directory.anthem = directory.anthem
    case.directory._stable_source_id = directory._stable_source_id
    case.directory._last_updated_partition_fingerprint_resource_type = (
        directory._last_updated_partition_fingerprint_resource_type
    )
    original = await _prepare(case)
    await _append(case, _payloads())
    second_by_family = _payloads()
    second_by_family["PractitionerRole"].update(
        resource_id="role-b",
        network_refs=["Organization/network-two"],
        healthcare_service_refs=["HealthcareService/" + "b" * 32],
    )
    await _append(case, second_by_family)
    monkeypatch.setattr(
        case.factory, "storage_binding", CompactV2FHIRSourceStorage(**asdict(case.factory.storage_binding))
    )
    original_finish = migration._finish

    async def swap_sets_before_finish(connection, policy, binding):
        aliases_rel = storage_relation(
            "source_schema", binding[migration.KEY]["compact_tables"]["resource_aliases"]["name"]
        )
        alias_entries = await connection.fetch(
            f"SELECT batch_ordinal,row_ordinal,{column} FROM {aliases_rel} "
            "WHERE resource_type='PractitionerRole' ORDER BY batch_ordinal,row_ordinal"
        )
        assert len(alias_entries) == 2 and alias_entries[0][column] != alias_entries[1][column]
        await connection.execute(
            f"UPDATE {aliases_rel} SET {column}=$1 WHERE batch_ordinal=$2 AND row_ordinal=$3",
            alias_entries[1][column],
            alias_entries[0]["batch_ordinal"],
            alias_entries[0]["row_ordinal"],
        )
        await connection.execute(
            f"UPDATE {aliases_rel} SET {column}=$1 WHERE batch_ordinal=$2 AND row_ordinal=$3",
            alias_entries[0][column],
            alias_entries[1]["batch_ordinal"],
            alias_entries[1]["row_ordinal"],
        )
        return await original_finish(connection, policy, binding)

    monkeypatch.setattr(migration, "_LEGACY_VERIFY_SAMPLE_ROWS", 0)
    monkeypatch.setattr(migration, "_finish", swap_sets_before_finish)
    with pytest.raises(ValueError, match="payload_conversion_data_changed"):
        await _migrate(case, payload_format=compact.COMPACT_V2_FORMAT)
    legacy_rel = storage_relation("source_schema", original["tables"]["resource_input"]["name"])
    assert await case.connection.fetchval("SELECT to_regclass($1)::text IS NOT NULL", legacy_rel)


async def _binding_for(case):
    return json.loads(
        await case.connection.fetchval(
            "SELECT candidate_storage_json::text FROM source_schema.provider_directory_endpoint_dataset WHERE dataset_id=$1",
            case.candidate.dataset_id,
        )
    )
