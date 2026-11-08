# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable old-header provenance and retained heaps share one ordinary inverse."""

import asyncio
import contextvars
import importlib
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_published_replacement_inputs as serving_inputs
from process import provider_directory_published_replacement_journal as journal
from process import provider_directory_published_source_replacement as replacement
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case
from tests.test_provider_directory_published_replacement_postgres import _alias, _apply_drift, _markers, _prepare
from tests.test_provider_directory_serving_recovery_postgres import _publish_address, _state


class _PreparedAddressStage:
    """Require one genuine secondary index on the synthetic prepared heap."""

    __tablename__ = "entity_address_unified_stage"
    __my_additional_indexes__ = [{"name": "marker", "index_elements": ["marker"]}]


async def _prepared_address(case, fence, stage):
    from process import entity_address_candidate_preparation as preparation

    native = importlib.import_module("process.entity_address_unified")
    database, schema = case.database, case.schema
    await database.status(f'CREATE TABLE "{schema}".entity_address_unified_stage (marker text PRIMARY KEY)')
    await database.status(f"INSERT INTO \"{schema}\".entity_address_unified_stage VALUES ('new')")
    index_name = native._stage_index_name(_PreparedAddressStage.__tablename__, "marker")
    await database.status(f'CREATE INDEX "{index_name}" ON "{schema}".entity_address_unified_stage(marker)')
    assert await database.scalar(native._record_geo_assurance_candidate_sql(schema, "entity_address_unified_stage", 1))
    dataset = fence.datasets[0]
    snapshot = await serving_inputs.capture(case.directory, fence)
    inputs = preparation.ProviderDirectoryAddressPreparationInput(
        dataset_pins=serving_inputs.desired_pins(snapshot),
        overlay_table=stage.stage_table,
        overlay_relation_oid=await database.scalar(f"SELECT '{schema}.{stage.stage_table}'::regclass::oid::bigint"),
        address_alias_generation=0,
        ordinary_serving_peer_pins=serving_inputs.peer_pins(snapshot),
    )
    context_by_field = dict(
        address_alias_generation=0,
        publish_validation={},
        staged_rows=1,
        partial_provider_directory_source_ids=[dataset.source_id],
        partial_provider_directory_dataset_id=dataset.dataset_id,
        partial_provider_directory_run_id=dataset.evidence_run_id,
        partial_provider_directory_scope="latest-run",
    )
    token = preparation._PREPARATION.set(inputs)
    try:
        await preparation.capture_overlay_fence(schema, context_by_field)
        return await preparation.prepare_finalized_generation(
            schema, _PreparedAddressStage, {}, context=context_by_field
        )
    finally:
        preparation._PREPARATION.reset(token)


async def _forward(case, fence, generation):
    database, schema, directory = case.database, case.schema, case.directory
    await database.status(f'CREATE TABLE "{schema}".overlay_next (marker text PRIMARY KEY)')
    await database.status(f"INSERT INTO \"{schema}\".overlay_next VALUES ('new')")
    request = SimpleNamespace(
        metrics=_markers(),
        publish_artifacts_targets={"address_overlay"},
        publish_corroboration=False,
        run_id=None,
    )
    recovery_by_field = dict(
        generation=generation,
        source=case.pin,
        run_id=None,
        stage=SimpleNamespace(schema=schema, stage_table="overlay_next", target_relation=receipts.ROOTS[0]),
        serving_inputs=await serving_inputs.capture(directory, fence),
    )
    admission = artifacts._publication_admission(directory, fence, request, {"Location"})
    token = artifacts._ACTIVE.set(replace(admission, recovery=recovery_by_field))
    try:
        async with database.transaction():
            await serving_inputs.lock_and_check(directory, recovery_by_field["serving_inputs"])
            await directory._lock_and_verify_artifact_dataset_fence(fence, database)
            await recovery.begin_overlay(directory, recovery_by_field)
            await receipts.lock(database, schema, (*receipts.ROOTS, "overlay_next"))
            await receipts.retain(database, schema, recovery_by_field["before"]["roots"][receipts.ROOTS[0]])
            await receipts.rename(database, schema, "overlay_next", receipts.ROOTS[0])
            await directory._cutover_provider_directory_artifact_sources(fence)
            await recovery.finish_overlay(directory, recovery_by_field)
            await _publish_address(case, generation)
    finally:
        artifacts._ACTIVE.reset(token)


@pytest.mark.asyncio
async def test_native_replacement_journal_replays_restores_old_alias_and_exact_heaps(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        await _prepare(case.database, case.schema)
        baseline = await _state(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            dataset = fence.datasets[0]
            case.pin = dict(
                source_id=dataset.source_id,
                endpoint_id=dataset.endpoint_id,
                dataset_id=dataset.dataset_id,
                dataset_hash=dataset.dataset_hash,
                parent_run_id=dataset.evidence_run_id,
            )
            case.fence = fence
            generation = journal.generation(case.pin)
            await _forward(case, fence, generation)
        assert await _alias(case.database, case.schema) == "endpoint_shared"
        forward = await _state(case)
        async with case.database.transaction():
            history = await recovery._load_history(case.database, case.schema, generation)
        assert set(history) == {"overlay", "published"}
        binding = history["overlay"]["document"]["preparation"][journal.FIELD]
        assert binding["incumbent"]["dataset_id"] == "dataset_serving"
        assert binding["incumbent"]["dataset_hash"] is None
        assert binding["publication"]["dataset_id"] == "dataset_shared"
        assert history["published"]["document"]["preparation"][journal.FIELD] == binding
        journal.validate(binding, case.pin, before=baseline)
        replay = await artifacts.replay_published_replacement(case.directory, _markers())
        assert replay["serving_recovery"] == {"generation": generation, "phase": "published"}
        assert await _state(case) == forward
        recovery_request_by_field = {
            recovery.OPERATION_PARAM: "recover",
            recovery.GENERATION_PARAM: generation,
            "source_ids": [case.pin["source_id"]],
        }
        # The inverse is deliberately a new recovery_request_by_field, without the ephemeral preparation capture.
        outcome = await asyncio.create_task(recovery._recover(case.directory, recovery_request_by_field))
        assert outcome["serving_recovery"]["current_acquired"] == case.pin
        assert await _alias(case.database, case.schema) == "endpoint_repoint"
        restored = await _state(case)
        assert restored["roots"] == baseline["roots"]
        assert (
            restored["metadata"]["address"]["local_generation"]
            == forward["metadata"]["address"]["local_generation"] + 1
        )
        assert await asyncio.create_task(recovery._recover(case.directory, recovery_request_by_field)) == outcome
        assert await _state(case) == restored
        async with case.database.transaction():
            history = await recovery._load_history(case.database, case.schema, generation)
        assert history["recovered"]["document"]["preparation"][journal.FIELD] == binding


async def _published(case):
    await _prepare(case.database, case.schema)
    async with replacement.preparation_scope(case.directory, _markers()) as fence:
        item = fence.datasets[0]
        case.pin = dict(
            source_id=item.source_id,
            endpoint_id=item.endpoint_id,
            dataset_id=item.dataset_id,
            dataset_hash=item.dataset_hash,
            parent_run_id=item.evidence_run_id,
        )
        generation = journal.generation(case.pin)
        await _forward(case, fence, generation)
    return {
        recovery.OPERATION_PARAM: "recover",
        recovery.GENERATION_PARAM: generation,
        "source_ids": [case.pin["source_id"]],
    }


@pytest.mark.parametrize(
    "damage", ["configured", "incumbent", "incumbent_metadata", "newer", "same_endpoint", "profile"]
)
async def test_native_persisted_replacement_refuses_current_drift_without_output_changes(monkeypatch, damage):
    async with ordinary_serving_case(monkeypatch) as case:
        task = await _published(case)
        forward = await _state(case)
        if damage == "profile":
            await case.database.status(
                f"CREATE TABLE {case.schema}.provider_directory_profile_serving_generation "
                "(singleton_key text PRIMARY KEY,generation bigint)"
            )
            await case.database.status(
                f"INSERT INTO {case.schema}.provider_directory_profile_serving_generation VALUES ('global',1)"
            )
        else:
            await _apply_drift(case.database, case.schema, damage)
        with pytest.raises((ValueError, RuntimeError), match="changed|newer_publication"):
            await recovery._recover(case.directory, task)
        assert (await _state(case))["roots"] == forward["roots"]
        assert await _alias(case.database, case.schema) == "endpoint_shared"


async def test_native_inverse_rollback_and_lost_ack_replay(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        task = await _published(case)
        forward = await _state(case)
        append = recovery._append

        async def fail_after_inverse(*args, **kwargs):
            await append(*args, **kwargs)
            if args[3] == "recovered":
                raise RuntimeError("after alias restore")

        with monkeypatch.context() as failure:
            failure.setattr(recovery, "_append", fail_after_inverse)
            with pytest.raises(RuntimeError, match="after alias restore"):
                await recovery._recover(case.directory, task)
        assert await _alias(case.database, case.schema) == "endpoint_shared"
        assert await _state(case) == forward
        once = recovery._recover_once

        async def lose_ack(*args):
            await once(*args)
            raise ConnectionError("lost inverse acknowledgment")

        with monkeypatch.context() as lost:
            lost.setattr(recovery, "_recover_once", lose_ack)
            result = await recovery._recover(case.directory, task)
        assert result == await recovery._recover(case.directory, task)
        assert await _alias(case.database, case.schema) == "endpoint_repoint"


@pytest.mark.parametrize("damage", ["version", "incumbent", "outputs", "missing"])
async def test_native_history_binding_cannot_be_removed_or_transplanted(monkeypatch, damage):
    async with ordinary_serving_case(monkeypatch) as case:
        task = await _published(case)
        field = journal.FIELD
        path = "{preparation," + field
        if damage == "version":
            expression = f"jsonb_set(document,'{path},version}}','1')"
        elif damage == "incumbent":
            expression = f"jsonb_set(document,'{path},incumbent,dataset_id}}','\"other\"')"
        elif damage == "outputs":
            expression = f"jsonb_set(document,'{path},retained_outputs,entity_address_unified,oid}}','1')"
        else:
            expression = f"document #- '{path}}}'"
        await case.database.status(
            f"UPDATE {case.schema}.{receipts.JOURNAL} SET document={expression},document_sha256="
            f"encode(sha256(convert_to(({expression})::text,'UTF8')),'hex') WHERE phase='published'"
        )
        refusal = "journal_generation_changed" if damage == "missing" else "replacement_"
        with pytest.raises(ValueError, match=refusal):
            async with case.database.transaction():
                await recovery._load_history(case.database, case.schema, task[recovery.GENERATION_PARAM])


async def _coupled_preparation(case, fence):
    database, directory, schema = case.database, case.directory, case.schema
    await database.status(f'CREATE TABLE "{schema}".overlay_next (marker text PRIMARY KEY)')
    await database.status(f"INSERT INTO \"{schema}\".overlay_next VALUES ('new')")

    async def rename_indexes(_schema, _stage):
        await database.status(
            f'ALTER INDEX "{_schema}"."{_stage}_pkey" RENAME TO provider_directory_address_overlay_pkey'
        )

    stage = directory.ProviderDirectoryPreparedArtifactStage(schema, "overlay_next", receipts.ROOTS[0], rename_indexes)
    bundle = directory.ProviderDirectoryArtifactBundle(stages=[stage])
    address = await _prepared_address(case, fence, stage)
    pin = next(pin for pin in address.preparation_input.dataset_pins if pin.source_id == fence.datasets[0].source_id)
    case.pin = dict(
        source_id=pin.source_id,
        endpoint_id=pin.endpoint_id,
        dataset_id=pin.dataset_id,
        dataset_hash=pin.dataset_hash,
        parent_run_id=pin.acquisition_root_run_id,
    )
    generation = journal.generation(case.pin)
    request = SimpleNamespace(
        metrics=_markers(),
        publish_artifacts_targets={"address_overlay"},
        publish_corroboration=False,
        run_id=None,
    )
    admission = artifacts._publication_admission(directory, fence, request, {"Location"})
    recovery_by_field = dict(
        generation=generation,
        source=case.pin,
        run_id=None,
        stage=stage,
        bundle=bundle,
        serving_inputs=await serving_inputs.capture(directory, fence),
    )
    return bundle, address, generation, admission, recovery_by_field


def _fail_after_coupled_apply(monkeypatch, directory, fence):
    from process import provider_directory_artifact_bundle_preparation as bundle_preparation

    apply_bundle = bundle_preparation.apply_prepared_artifact_bundle

    async def fail_after_apply(*args, **kwargs):
        assert artifacts.is_fence_held(directory, fence, directory.db)
        await apply_bundle(*args, **kwargs)
        raise RuntimeError("after coupled apply")

    monkeypatch.setattr(bundle_preparation, "apply_prepared_artifact_bundle", fail_after_apply)


async def _execute_coupled(case, fence, bundle, address, admission, recovery_by_field, monkeypatch, abort):
    """Inject a failure only after the genuine native Address publisher has run."""
    from process import entity_address_candidate_preparation as preparation

    directory = case.directory
    generation = recovery_by_field["generation"]
    prepare_relation = receipts.prepare_relation

    async def require_reader_compatible_hash(database, schema, name):
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND granted "
            "AND mode='ShareLock' AND relation=ANY(ARRAY[to_regclass(:source),to_regclass(:datasets)]))",
            source=f"{schema}.provider_directory_source",
            datasets=f"{schema}.provider_directory_endpoint_dataset",
        )
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND granted "
            "AND mode='AccessExclusiveLock' AND relation=ANY(ARRAY["
            "to_regclass(:overlay),to_regclass(:address)]))",
            overlay=f"{schema}.{receipts.ROOTS[0]}",
            address=f"{schema}.{receipts.ROOTS[1]}",
        )
        return await prepare_relation(database, schema, name)

    monkeypatch.setattr(receipts, "prepare_relation", require_reader_compatible_hash)
    prepare_address = recovery.prepare_address

    async def retain_cutover_deadline(database, schema, swaps, context):
        original_deadline = context["serving_recovery_wall"].when()
        await prepare_address(database, schema, swaps, context)
        assert context["serving_recovery_wall"].when() <= original_deadline

    monkeypatch.setattr(recovery, "prepare_address", retain_cutover_deadline)
    token = artifacts._ACTIVE.set(replace(admission, recovery=recovery_by_field))
    fence_token = directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(fence)
    try:
        if abort == "apply":
            _fail_after_coupled_apply(monkeypatch, directory, fence)
            with pytest.raises(RuntimeError, match="after coupled apply"):
                await artifacts.publish_prepared_replacement(directory, fence, bundle, address)
        elif abort:
            publish = preparation.publish_prepared_entity_address_generation

            async def fail_after_address(*args, **kwargs):
                await publish(*args, **kwargs)
                raise RuntimeError("after coupled Address")

            monkeypatch.setattr(preparation, "publish_prepared_entity_address_generation", fail_after_address)
            with pytest.raises(RuntimeError, match="after coupled Address"):
                await artifacts.publish_prepared_replacement(directory, fence, bundle, address)
        else:
            outcome = await artifacts.publish_prepared_replacement(directory, fence, bundle, address)
            assert outcome["serving_recovery"] == {"generation": generation, "phase": "published"}
    finally:
        assert artifacts._TERMINAL.get() is None
        assert recovery._COUPLED_ADDRESS.get() is None
        directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)
        artifacts._ACTIVE.reset(token)


@pytest.mark.parametrize("abort", [False, True, "apply"])
async def test_native_prepared_owner_commits_overlay_address_alias_and_journal_together(monkeypatch, abort):
    """Real prepared-stage publisher/authority; marker rows do not claim full Address recipe parity."""
    native = importlib.import_module("process.entity_address_unified")
    async with ordinary_serving_case(monkeypatch) as case:
        database, directory, schema = case.database, case.directory, case.schema
        monkeypatch.setattr(native, "db", database)
        await _prepare(database, schema)
        baseline = await _state(case)
        async with replacement.preparation_scope(directory, _markers()) as fence:
            bundle, address, generation, admission, recovery_by_field = await _coupled_preparation(case, fence)
            await _execute_coupled(case, fence, bundle, address, admission, recovery_by_field, monkeypatch, abort)
        if abort:
            assert await _state(case) == baseline
            assert await _alias(database, schema) == "endpoint_repoint"
            assert not address.committed and not bundle.promoted
        else:
            assert address.committed and bundle.promoted
            assert await _alias(database, schema) == "endpoint_shared"
            async with database.transaction():
                history = await recovery._load_history(database, schema, generation)
            assert set(history) == {"overlay", "published"}
            assert history["overlay"]["document"]["writer"] == history["published"]["document"]["writer"]
            recovery_request_by_field = {
                recovery.OPERATION_PARAM: "recover",
                recovery.GENERATION_PARAM: generation,
                "source_ids": [case.pin["source_id"]],
            }
            await recovery._recover(directory, recovery_request_by_field)
            assert (await _state(case))["roots"] == baseline["roots"]
            assert await _alias(database, schema) == "endpoint_repoint"


async def _reject_coupled_session(case, address):
    """Use a genuine second ordinary transaction while retaining the original publication task."""
    from db.connection import _SESSION, _TRANSACTION

    token = _TRANSACTION.set(())
    session_token = _SESSION.set(None)
    try:
        async with case.database.session_factory() as session, session.begin():
            async with case.database.bind_existing_session(session):
                await recovery.prepare_address(case.database, case.schema, address.swaps, address.context)
    finally:
        _SESSION.reset(session_token)
        _TRANSACTION.reset(token)


async def _reject_coupled_context(case, address, stale):
    """Exercise the real scope guard without substituting its actor or native proofs."""
    context = address.context
    if stale == "session":
        await _reject_coupled_session(case, address)
        return
    if stale == "task":
        await asyncio.create_task(recovery.prepare_address(case.database, case.schema, address.swaps, context))
        return
    if stale == "admission":
        admission = artifacts.current(case.directory)
        token = artifacts._ACTIVE.set(replace(admission, recovery=dict(admission.recovery)))
        try:
            await recovery.prepare_address(case.database, case.schema, address.swaps, context)
        finally:
            artifacts._ACTIVE.reset(token)
        return
    if stale == "context":
        await recovery.prepare_address(case.database, case.schema, address.swaps, dict(context))
        return
    key = recovery.GENERATION_PARAM if stale == "generation" else "serving_recovery_prepared_address"
    original = context[key]
    context[key] = recovery.publication_generation(case.pin) if stale == "generation" else dict(original)
    try:
        await recovery.prepare_address(case.database, case.schema, address.swaps, context)
    finally:
        context[key] = original


@pytest.mark.parametrize("stale", ["admission", "session", "task", "generation", "context", "prepared"])
async def test_native_coupled_preparation_refuses_transplanted_writer_or_context(monkeypatch, stale):
    """A real inserted overlay cannot authorize another session, task, admission or prepared context."""
    from process import entity_address_candidate_preparation as preparation

    native = importlib.import_module("process.entity_address_unified")
    async with ordinary_serving_case(monkeypatch) as case:
        monkeypatch.setattr(native, "db", case.database)
        monkeypatch.setenv("HLTHPRT_DB_DATABASE", case.database.engine.url.database)
        await _prepare(case.database, case.schema)
        baseline = await _state(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            bundle, address, generation, admission, selected = await _coupled_preparation(case, fence)

            async def refuse_prepared_publish(actual_address, *, callbacks):
                assert actual_address is address and callbacks is not None
                state = recovery._COUPLED_ADDRESS.get()
                assert state is not None and type(state["document"]) is str
                original_deadline = address.context["serving_recovery_wall"].when()
                with pytest.raises(ValueError, match="coupled_preparation_changed"):
                    await _reject_coupled_context(case, address, stale)
                assert recovery._COUPLED_ADDRESS.get() is state
                assert address.context["serving_recovery_wall"].when() == original_deadline
                raise RuntimeError("after coupled refusal")

            monkeypatch.setattr(preparation, "publish_prepared_entity_address_generation", refuse_prepared_publish)
            token = artifacts._ACTIVE.set(replace(admission, recovery=selected))
            fence_token = case.directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(fence)
            try:
                with pytest.raises(RuntimeError, match="after coupled refusal"):
                    await artifacts.publish_prepared_replacement(case.directory, fence, bundle, address)
            finally:
                assert recovery._COUPLED_ADDRESS.get() is None
                assert artifacts._TERMINAL.get() is None
                case.directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)
                artifacts._ACTIVE.reset(token)
        assert await _state(case) == baseline
        assert await _alias(case.database, case.schema) == "endpoint_repoint"
        assert not address.committed and not bundle.promoted
        async with case.database.transaction():
            assert not await recovery._load_history(case.database, case.schema, generation)


async def _wait_for_advisory_waiter(database, backend_pid):
    """Observe the real blocked lock request before probing Source lock availability."""
    async with asyncio.timeout(2):
        while not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=:pid AND locktype='advisory' AND NOT granted)",
            pid=backend_pid,
        ):
            await asyncio.sleep(0.01)


async def test_native_recovery_waits_for_endpoints_before_locking_source(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        task = await _published(case)
        async with case.database.transaction():
            history = await recovery._load_history(case.database, case.schema, task[recovery.GENERATION_PARAM])
        binding = history["overlay"]["document"]["preparation"][journal.FIELD]
        started = asyncio.Event()
        contender_pids = []

        async def contender():
            async with case.database.transaction():
                await case.database.status("SET LOCAL lock_timeout='3s'")
                contender_pids.append(await case.database.scalar("SELECT pg_backend_pid()"))
                started.set()
                await journal.check(case.directory, case.pin, binding)

        pending = None
        try:
            async with case.database.transaction():
                endpoints = SimpleNamespace(
                    endpoint_ids=sorted({binding[key]["endpoint_id"] for key in ("incumbent", "publication")})
                )
                await case.directory._lock_artifact_fence_endpoint_advisories(endpoints, case.database)
                pending = asyncio.create_task(contender(), context=contextvars.Context())
                await asyncio.wait_for(started.wait(), timeout=2)
                await _wait_for_advisory_waiter(case.database, contender_pids[0])
                await case.database.all(
                    f"SELECT source_id FROM {case.schema}.provider_directory_source "
                    "WHERE source_id=:source FOR SHARE NOWAIT",
                    source=case.pin["source_id"],
                )
            await asyncio.wait_for(pending, timeout=3)
        finally:
            if pending is not None:
                if not pending.done():
                    pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)


async def _damage_prepared_bundle(case, native, bundle, damage):
    """Change one real prepared identity or secondary index before guarded publication."""
    match damage:
        case "extra_stage":
            bundle.stages.append(bundle.stages[0])
        case "another_bundle":
            bundle = case.directory.ProviderDirectoryArtifactBundle(stages=list(bundle.stages))
        case "promoted":
            bundle.promoted = True
        case "profile":
            bundle.profile_delta = object()
        case "archive":
            bundle.archive_delta = object()
        case "missing_index":
            name = native._stage_index_name(_PreparedAddressStage.__tablename__, "marker")
            await case.database.status(f'DROP INDEX "{case.schema}"."{name}"')
    return bundle


@pytest.mark.parametrize("damage", ["extra_stage", "another_bundle", "promoted", "profile", "archive", "missing_index"])
async def test_native_coupled_owner_refuses_unprepared_bundle_or_lost_index(monkeypatch, damage):
    native = importlib.import_module("process.entity_address_unified")
    async with ordinary_serving_case(monkeypatch) as case:
        monkeypatch.setattr(native, "db", case.database)
        await _prepare(case.database, case.schema)
        baseline = await _state(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            bundle, address, generation, admission, recovery_by_field = await _coupled_preparation(case, fence)
            bundle = await _damage_prepared_bundle(case, native, bundle, damage)
            token = artifacts._ACTIVE.set(replace(admission, recovery=recovery_by_field))
            fence_token = case.directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(fence)
            try:
                with pytest.raises(RuntimeError, match="replacement_bundle_changed|all serving indexes"):
                    await artifacts.publish_prepared_replacement(case.directory, fence, bundle, address)
            finally:
                case.directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)
                artifacts._ACTIVE.reset(token)
        assert await _state(case) == baseline
        assert await _alias(case.database, case.schema) == "endpoint_repoint"


@pytest.mark.parametrize("operation", ["publication_replay", "recovery_replay", "lost_ack"])
async def test_native_replacement_replay_rechecks_metadata_changed_after_hashing(monkeypatch, operation):
    async with ordinary_serving_case(monkeypatch) as case:
        task = await _published(case)
        if operation != "publication_replay":
            await recovery._recover(case.directory, task)
        original_fence = recovery._source_fence

        async def mutate_metadata():
            await case.database.status(
                f"UPDATE {case.schema}.address_alias_artifact_state_v1 SET generation=generation+1 "
                "WHERE artifact_name='provider_directory_address_overlay'"
            )

        async def fence_with_late_drift(*args, **kwargs):
            await original_fence(*args, **kwargs)
            await asyncio.create_task(mutate_metadata(), context=contextvars.Context())

        monkeypatch.setattr(recovery, "_source_fence", fence_with_late_drift)
        with pytest.raises(ValueError, match="current_serving_changed"):
            if operation == "publication_replay":
                await artifacts.replay_published_replacement(case.directory, _markers())
            elif operation == "recovery_replay":
                await recovery._recover_once(case.directory, task)
            else:
                await recovery._verify_recovered(case.directory, task)
