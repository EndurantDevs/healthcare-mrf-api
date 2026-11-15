# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Core preflight component checks; runtime doubles do not issue Source authority."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_rooted_graph_current_preflight as preflight
from process import provider_directory_rooted_graph_source_runtime as source_runtime
from process.provider_directory_dataset_scoped_publication_contract import ROOTED_COMBINED_VARIANT
from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql
from tests.provider_directory_rooted_graph_publication_test_support import exact_current
from tests.rooted_predecessor_component_support import ComponentRuntime
from tests.test_provider_directory_rooted_graph_publication_freeze import SCHEMA


@pytest.fixture
def preflight_case(monkeypatch):
    runtime = ComponentRuntime()
    database = SimpleNamespace(scalar=AsyncMock(return_value=True), all=AsyncMock(), status=AsyncMock())
    current = exact_current(variant=ROOTED_COMBINED_VARIANT)
    fence_by_field = dict(
        dataset_id=current.dataset_id,
        variant=current.variant,
        header=dict(dataset_hash=current.dataset_hash, resource_count=current.resource_count),
        parent=dict(dataset_hash=current.dataset_hash, resource_count=current.resource_count),
    )
    snapshot = AsyncMock(return_value="fixed-snapshot")
    metadata = AsyncMock(return_value=current)
    monkeypatch.setattr(preflight, "current_rooted_graph_runtime", lambda _database: runtime)
    monkeypatch.setattr(preflight, "_current_metadata", metadata)
    monkeypatch.setattr(preflight, "_snapshot", snapshot)
    monkeypatch.setattr(preflight.scoped, "_schema_name", lambda: SCHEMA)

    async def scalar(query, **parameters):
        if query == root_input_fence_sql(SCHEMA, ":dataset_id", variant=current.variant):
            assert parameters == {"dataset_id": current.dataset_id}
            if database.scalar.return_value is True:
                return fence_by_field
        return database.scalar.return_value

    database.scalar.side_effect = scalar
    return SimpleNamespace(
        runtime=runtime, database=database, current=current, snapshot=snapshot, metadata=metadata, fence=fence_by_field
    )


@pytest.mark.asyncio
async def test_adapter_uses_configured_named_factory():
    runtime = SimpleNamespace(relation_coordinates=Mock(return_value=("schema", "table")))
    factory = SimpleNamespace(rooted_graph_runtime=runtime, transaction=Mock())
    database = object()
    assert source_runtime.current_rooted_graph_runtime(database) is None
    async with source_runtime.source.bind_generic_source_transaction_factory(factory):
        assert source_runtime.current_rooted_graph_runtime(database) is runtime
        assert source_runtime.rooted_graph_relation_coordinates("schema", "root", "table") == ("schema", "table")
    runtime.relation_coordinates.assert_called_once_with("schema", "root", "table")
    factory.transaction.assert_not_called()
    assert source_runtime.current_rooted_graph_runtime(database) is None
    assert source_runtime.rooted_graph_relation_coordinates("schema", "root", "table") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("is_legacy", [False, True])
async def test_snapshot_binds_dataset_and_canonicalizes_json(monkeypatch, is_legacy):
    current = exact_current() if is_legacy else exact_current(variant=ROOTED_COMBINED_VARIANT)
    inputs_by_root = {
        "provider_directory_rooted_graph_" + root: {"schema": "predecessor_snapshot", "name": "prior_" + root}
        for root in ("acquisition", "twin_attempt", "twin_admission")
    }
    document_by_field = {
        "identity": {"dataset_id": current.dataset_id},
        "storage": {"catalog_schema": SCHEMA, "storage_schema": "predecessor_snapshot"},
        "tables": {},
        "inputs": inputs_by_root,
    }
    database = SimpleNamespace(
        scalar=AsyncMock(return_value='{"z":1,"a":2}'),
        first=AsyncMock(return_value={"candidate_storage_json": document_by_field}),
    )
    monkeypatch.setattr(preflight.scoped, "_schema_name", lambda: SCHEMA)
    assert await preflight._snapshot(database, current) == '{"a":2,"z":1}'
    assert database.scalar.call_args.kwargs == {"dataset_id": current.dataset_id}
    expected = (
        "provider_directory_uhc_flex_practitioner_dataset" if is_legacy else "provider_directory_rooted_graph_dataset"
    )
    assert expected in database.scalar.call_args.args[0]
    if is_legacy:
        database.first.assert_not_awaited()
    else:
        database.first.assert_awaited_once_with(
            f'SELECT candidate_storage_json FROM "{SCHEMA}"."provider_directory_endpoint_dataset" WHERE dataset_id=:dataset_id',
            dataset_id=current.dataset_id,
        )
        assert all(
            '"predecessor_snapshot"."' + table["name"] + '"' in database.scalar.call_args.args[0]
            for table in inputs_by_root.values()
        )
    database.scalar.return_value = {"a": 2, "z": 1}
    assert await preflight._snapshot(database, current) == '{"a":2,"z":1}'
    database.scalar.return_value = []
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight._snapshot(database, current)


@pytest.mark.asyncio
async def test_preflight_reuses_only_revalidated_runtime_proof(preflight_case):
    case = preflight_case
    proof = await preflight.prepare_current_root(case.database)
    assert await preflight.prepare_current_root(case.database) is proof
    assert case.database.scalar.await_count == 3
    assert case.runtime.transactions == [{"isolation": "repeatable read", "readonly": True}] * 2
    case.database.scalar.return_value = False
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.prepare_current_root(case.database)
    case.database.scalar.return_value = True
    assert await preflight.prepare_current_root(case.database) is proof
    assert preflight.current_root_proof(case.database) is proof
    case.snapshot.return_value = "replacement-snapshot"
    refreshed = await preflight.prepare_current_root(case.database)
    assert refreshed is not proof and refreshed.snapshot_json == "replacement-snapshot"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [[False], [None], [0], ["true"], [True, False]])
async def test_preflight_never_caches_failed_native_results(preflight_case, failure):
    case = preflight_case
    case.database.scalar.side_effect = failure
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.prepare_current_root(case.database)
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        preflight.current_root_proof(case.database)


@pytest.mark.asyncio
async def test_preflight_rejects_snapshot_drift(preflight_case):
    case = preflight_case
    case.snapshot.side_effect = ["before", "after"]
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.prepare_current_root(case.database)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["header", "parent"])
async def test_rooted_predecessor_rechecks_logical_fence(preflight_case, field):
    case = preflight_case
    proof = await preflight.prepare_current_root(case.database)
    assert json.loads(proof.input_fence_json) == case.fence
    assert await preflight.require_current_root_fence(case.database, proof) == case.current
    case.database.all.assert_not_awaited()
    case.database.status.assert_not_awaited()
    case.fence[field]["dataset_hash"] = "changed"
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.require_current_root_fence(case.database, proof)


@pytest.mark.asyncio
async def test_rooted_preflight_requires_validated_publication_data(preflight_case):
    case = preflight_case
    case.database.scalar.return_value = False
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.prepare_current_root(case.database)
    case.database.scalar.assert_awaited_once_with(
        preflight.native_receipt_sql(SCHEMA, ":dataset_id"), dataset_id=case.current.dataset_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["type", "current", "snapshot", "input_fence"])
async def test_fence_refuses_changed_proof(preflight_case, field):
    case = preflight_case
    proof = await preflight.prepare_current_root(case.database)
    changed = {
        "type": object(),
        "current": replace(proof, current=replace(proof.current, dataset_hash="f" * 64)),
        "snapshot": replace(proof, snapshot_json="changed"),
        "input_fence": replace(proof, input_fence_json="{}"),
    }[field]
    case.metadata.reset_mock()
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.require_current_root_fence(case.database, changed)
    if field == "type":
        case.metadata.assert_not_awaited()
    else:
        case.metadata.assert_awaited_once_with(case.database, lock_rows=True)


@pytest.mark.asyncio
async def test_legacy_fence_locks_then_rechecks_generation(preflight_case, monkeypatch):
    case = preflight_case
    current = case.metadata.return_value = exact_current()
    case.snapshot.return_value = json.dumps({"header": {"candidate_acquisition_id": "candidate"}})
    content = AsyncMock(return_value=(current.dataset_hash, current.resource_count, {}))
    monkeypatch.setattr(preflight, "validate_legacy_content", content)
    fence_by_field = dict(
        dataset_id=current.dataset_id, variant=current.variant, header={"dataset_hash": current.dataset_hash}
    )
    metadata_query = preflight.legacy_metadata_query(preflight.validation_body_sql(preflight.LEGACY, SCHEMA), SCHEMA)

    async def scalar(query, **parameters):
        if query == root_input_fence_sql(SCHEMA, ":dataset_id", variant=current.variant):
            assert parameters == {"dataset_id": current.dataset_id}
            return fence_by_field
        assert parameters == (
            {"dataset_id": current.dataset_id} if query == metadata_query else {"cohort_id": current.root_cohort_id}
        )
        return True

    case.database.scalar.side_effect = scalar
    capture = AsyncMock(wraps=preflight.capture_root_input_fence)
    monkeypatch.setattr(preflight, "capture_root_input_fence", capture)
    proof = await preflight.prepare_current_root(case.database)
    assert await preflight.prepare_current_root(case.database) is proof
    case.database.status.assert_not_awaited()
    case.database.all.assert_not_awaited()
    assert await preflight.require_current_root_fence(case.database, proof) == proof.current
    assert case.metadata.await_args_list[-1].kwargs == {"lock_rows": True}
    assert (proof.dataset_sha256, proof.resource_count) == (current.dataset_hash, current.resource_count)
    assert capture.await_count == 2
    fence_by_field["header"]["dataset_hash"] = "f" * 64
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.require_current_root_fence(case.database, proof)
    content.assert_awaited_once_with(case.database, current, {"candidate_acquisition_id": "candidate"})
    assert sum(call.args[0] == metadata_query for call in case.database.scalar.await_args_list) == 1


@pytest.mark.asyncio
async def test_postcut_capture_keeps_original_root_coordinates(preflight_case):
    case = preflight_case
    proof = await preflight.prepare_current_root(case.database)

    async def scalar(query, **parameters):
        assert query == root_input_fence_sql(SCHEMA, ":dataset_id", variant=proof.current.variant)
        return case.fence if parameters == {"dataset_id": proof.current.dataset_id} else None

    case.database.scalar.side_effect = scalar
    captured = await preflight.capture_root_input_fence(case.database, proof.current)
    assert json.loads(captured) == case.fence
    assert case.database.scalar.call_args.kwargs == {"dataset_id": proof.current.dataset_id}
    other = replace(proof.current, dataset_id=proof.current.dataset_id[:-1] + "5")
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.capture_root_input_fence(case.database, other)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "missing", "duplicate", "hash", "unpublished", "not_current"])
async def test_current_metadata_requires_exact_consistency(monkeypatch, damage):
    header_by_field = dict(
        dataset_id="edition",
        endpoint_id="endpoint",
        dataset_hash="a" * 64,
        resource_count=1,
        status="published",
        is_current=True,
    )
    parents_by_id = {"edition": dict(header_by_field)}
    if damage == "missing":
        parents_by_id.clear()
    if damage == "duplicate":
        parents_by_id["other"] = dict(header_by_field)
    if damage == "hash":
        parents_by_id["edition"]["dataset_hash"] = "b" * 64
    if damage == "unpublished":
        header_by_field["status"] = "validated"
    if damage == "not_current":
        header_by_field["is_current"] = False
    monkeypatch.setattr(preflight.scoped, "_lock_pair_registry", AsyncMock())
    monkeypatch.setattr(preflight.scoped, "_locked_parent_by_id", AsyncMock(return_value=parents_by_id))
    monkeypatch.setattr(
        preflight.scoped, "_locked_current_header", AsyncMock(return_value=("variant", header_by_field))
    )
    selected = object()
    translate = Mock(return_value=selected)
    monkeypatch.setattr(preflight.scoped, "_exact_current_from_header", translate)
    if damage is None:
        assert await preflight._current_metadata(object(), lock_rows=False) is selected
    else:
        with pytest.raises(RuntimeError, match="predecessor_invalid"):
            await preflight._current_metadata(object(), lock_rows=False)
        translate.assert_not_called()


def _change_predecessor(case, drift):
    if drift == "snapshot":
        case.snapshot.return_value = "changed-snapshot"
    elif drift == "input_fence":
        case.fence["header"]["dataset_hash"] = "changed"
    elif drift == "current":
        case.metadata.return_value = replace(case.current, dataset_hash="f" * 64)


@pytest.fixture
def acquisition_fence_case(preflight_case, monkeypatch):
    """Use the existing component transport with the configured runtime branch."""
    case = preflight_case

    @asynccontextmanager
    async def transaction(database, **options):
        case.runtime.transactions.append(options)
        yield SimpleNamespace(session=database)

    monkeypatch.setattr(case.runtime, "transaction", transaction)
    monkeypatch.setattr(source_runtime, "current_rooted_graph_runtime", lambda database=None: case.runtime)
    monkeypatch.setattr(
        case.runtime, "relation_coordinates", lambda schema, root, name: ("candidate", name), raising=False
    )

    async def scalar(query, **parameters):
        if query == root_input_fence_sql(SCHEMA, ":dataset_id", variant=case.current.variant):
            assert parameters == {"dataset_id": case.current.dataset_id}
            return case.fence
        return True

    case.database.scalar.side_effect = scalar
    case.database.first = AsyncMock(return_value=None)
    case.fence_calls = []
    original_fence = preflight.require_current_root_fence

    async def fence(database, proof, *, lock_rows=True):
        case.fence_calls.append((lock_rows, len(case.lock_calls)))
        return await original_fence(database, proof, lock_rows=lock_rows)

    case.lock_calls = []
    monkeypatch.setattr(preflight, "require_current_root_fence", fence)
    return case


async def _acquisition_current_route(case, route):
    from process import provider_directory_rooted_graph_acquisition_runtime as acquisition
    from process import provider_directory_rooted_graph_operator as operator
    from process import provider_directory_rooted_graph_twin_store as twin
    from process.provider_directory_dataset_scoped_publication import EXACT_DATASET_PUBLICATION_LOCK_IDENTITY

    if route == "operator":
        return await operator._select_exact_current_root(case.database)
    if route == "status":
        return await acquisition._locked_acquisition_status(
            SimpleNamespace(acquisition_id="candidate"),
            case.database,
            EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
            lambda: None,
            None,
            lambda current, identity: current == case.current,
        )
    return await twin._lock_logical_current(case.database)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["operator", "status", "admission"])
@pytest.mark.parametrize("drift", [None, "snapshot", "input_fence", "current"])
async def test_acquisition_fences_compare_logical_inputs_under_advisory_lock(acquisition_fence_case, route, drift):
    from process.provider_directory_dataset_scoped_publication import EXACT_DATASET_PUBLICATION_LOCK_IDENTITY

    case = acquisition_fence_case
    proof = await preflight.prepare_current_root(case.database)
    original_scalar = case.database.scalar.side_effect

    async def scalar(query, **parameters):
        if "pg_advisory_xact_lock" in query or "pg_try_advisory_xact_lock" in query:
            assert list(parameters.values()) == [EXACT_DATASET_PUBLICATION_LOCK_IDENTITY]
            case.lock_calls.append(query)
            _change_predecessor(case, drift)
            return True
        return await original_scalar(query, **parameters)

    case.database.scalar.side_effect = scalar
    if drift is None:
        selected = await _acquisition_current_route(case, route)
        assert selected == ("absent" if route == "status" else proof.current)
    else:
        with pytest.raises(RuntimeError, match="predecessor_invalid"):
            await _acquisition_current_route(case, route)
        case.database.first.assert_not_awaited()
    assert len(case.lock_calls) == 1
    assert case.fence_calls[-1] == (False, 1)
    assert all(call.kwargs == {"lock_rows": False} for call in case.metadata.await_args_list)
    if route == "status" and drift is None:
        assert "FOR SHARE" in case.database.first.call_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["operator", "status"])
async def test_acquisition_busy_advisory_lock_refuses_before_fence(acquisition_fence_case, route):
    case = acquisition_fence_case
    original_scalar = case.database.scalar.side_effect

    async def scalar(query, **parameters):
        if "pg_try_advisory_xact_lock" in query:
            case.lock_calls.append(query)
            return False
        return await original_scalar(query, **parameters)

    case.database.scalar.side_effect = scalar
    with pytest.raises(RuntimeError):
        await _acquisition_current_route(case, route)
    assert len(case.lock_calls) == 1 and case.fence_calls == []
    case.database.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "snapshot", "input_fence"])
async def test_final_publication_keeps_default_row_locks_and_rechecks_inputs(
    acquisition_fence_case, monkeypatch, drift
):
    from process import provider_directory_rooted_graph_publication_finalize as finalize

    case = acquisition_fence_case
    proof = await preflight.prepare_current_root(case.database)
    completed = datetime(2026, 1, 1, tzinfo=UTC)

    async def fetchval(query, *parameters):
        if "pg_try_advisory_xact_lock" in query:
            case.lock_calls.append(query)
            _change_predecessor(case, drift)
            return True
        return completed

    connection = SimpleNamespace(is_in_transaction=lambda: True, execute=AsyncMock(), fetchval=fetchval)
    case.runtime.current_transaction = lambda database: SimpleNamespace(connection=connection)
    handoff_by_field = {
        "identity": {"dataset_id": "candidate", "endpoint_id": "endpoint", "root_dataset_id": proof.current.dataset_id},
        "operation": {"publication_acquisition_id": "acquisition"},
        "dataset_hash": "a" * 64,
        "preparation_id": "preparation",
        "counts": {"Practitioner": 1},
    }
    preparations = SimpleNamespace(
        fhir_source=(None, SimpleNamespace(document=handoff_by_field, document_sha256="b" * 64))
    )
    fence = AsyncMock(wraps=preflight.require_current_root_fence)
    insert, switch = AsyncMock(), AsyncMock()
    monkeypatch.setattr(finalize, "current_rooted_graph_runtime", lambda database: case.runtime)
    monkeypatch.setattr(finalize, "require_current_root_fence", fence)
    monkeypatch.setattr(finalize, "_insert_frozen_header", insert)
    monkeypatch.setattr(finalize, "_switch_frozen_headers", switch)
    if drift is None:
        publication = await finalize.publish_prepared_rooted_graph_dataset(
            preparations, None, {"state": "validated"}, database=case.database
        )
        assert publication["resource_count"] == 1 and insert.await_count == 2
        switch.assert_awaited_once_with(case.database, handoff_by_field, proof.current)
    else:
        with pytest.raises(RuntimeError, match="predecessor_invalid"):
            await finalize.publish_prepared_rooted_graph_dataset(
                preparations, None, {"state": "validated"}, database=case.database
            )
        insert.assert_not_awaited()
        switch.assert_not_awaited()
    fence.assert_awaited_once_with(case.database, proof)
    assert case.fence_calls[-1] == (True, 1)
    assert case.metadata.await_args_list[-1].kwargs == {"lock_rows": True}
