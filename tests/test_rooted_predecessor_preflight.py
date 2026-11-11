# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Core preflight component checks; runtime doubles do not issue Source authority."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from weakref import ref

import pytest

from process import provider_directory_rooted_graph_current_preflight as preflight
from process import provider_directory_rooted_graph_source_runtime as source_runtime
from process.provider_directory_dataset_scoped_publication_contract import ROOTED_COMBINED_VARIANT
from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql
from tests.provider_directory_rooted_graph_publication_test_support import exact_current
from tests.rooted_predecessor_component_support import ComponentRuntime
from tests.test_provider_directory_rooted_graph_publication_freeze import SCHEMA, _current_bodies


@pytest.fixture
def preflight_case(monkeypatch):
    runtime = ComponentRuntime()
    database = SimpleNamespace(scalar=AsyncMock(return_value=True), all=AsyncMock(), status=AsyncMock())
    current = exact_current(variant=ROOTED_COMBINED_VARIANT)
    fence_by_field = {
        "roots": {},
        "catalog": {
            "control_epochs": {"header": "10", "parent": "11"},
            "published_proof": {
                "preparation_id": "prior-preparation",
                "handoff_sha256": "a" * 64,
                "publication_sha256": "b" * 64,
            },
        },
    }
    snapshot = AsyncMock(return_value="fixed-snapshot")
    metadata = AsyncMock(return_value=current)
    monkeypatch.setattr(preflight, "current_rooted_graph_runtime", lambda _database: runtime)
    monkeypatch.setattr(preflight, "_current_metadata", metadata)
    monkeypatch.setattr(preflight, "_snapshot", snapshot)
    monkeypatch.setattr(preflight.scoped, "_schema_name", lambda: SCHEMA)

    async def scalar(query, **parameters):
        if query == root_input_fence_sql(SCHEMA, ":dataset_id", "CAST(:roots AS jsonb)", variant=current.variant):
            assert parameters == {"dataset_id": current.dataset_id, "roots": "{}"}
            if database.scalar.return_value is True:
                return fence_by_field
        return database.scalar.return_value

    database.scalar.side_effect = scalar
    return SimpleNamespace(
        runtime=runtime, database=database, current=current, snapshot=snapshot, metadata=metadata, fence=fence_by_field
    )


def test_adapter_bridge_preserves_factory_binding(monkeypatch):
    runtime = SimpleNamespace(require_bound_factory=Mock(), relation_coordinates=Mock(return_value=("schema", "table")))
    factory = SimpleNamespace(rooted_graph_runtime=runtime)
    monkeypatch.setattr(source_runtime.source, "_FACTORY", SimpleNamespace(get=lambda: factory))
    database = object()
    assert source_runtime.current_rooted_graph_runtime(database) is runtime
    runtime.require_bound_factory.assert_called_once_with(factory, database)
    assert source_runtime.rooted_graph_relation_coordinates("schema", "root", "table") == ("schema", "table")
    runtime.relation_coordinates.assert_called_once_with("schema", "root", "table")
    runtime.require_bound_factory.side_effect = RuntimeError("binding_changed")
    with pytest.raises(RuntimeError, match="binding_changed"):
        source_runtime.current_rooted_graph_runtime(database)
    factory.rooted_graph_runtime = None
    assert source_runtime.rooted_graph_relation_coordinates("schema", "root", "table") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("is_legacy", [False, True])
async def test_snapshot_binds_dataset_and_canonicalizes_json(monkeypatch, is_legacy):
    current = exact_current() if is_legacy else exact_current(variant=ROOTED_COMBINED_VARIANT)
    database = SimpleNamespace(scalar=AsyncMock(return_value='{"z":1,"a":2}'))
    monkeypatch.setattr(preflight.scoped, "_schema_name", lambda: SCHEMA)
    assert await preflight._snapshot(database, current) == '{"a":2,"z":1}'
    assert database.scalar.call_args.kwargs == {"dataset_id": current.dataset_id}
    expected = (
        "provider_directory_uhc_flex_practitioner_dataset" if is_legacy else "provider_directory_rooted_graph_dataset"
    )
    assert expected in database.scalar.call_args.args[0]
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
    assert case.database.scalar.await_count == 6
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
@pytest.mark.parametrize("field", ["control_epochs", "published_proof"])
async def test_rooted_predecessor_saves_and_rechecks_the_same_protected_fence(preflight_case, field):
    case = preflight_case
    proof = await preflight.prepare_current_root(case.database)
    assert json.loads(proof.input_fence_json) == case.fence
    assert await preflight.require_current_root_fence(case.database, proof) == case.current
    case.database.all.assert_not_awaited()
    case.database.status.assert_not_awaited()
    case.fence["catalog"][field] = {"changed": True}
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.require_current_root_fence(case.database, proof)


@pytest.mark.asyncio
async def test_rooted_preflight_requires_the_published_preparation(preflight_case):
    case = preflight_case
    case.fence["catalog"]["published_proof"] = None
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.prepare_current_root(case.database)


@pytest.mark.asyncio
async def test_fence_refuses_forged_or_transferred_capabilities(preflight_case):
    case = preflight_case
    proof = await preflight.prepare_current_root(case.database)
    other_runtime = ComponentRuntime()
    for changed in (replace(proof, brand=object()), replace(proof, runtime_ref=ref(other_runtime)), object()):
        case.metadata.reset_mock()
        with pytest.raises(RuntimeError, match="predecessor_invalid"):
            await preflight.require_current_root_fence(case.database, changed)
        case.metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_fence_locks_then_rechecks_generation(preflight_case, monkeypatch):
    case = preflight_case
    case.metadata.return_value = exact_current()
    case.snapshot.return_value = json.dumps({"header": {"candidate_acquisition_id": "candidate"}})
    case.database.scalar.side_effect = [_current_bodies()[preflight.LEGACY], True, True]
    monkeypatch.setattr(preflight.scoped, "_schema_name", lambda: SCHEMA)
    content = AsyncMock(return_value=(case.metadata.return_value.dataset_hash, 2, {"resource": {1, 2}}))
    monkeypatch.setattr(preflight, "validate_legacy_content", content)
    encoded = json.dumps({"roots": {"resource": [1, 2]}, "catalog": {"generation": 1}})
    capture = AsyncMock(return_value=encoded)
    monkeypatch.setattr(preflight, "capture_input_fence", capture)
    proof = await preflight.prepare_current_root(case.database)
    assert await preflight.prepare_current_root(case.database) is proof
    case.database.status.assert_not_awaited()
    case.database.all.assert_not_awaited()
    case.database.all.return_value = [{"nspname": SCHEMA, "relname": name} for name in ("first", "second")]
    assert await preflight.require_current_root_fence(case.database, proof) == proof.current
    assert case.database.all.call_args.kwargs == {"oids": [1, 2]}
    assert len(case.database.status.call_args_list) == 2
    assert all("ACCESS SHARE MODE NOWAIT" in call.args[0] for call in case.database.status.call_args_list)
    capture.return_value = encoded.replace('"generation": 1', '"generation": 2')
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.require_current_root_fence(case.database, proof)
    assert content.await_count == 1


@pytest.mark.asyncio
async def test_postcut_capture_keeps_original_root_coordinates(preflight_case):
    case = preflight_case
    proof = await preflight.prepare_current_root(case.database)
    case.database.scalar.return_value = {"roots": {}, "catalog": {"published": True}}
    captured = await preflight.capture_root_input_fence(case.database, proof.current)
    assert json.loads(captured) == case.database.scalar.return_value
    assert case.database.scalar.call_args.kwargs == {"dataset_id": proof.current.dataset_id, "roots": "{}"}
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.capture_root_input_fence(case.database, SimpleNamespace(dataset_id="other-edition"))


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
