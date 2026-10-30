# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The initial rename reserve uses the existing native dependency observer/arithmetic."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_inverse_commit as commit
from tests.provider_directory_serving_inverse_capacity_support import geometry, scope


@pytest.mark.asyncio
async def test_dml_reserve_preserves_native_control_commit_treatment():
    assert await commit.required_commit_bytes(None, scope(), dict(block_size=8192, wal_block_size=8192)) == 8192
    plan = geometry()
    plan["commit_wal_upper_bound_bytes"] = 1
    with pytest.raises(ValueError, match="commit_reserve_too_small"):
        await commit.assert_commit_reserve(None, plan, dict(block_size=8192, wal_block_size=8192))


@pytest.mark.asyncio
async def test_initial_reserve_observes_all_four_real_roots_through_existing_bounded_census(monkeypatch):
    from process import provider_directory_profile_failed_cleanup as cleanup

    scope_by_field = scope()
    for entry in scope_by_field["families"]:
        if entry["family"] in {"profile", "evidence"}:
            entry.update(
                restore_mode="initial_heap",
                retained_data_sha256=None,
                insert_rows=0,
                insert_logical_bytes=0,
                live_rows=0,
                live_logical_bytes=0,
            )
    scope_by_field["initial_receipt"] = dict(
        build_id="pdpb_" + "a" * 32,
        generation_id=scope_by_field["profile_provenance"]["generation_id"],
        run_id="run_" + "b" * 32,
        payload_sha256="c" * 64,
    )
    scope_by_field["input_sha256"] = contract.digest(
        contract.INPUT_DOMAIN,
        {key: scope_entry for key, scope_entry in scope_by_field.items() if key != "input_sha256"},
    )
    # This validates wire shapes only; the actual observer remains responsible
    # for all native/external-dependency/event-trigger refusals on the real DB.
    contract.validated_inverse_scope(scope_by_field)
    objects = [[1259, 100, 0], [1259, 101, 0], [1247, 102, 0], [2606, 103, 0]]
    evidence_by_field = dict(catalog_tuple_count=len(objects) + 1, relation_count=2)
    census = AsyncMock(return_value=(objects, 2))
    monkeypatch.setattr(cleanup, "_stage_dependency_closure", census)
    deletion = AsyncMock(side_effect=AssertionError("rename must not project catalog deletes"))
    monkeypatch.setattr(cleanup, "_catalog_deletion_manifest", deletion)
    fhir = SimpleNamespace(_schema=lambda: "unit")
    native_by_field = dict(block_size=8192, wal_block_size=8192)
    commit_bound = await commit.required_commit_bytes(fhir, scope_by_field, native_by_field)
    expected = cleanup._commit_wal_bound([evidence_by_field] * 4, SimpleNamespace(postgres_wal_block_size_bytes=8192))
    assert commit_bound == expected
    roots = {
        entry[key]
        for entry in scope_by_field["families"]
        if entry["restore_mode"] == "initial_heap"
        for key in ["root_oid", "retained_oid"]
    }
    assert len(roots) == 4 and {call.args[1] for call in census.await_args_list} == roots
    deletion.assert_not_awaited()
    with pytest.raises(ValueError, match="commit_reserve_too_small"):
        await commit.assert_commit_reserve(
            fhir, {"scope": scope_by_field, "commit_wal_upper_bound_bytes": commit_bound - 1}, native_by_field
        )
    census.side_effect = RuntimeError("drop_external_dependency")
    with pytest.raises(RuntimeError, match="external_dependency"):
        await commit.required_commit_bytes(fhir, scope_by_field, native_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["empty", "oversized", "kind", "external", "relations", "shape"])
async def test_rename_census_preserves_native_closure_refusals(monkeypatch, failure):
    from process import provider_directory_profile_failed_cleanup as cleanup

    objects = [] if failure == "empty" else [(999 if failure == "kind" else 1259, 17000, 0)]
    if failure == "oversized":
        objects *= 129
    fhir = SimpleNamespace(
        db=SimpleNamespace(
            all=AsyncMock(return_value=objects),
            scalar=AsyncMock(
                side_effect=[failure == "external", 33 if failure == "relations" else 1, failure == "shape"]
            ),
            status=AsyncMock(),
        )
    )
    deletion = AsyncMock(side_effect=AssertionError("unexpected DROP projection"))
    monkeypatch.setattr(cleanup, "_catalog_deletion_manifest", deletion)
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_"):
        await commit.rename_dependencies(fhir, {17000, 18000, 19000, 20000})
    deletion.assert_not_awaited()
    fhir.db.status.assert_not_awaited()
