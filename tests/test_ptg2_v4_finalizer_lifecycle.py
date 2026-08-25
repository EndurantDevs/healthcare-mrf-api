# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Packed-finalizer reader and lifecycle guards."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars as sidecars
from api import ptg2_tables
from process.ptg_parts import ptg2_shared_gc as shared_gc
from process.ptg_parts import ptg2_v4_finalizer_maps as finalizer_maps
from process.ptg_parts import ptg2_v4_finalizer_range_reader as range_reader
from process.ptg_parts import ptg2_v4_failed_layout_state as failed_state
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    FinalizerMapError,
)
from tests.ptg2_shared_gc_test_support import _Executor
from tests.ptg2_v4_orchestration_support import _v4_reuse_manifest, ptg
from tests.test_ptg2_v4_finalizer_maps import (
    _Rows,
    _ScriptedSession,
    _packed_fixture,
    _root_row,
)


def _finalizer_manifest() -> dict[str, object]:
    return {
        "contract": PTG2_V4_FINALIZER_MAP_CONTRACT,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "map_digest": (b"d" * 32).hex(),
        "object_kinds": list(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS),
        "object_kind_count": 6,
        "map_pack_count": 6,
        "coordinate_count": 6,
        "entry_count": 9,
        "logical_byte_count": 12,
        "stored_map_byte_count": 600,
        "target_block_count": 6,
        "canonical_mapping_digest": (b"c" * 32).hex(),
        "canonical_byte_count": 640,
        "target_identity_digest": (b"t" * 32).hex(),
    }


def _finalizer_root_fields() -> dict[str, object]:
    manifest = _finalizer_manifest()
    return {
        "state": "sealed",
        "generation": snapshot_maps.PTG2_V4_SHARED_GENERATION,
        "finalizer_root_present": True,
        "finalizer_root_state": "complete",
        "finalizer_root_contract": PTG2_V4_FINALIZER_MAP_CONTRACT,
        "finalizer_root_map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "finalizer_root_map_digest": b"d" * 32,
        "finalizer_root_canonical_mapping_digest": b"c" * 32,
        "finalizer_root_canonical_byte_count": 640,
        "finalizer_root_target_identity_digest": b"t" * 32,
        "finalizer_root_completed_at": object(),
        "finalizer_relational_mapping_present": False,
        **{
            f"finalizer_root_{field_name}": manifest[field_name]
            for field_name in (
                "object_kind_count",
                "map_pack_count",
                "coordinate_count",
                "entry_count",
                "logical_byte_count",
                "stored_map_byte_count",
                "target_block_count",
            )
        },
    }


@pytest.mark.asyncio
async def test_partial_finalizer_storage_extension_fails_closed() -> None:
    session = _ScriptedSession(
        (),
        finalizer_tables={"ptg2_v4_finalizer_map_root": True},
    )
    with pytest.raises(FinalizerMapError, match="storage extension is partial"):
        await finalizer_maps.has_complete_v4_finalizer_map(
            session,
            schema_name="mrf",
            snapshot_key=17,
        )


def test_reused_serving_index_validates_explicit_finalizer_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ptg,
        "validate_source_witness_manifest",
        lambda value, **_kwargs: value,
    )
    monkeypatch.setattr(
        ptg,
        "validate_provider_identifier_quarantine",
        lambda value: value,
    )
    layout_manifest = _v4_reuse_manifest()
    layout_manifest["serving_index"]["finalizer_mapping"] = _finalizer_manifest()

    reused = ptg._reused_shared_v3_serving_index(
        layout_manifest,
        source_key="source",
        shared_snapshot_key=17,
        expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
    )

    assert reused["finalizer_mapping"] == _finalizer_manifest()


@pytest.mark.parametrize(
    "mutation",
    (
        lambda manifest: manifest.update(contract="unknown"),
        lambda manifest: manifest.update(object_kind_count=True),
        lambda manifest: manifest.update(target_block_count=0),
        lambda manifest: manifest.update(canonical_byte_count=0),
        lambda manifest: manifest.update(unexpected=True),
    ),
)
def test_reused_serving_index_rejects_invalid_finalizer_manifest(mutation) -> None:
    manifest = _finalizer_manifest()
    mutation(manifest)
    with pytest.raises(RuntimeError, match="finalizer mapping"):
        ptg._validate_reused_v4_finalizer_manifest(
            {"finalizer_mapping": manifest}
        )


def test_reused_serving_index_rejects_explicit_null_finalizer_manifest() -> None:
    with pytest.raises(RuntimeError, match="finalizer mapping"):
        ptg._validate_reused_v4_finalizer_manifest({"finalizer_mapping": None})


def test_reused_serving_index_accepts_exact_finalizer_receipt() -> None:
    manifest = _finalizer_manifest()
    ptg._validate_reused_v4_finalizer_manifest({"finalizer_mapping": manifest})
    snapshot_maps._validate_reused_finalizer_root(
        _finalizer_root_fields(),
        {"serving_index": {"finalizer_mapping": manifest}},
    )


def test_sealed_reuse_accepts_legacy_or_exact_packed_finalizer_state() -> None:
    snapshot_maps._validate_reused_finalizer_root({}, {"serving_index": {}})
    snapshot_maps._validate_reused_finalizer_root(
        _finalizer_root_fields(),
        {"serving_index": {"finalizer_mapping": _finalizer_manifest()}},
    )


@pytest.mark.parametrize(
    ("root_mutation", "manifest_mutation", "message"),
    (
        (
            lambda root: root.update(finalizer_root_present=False),
            lambda _manifest: None,
            "must appear together",
        ),
        (
            lambda _root: None,
            lambda manifest: manifest["serving_index"].pop("finalizer_mapping"),
            "must appear together",
        ),
        (
            lambda root: root.update(finalizer_relational_mapping_present=True),
            lambda _manifest: None,
            "mixed relational mappings",
        ),
        (
            lambda root: root.update(finalizer_root_coordinate_count=7),
            lambda _manifest: None,
            "root is inconsistent",
        ),
    ),
)
def test_sealed_reuse_rejects_one_sided_mixed_or_drifted_finalizer_state(
    root_mutation,
    manifest_mutation,
    message: str,
) -> None:
    root_fields = _finalizer_root_fields()
    layout_manifest_by_field = {
        "serving_index": {"finalizer_mapping": _finalizer_manifest()}
    }
    root_mutation(root_fields)
    manifest_mutation(layout_manifest_by_field)
    with pytest.raises(RuntimeError, match=message):
        snapshot_maps._validate_reused_finalizer_root(
            root_fields,
            layout_manifest_by_field,
        )


@pytest.mark.asyncio
async def test_direct_existence_shortcuts_use_packed_range_adapter(monkeypatch):
    packed_ranges = AsyncMock(return_value={7: (7 << 31,)})
    monkeypatch.setattr(sidecars, "_packed_finalizer_block_keys_by_range", packed_ranges)
    complete_root = AsyncMock(return_value=True)
    monkeypatch.setattr(
        sidecars,
        "has_complete_v4_finalizer_map",
        complete_root,
    )

    assert await sidecars.has_serving_binary_code_block(
        object(), 7, shared_snapshot_key=41
    )
    assert await sidecars.has_shared_provider_pages_in_db(object(), 41)
    assert packed_ranges.await_count == 1
    assert packed_ranges.await_args.kwargs["maximum_pack_rows"] == 1
    complete_root.assert_awaited_once()


@pytest.mark.asyncio
async def test_packed_range_adapter_authenticates_map_and_targets(monkeypatch):
    object_kind = "by_code_provider_shard_v1"
    pack_by_field, target_rows, _expected = _packed_fixture(object_kind)
    pack_by_field["range_key"] = 7
    session = _ScriptedSession(
        (_Rows((_root_row(),)), _Rows((pack_by_field,)), _Rows(target_rows))
    )

    keys_by_range = await sidecars._packed_finalizer_block_keys_by_range(
        session,
        shared_snapshot_key=17,
        schema_name="mrf",
        object_kind=object_kind,
        ranges=((7, 1, 2),),
    )

    assert keys_by_range == {7: (1, 2)}
    range_calls = [
        params for sql, params in session.calls if "WITH requested_range" in sql
    ]
    assert range_calls[0]["batch_rows"] == 128


@pytest.mark.asyncio
async def test_packed_range_adapter_chunks_target_validation(monkeypatch):
    object_kind = "by_code_provider_shard_v1"
    pack_by_field, target_rows, _expected = _packed_fixture(object_kind)
    first_range_pack_by_field = {**pack_by_field, "range_key": 7}
    second_range_pack_by_field = {**pack_by_field, "range_key": 8}
    monkeypatch.setattr(
        range_reader,
        "PTG2_V4_FINALIZER_RANGE_PACK_BATCH_ROWS",
        1,
    )
    session = _ScriptedSession(
        (
            _Rows((_root_row(),)),
            _Rows((first_range_pack_by_field,)),
            _Rows(target_rows),
            _Rows((second_range_pack_by_field,)),
            _Rows(target_rows),
            _Rows(),
        )
    )

    keys_by_range = await sidecars._packed_finalizer_block_keys_by_range(
        session,
        shared_snapshot_key=17,
        schema_name="mrf",
        object_kind=object_kind,
        ranges=((7, 1, 2), (8, 1, 2)),
    )

    assert keys_by_range == {7: (1, 2), 8: (1, 2)}
    target_calls = [
        params
        for sql, params in session.calls
        if "ptg2_v4_finalizer_map_target" in sql
        and "SELECT target.block_hash" in sql
    ]
    assert len(target_calls) == 2
    assert all(len(params["block_hashes"]) == 2 for params in target_calls)
    pack_calls = [
        params
        for sql, params in session.calls
        if "WITH requested_range" in sql
    ]
    assert [params["has_cursor"] for params in pack_calls] == [False, True, True]


@pytest.mark.asyncio
async def test_packed_range_adapter_charges_before_retaining_key():
    object_kind = "by_code_provider_shard_v1"
    pack_by_field, target_rows, _expected = _packed_fixture(object_kind)
    pack_by_field["range_key"] = 7
    session = _ScriptedSession(
        (_Rows((_root_row(),)), _Rows((pack_by_field,)), _Rows(target_rows))
    )

    class _FailingRetention:
        budget = object()
        claimed_keys = 0

        def claim(self, _byte_count, *, category):
            assert category == "a decoded discovered forward shard"
            self.claimed_keys += 1
            if self.claimed_keys == 2:
                raise RuntimeError("retention exhausted")

    retention = _FailingRetention()
    with pytest.raises(RuntimeError, match="retention exhausted"):
        await sidecars._packed_finalizer_block_keys_by_range(
            session,
            shared_snapshot_key=17,
            schema_name="mrf",
            object_kind=object_kind,
            ranges=((7, 1, 2),),
            temporary_retention=retention,
        )

    assert retention.claimed_keys == 2


@pytest.mark.asyncio
async def test_activation_translates_packed_root_mismatch(monkeypatch):
    monkeypatch.setattr(
        ptg2_tables,
        "has_complete_v4_finalizer_map",
        AsyncMock(side_effect=FinalizerMapError("root mismatch")),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="root mismatch"):
        await ptg2_tables._validate_v4_finalizer_map_root(
            object(), snapshot_key=17
        )


@pytest.mark.asyncio
async def test_new_v4_seal_authenticates_finalizer_before_fingerprint(monkeypatch):
    seal = AsyncMock()
    authenticate = AsyncMock(side_effect=FinalizerMapError("mixed contract"))
    fingerprint = AsyncMock()
    monkeypatch.setattr(snapshot_maps, "_seal_new_v4_layout", seal)
    monkeypatch.setattr(
        finalizer_maps,
        "has_valid_finalizer_map",
        authenticate,
    )
    monkeypatch.setattr(snapshot_maps, "publish_layout_fingerprint", fingerprint)
    state = SimpleNamespace(
        schema_name="mrf",
        schema='"mrf"',
        snapshot_key=17,
        build_token="build",
        support_digest=b"s" * 32,
        sealed_manifest={},
        summary=SimpleNamespace(map_digest=b"m" * 32, logical_byte_count=1),
    )

    with pytest.raises(RuntimeError, match="authenticated during seal"):
        await snapshot_maps._seal_and_publish_v4_layout(object(), state)

    seal.assert_awaited_once()
    authenticate.assert_awaited_once()
    fingerprint.assert_not_awaited()


def test_gc_sql_protects_and_releases_finalizer_map_hashes():
    plan_sql = shared_gc._layout_plan_sql("mrf")
    release_sql = shared_gc._release_layouts_sql("mrf")
    delete_sql = shared_gc._delete_blocks_sql(
        "mrf",
        v4_tables_available=True,
        finalizer_tables_available=True,
    )

    assert "ptg2_v4_finalizer_map_pack" in plan_sql
    assert "ptg2_v4_finalizer_map_target" in plan_sql
    assert "ptg2_v4_finalizer_map_pack" in release_sql
    assert "ptg2_v4_finalizer_map_target" in release_sql
    assert "ptg2_v4_finalizer_map_pack" in delete_sql
    assert "ptg2_v4_finalizer_map_target" in delete_sql
    assert "ptg2_v4_finalizer_map_pack" not in shared_gc._layout_plan_sql(
        "mrf", finalizer_tables_available=False
    )
    assert "ptg2_v4_finalizer_map_target" not in shared_gc._release_layouts_sql(
        "mrf", finalizer_tables_available=False
    )
    base_only_delete_sql = shared_gc._delete_blocks_sql(
        "mrf",
        v4_tables_available=True,
        finalizer_tables_available=False,
    )
    assert "ptg2_v4_snapshot_map_pack" in base_only_delete_sql
    assert "ptg2_v4_finalizer_map_pack" not in base_only_delete_sql


@pytest.mark.asyncio
async def test_gc_resolves_bounded_finalizer_candidate_hashes():
    block_hash = b"h" * 32
    executor = _Executor([{"block_hash": block_hash}])

    reachable = await shared_gc._v4_finalizer_candidate_hashes(
        executor,
        schema_name="mrf",
        candidate_hashes={block_hash, b"x" * 32},
        snapshot_keys=(17,),
    )

    assert reachable == {block_hash}
    sql, params = executor.all_calls[0]
    assert "ptg2_v4_finalizer_map_pack" in sql
    assert "ptg2_v4_finalizer_map_target" in sql
    assert params["snapshot_keys"] == [17]


@pytest.mark.asyncio
async def test_failed_cleanup_allows_complete_finalizer_root(monkeypatch):
    monkeypatch.setattr(
        shared_gc,
        "_owned_v4_layout_fingerprint",
        AsyncMock(return_value=b"f" * 32),
    )
    executor = _Executor(
        [{
            "build_token": "token",
            "is_bound": False,
            "root_state": "building",
            "finalizer_root_state": "complete",
        }]
    )

    assert await shared_gc._is_owned_v4_layout_locked(
        executor,
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
    )


@pytest.mark.asyncio
async def test_failed_layout_counts_include_finalizer_ownership():
    executor = AsyncMock()
    tables_present = {
        table_name: True for table_name in failed_state._FINALIZER_MAP_TABLES
    }
    executor.first.side_effect = [
        tables_present,
        {},
        tables_present,
        {},
        tables_present,
        {},
    ]

    await failed_state.load_reference_counts(
        executor,
        schema_name="mrf",
        snapshot_id="snapshot",
        snapshot_key=17,
    )
    reference_sql = executor.first.await_args.args[0]
    assert "ptg2_v4_finalizer_map_pack" in reference_sql
    assert "ptg2_v4_finalizer_map_target" in reference_sql

    await failed_state.load_block_stats(
        executor,
        schema_name="mrf",
        snapshot_key=17,
        v4_hashes=set(),
    )
    block_stats_sql = executor.first.await_args.args[0]
    assert "ptg2_v4_finalizer_map_pack" in block_stats_sql
    assert "ptg2_v4_finalizer_map_target" in block_stats_sql

    await failed_state.load_recovery_postconditions(
        executor,
        schema_name="mrf",
        snapshot_key=17,
    )
    postcondition_sql = executor.first.await_args.args[0]
    assert "ptg2_v4_finalizer_map_root" in postcondition_sql
    assert "ptg2_v4_finalizer_map_pack" in postcondition_sql
    assert "ptg2_v4_finalizer_map_target" in postcondition_sql


@pytest.mark.asyncio
async def test_failed_layout_counts_accept_absent_or_reject_partial_finalizer_storage():
    legacy = AsyncMock()
    legacy.first.side_effect = [{}, {}]
    await failed_state.load_reference_counts(
        legacy,
        schema_name="mrf",
        snapshot_id="snapshot",
        snapshot_key=17,
    )
    legacy_sql = legacy.first.await_args.args[0]
    assert "ptg2_v4_finalizer_map_target" not in legacy_sql

    partial = AsyncMock()
    partial.first.return_value = {
        failed_state.PTG2_V4_FINALIZER_MAP_ROOT_TABLE: True,
    }
    with pytest.raises(RuntimeError, match="storage extension is partial"):
        await failed_state.load_reference_counts(
            partial,
            schema_name="mrf",
            snapshot_id="snapshot",
            snapshot_key=17,
        )
    assert partial.first.await_count == 1
