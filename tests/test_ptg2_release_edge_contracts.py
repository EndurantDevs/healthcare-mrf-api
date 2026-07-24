from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_v4 as candidate_v4
from api import ptg2_v4_graph
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)
from api.ptg2_shared_blocks import PTG2SharedBlockError
from process.ptg_parts import (
    canonical,
    import_rows,
    ptg2_manifest_artifacts as manifest_artifacts,
    ptg2_manifest_publish,
    rust_stage,
    table_setup,
)


def _write_mapping_fixture(
    tmp_path: Path,
) -> tuple[dict[str, object], Path, bytes, bytes]:
    first_id = b"a" * 16
    second_id = b"b" * 16
    manifest = manifest_artifacts.write_global_local_id_mapping(
        tmp_path,
        "mapping",
        {second_id: (3, 1, 3), first_id: ()},
    )
    return manifest, tmp_path / "mapping.manifest.json", first_id, second_id


def _install_mapping_manifest(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, object],
) -> None:
    monkeypatch.setattr(
        manifest_artifacts,
        "read_manifest",
        lambda *_args, **_kwargs: payload,
    )


def test_global_local_mapping_round_trip(tmp_path: Path) -> None:
    _manifest, manifest_path, first_id, second_id = _write_mapping_fixture(
        tmp_path
    )

    assert manifest_artifacts.read_global_local_id_mapping(manifest_path) == {
        second_id: (1, 3),
    }
    assert manifest_artifacts.build_dense_id_mapping(
        (second_id, first_id, second_id)
    ) == {first_id: 0, second_id: 1}
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts._normalize_local_ids((-1,))


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (("version", 999), ("artifact_type", "unexpected")),
)
def test_global_local_mapping_rejects_invalid_manifest_header(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    invalid_value: object,
) -> None:
    manifest, manifest_path, _first_id, _second_id = _write_mapping_fixture(
        tmp_path
    )
    invalid_manifest = copy.deepcopy(manifest)
    invalid_manifest[field_name] = invalid_value
    _install_mapping_manifest(monkeypatch, invalid_manifest)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (("record_size", 1), ("record_format", "unexpected")),
)
def test_global_local_mapping_rejects_invalid_record_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    invalid_value: object,
) -> None:
    manifest, manifest_path, _first_id, _second_id = _write_mapping_fixture(
        tmp_path
    )
    invalid_manifest = copy.deepcopy(manifest)
    invalid_manifest["sidecars"][0][field_name] = invalid_value
    _install_mapping_manifest(monkeypatch, invalid_manifest)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)


def test_global_local_mapping_rejects_invalid_record_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, manifest_path, _first_id, _second_id = _write_mapping_fixture(
        tmp_path
    )
    unaligned = copy.deepcopy(manifest)
    unaligned["sidecars"][0]["byte_count"] = 1
    _install_mapping_manifest(monkeypatch, unaligned)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)

    wrong_count = copy.deepcopy(manifest)
    wrong_count["sidecars"][0]["record_count"] = 99
    _install_mapping_manifest(monkeypatch, wrong_count)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)

    short_sidecar = tmp_path / "short.bin"
    short_sidecar.write_bytes(b"x" * 19)
    partial_record = copy.deepcopy(manifest)
    partial_record["sidecars"][0].update(
        {
            "path": short_sidecar.name,
            "byte_count": manifest_artifacts.PTG2_MANIFEST_MAPPING_RECORD_SIZE,
            "record_count": 1,
        }
    )
    _install_mapping_manifest(monkeypatch, partial_record)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)


def test_fast_provider_reference_projection_deduplicates_and_reports_missing() -> None:
    provider_map = {
        1: [
            None,
            {},
            {
                "__hash__": 11,
                "provider_group_hashes": [101, 102],
                "provider_count": 2,
                "network_name": ["primary"],
            },
            {"__hash__": 11, "provider_count": 99},
        ],
        2: [
            {
                "__hash__": 12,
                "npi": [1000000001, 1000000002],
                "network_name": "secondary",
            }
        ],
    }

    projected, missing = import_rows._fast_provider_entry_from_provider_refs(
        provider_map,
        [1, "1", 2, 3],
    )

    assert projected is not None
    assert projected["provider_count"] == 4
    assert projected["provider_group_hashes"] == [12, 101, 102]
    assert projected["network_name"] == ["primary", "secondary"]
    assert missing == [3]
    assert import_rows._fast_provider_entry_from_parts(
        entry_hashes=set(),
        provider_group_hashes=set(),
        provider_count=0,
    ) is None


@pytest.mark.asyncio
async def test_rust_stage_merge_covers_insert_drop_and_fast_rebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status = AsyncMock()
    monkeypatch.setattr(rust_stage.db, "status", status)
    monkeypatch.setenv(rust_stage.PTG2_FAST_FINAL_REBUILD_ENV, "false")
    monkeypatch.setenv(rust_stage.PTG2_COMPACT_BULK_DROP_INDEXES_ENV, "false")

    await rust_stage._merge_rust_copy_stage_tables(
        {
            "procedure": "procedure_stage",
            "serving_rate_compact": "serving_stage",
            rust_stage._serving_stage_lane_key(1): "serving_stage_1",
        }
    )

    statements = [call.args[0] for call in status.await_args_list]
    assert any("ON CONFLICT" in statement for statement in statements)
    assert sum("DROP TABLE IF EXISTS" in statement for statement in statements) == 3

    status.reset_mock()
    monkeypatch.setenv(rust_stage.PTG2_FAST_FINAL_REBUILD_ENV, "true")
    await rust_stage._merge_rust_copy_stage_tables(
        {"price_code_set": "price_code_stage"},
        drop=False,
    )
    statements = [call.args[0] for call in status.await_args_list]
    assert any("RENAME TO" in statement for statement in statements)
    assert not any("INSERT INTO" in statement for statement in statements)


@pytest.mark.asyncio
async def test_serving_stage_ensure_tolerates_optional_ddl_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_statements: list[str] = []

    async def status(statement: str) -> None:
        seen_statements.append(statement)
        if (
            "SET UNLOGGED" in statement
            or "ADD COLUMN IF NOT EXISTS plan_name" in statement
            or "DROP NOT NULL" in statement
            or "CREATE INDEX" in statement
        ):
            raise RuntimeError("optional DDL unavailable")

    monkeypatch.setattr(table_setup.db, "status", status)
    monkeypatch.setenv(table_setup.PTG2_UNLOGGED_STAGE_ENV, "true")
    monkeypatch.setenv(table_setup.PTG2_STAGE_INDEXES_ENV, "true")

    await table_setup._ensure_ptg2_serving_rate_stage_table("candidate_schema")

    assert any("CREATE UNLOGGED TABLE" in statement for statement in seen_statements)
    assert any("ADD COLUMN IF NOT EXISTS confidence" in statement for statement in seen_statements)
    assert any("CREATE INDEX" in statement for statement in seen_statements)

    seen_statements.clear()
    monkeypatch.setenv(table_setup.PTG2_UNLOGGED_STAGE_ENV, "false")
    monkeypatch.setenv(table_setup.PTG2_STAGE_INDEXES_ENV, "false")
    await table_setup._ensure_ptg2_serving_rate_stage_table("candidate_schema")
    assert not any("SET UNLOGGED" in statement for statement in seen_statements)
    assert not any("CREATE INDEX" in statement for statement in seen_statements)


def test_ptg_helpers_cover_money_and_lean_source_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert canonical.money_number(None) is None
    assert canonical.money_number("invalid") is None
    assert canonical.money_number("12.50") == 12.5
    assert canonical.money_number(12) == 12

    environment_name = (
        ptg2_manifest_publish.PTG2_MANIFEST_LEAN_SOURCE_UNIQUE_GUARD_ENV
    )
    monkeypatch.delenv(environment_name, raising=False)
    assert not ptg2_manifest_publish.should_use_lean_source_guard(
        arch_version="postgres_binary_v3",
        skip_final_serving_table=True,
    )
    assert ptg2_manifest_publish.should_use_lean_source_guard(
        arch_version="legacy",
        skip_final_serving_table=False,
    )
    monkeypatch.setenv(environment_name, "false")
    assert not ptg2_manifest_publish.should_use_lean_source_guard(
        arch_version="legacy",
        skip_final_serving_table=False,
    )


@pytest.mark.asyncio
async def test_v4_dictionary_reverse_lookup_and_candidate_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=[
                {"npi_key": 2, "npi": 1000000002},
                SimpleNamespace(_mapping={"npi_key": 1, "npi": 1000000001}),
            ]
        )
    )
    monkeypatch.setattr(ptg2_v4_graph, "_charge_v4_hot_npi_work", lambda *_args, **_kwargs: None)

    assert await ptg2_v4_graph.v4_npi_values_for_keys(
        session,
        snapshot_key=7,
        npi_keys=(2, 1, 2),
        schema_name="candidate_schema",
    ) == {2: 1000000002, 1: 1000000001}
    assert await ptg2_v4_graph.v4_npi_values_for_keys(
        session,
        snapshot_key=7,
        npi_keys=(),
        schema_name="candidate_schema",
    ) == {}

    tiny_budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1)
    with pytest.raises(CandidateAuditDecodedRetentionError):
        candidate_v4._reserve_v4_graph_projection(
            {1000000001: {1}},
            {1},
            tiny_budget,
        )

    candidate_keys_by_npi = {1000000001: set()}
    retained_bytes = candidate_v4._candidate_map_retained_bytes(
        candidate_keys_by_npi
    )
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
    budget.claim(retained_bytes, category="candidate projection")
    assert await candidate_v4.prove_v4_candidate_sets(
        object(),
        SimpleNamespace(),
        candidate_keys_by_npi,
        budget,
        schema_name="candidate_schema",
    ) == {1000000001: ()}
    assert budget.retained_bytes > 0


def test_v4_reverse_projection_enforces_member_limit() -> None:
    from api import ptg2_serving

    with pytest.raises(PTG2SharedBlockError):
        ptg2_serving._v4_single_npi_reverse_sets(
            1000000001,
            {1000000001: 1},
            {1: (5,)},
            {10: (5,), 11: (5,)},
            (10, 11),
            1,
        )
    with pytest.raises(PTG2SharedBlockError):
        ptg2_serving._v4_sets_by_npi_reverse(
            normalized_npis=(1000000001, 1000000002),
            npi_key_by_value={1000000001: 1, 1000000002: 2},
            first_members_by_npi_key={1: (5,), 2: (5,)},
            first_members_by_allowed_set={10: (5,)},
            allowed_provider_sets=(10,),
            max_members=1,
        )
    with pytest.raises(PTG2SharedBlockError):
        ptg2_serving._v4_sets_from_first_members(
            (1000000001,),
            {1000000001: 1},
            {1: (5,)},
            {5: (10, 11)},
            (10, 11),
            max_members=1,
        )
