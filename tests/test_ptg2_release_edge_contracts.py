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
    provider_references,
    ptg2_manifest_artifacts as manifest_artifacts,
    ptg2_manifest_publish,
    rust_stage,
    snapshot_cleanup,
    table_setup,
)


def test_global_local_mapping_round_trip_and_manifest_guards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_id = b"a" * 16
    second_id = b"b" * 16
    manifest = manifest_artifacts.write_global_local_id_mapping(
        tmp_path,
        "mapping",
        {second_id: (3, 1, 3), first_id: ()},
    )
    manifest_path = tmp_path / "mapping.manifest.json"

    assert manifest_artifacts.read_global_local_id_mapping(manifest_path) == {
        second_id: (1, 3),
    }
    assert manifest_artifacts.build_dense_id_mapping(
        (second_id, first_id, second_id)
    ) == {first_id: 0, second_id: 1}
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts._normalize_local_ids((-1,))

    def install_manifest(payload: dict[str, object]) -> None:
        monkeypatch.setattr(
            manifest_artifacts,
            "read_manifest",
            lambda *_args, **_kwargs: payload,
        )

    invalid_version = copy.deepcopy(manifest)
    invalid_version["version"] = 999
    install_manifest(invalid_version)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)

    invalid_type = copy.deepcopy(manifest)
    invalid_type["artifact_type"] = "unexpected"
    install_manifest(invalid_type)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)

    invalid_size = copy.deepcopy(manifest)
    invalid_size["sidecars"][0]["record_size"] = 1
    install_manifest(invalid_size)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)

    invalid_format = copy.deepcopy(manifest)
    invalid_format["sidecars"][0]["record_format"] = "unexpected"
    install_manifest(invalid_format)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)

    unaligned = copy.deepcopy(manifest)
    unaligned["sidecars"][0]["byte_count"] = 1
    install_manifest(unaligned)
    with pytest.raises(manifest_artifacts.PTG2ManifestArtifactError):
        manifest_artifacts.read_global_local_id_mapping(manifest_path)

    wrong_count = copy.deepcopy(manifest)
    wrong_count["sidecars"][0]["record_count"] = 99
    install_manifest(wrong_count)
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
    install_manifest(partial_record)
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


def test_snapshot_cleanup_helpers_preserve_owned_and_available_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serving_index = {
        "table": "mrf.ptg2_serving_rate_compact_transient",
        "serving_table_retained": False,
        "materialized_tables": {
            "primary": "mrf.ptg2_serving_rate_compact_primary",
            "invalid": "unrelated_table",
        },
    }
    assert snapshot_cleanup._required_snapshot_table_names(serving_index) == [
        "ptg2_serving_rate_compact_primary"
    ]
    assert snapshot_cleanup._required_snapshot_table_names(
        {
            "table": "mrf.ptg2_serving_rate_compact_transient",
            "serving_table_retained": False,
        }
    ) == []

    db_ids: set[str] = set()
    local_refs: set[str] = set()
    snapshot_cleanup._record_snapshot_artifact_reference(
        None,
        db_ids,
        local_refs,
        include_local_artifacts=True,
    )
    snapshot_cleanup._record_snapshot_artifact_reference(
        "db://ptg2_artifact/artifact_1",
        db_ids,
        local_refs,
        include_local_artifacts=True,
    )
    snapshot_cleanup._record_snapshot_artifact_reference(
        "relative.bin",
        db_ids,
        local_refs,
        include_local_artifacts=True,
    )
    snapshot_cleanup._record_snapshot_artifact_reference(
        "ignored.bin",
        db_ids,
        local_refs,
        include_local_artifacts=False,
    )
    assert db_ids == {"artifact_1"}
    assert local_refs == {"relative.bin"}

    local_file = tmp_path / "artifact.bin"
    local_file.write_bytes(b"ok")
    monkeypatch.setattr(
        snapshot_cleanup,
        "resolve_ptg2_artifact_dir",
        lambda: tmp_path,
    )
    assert snapshot_cleanup._is_local_artifact_available(str(local_file))
    assert snapshot_cleanup._is_local_artifact_available(local_file.as_uri())
    assert snapshot_cleanup._is_local_artifact_available(local_file.name)
    assert not snapshot_cleanup._is_local_artifact_available("missing.bin")

    owners = snapshot_cleanup._snapshot_table_owners(
        [
            {
                "snapshot_id": "candidate_a",
                "manifest": {
                    "serving_index": {
                        "table": "mrf.ptg2_serving_rate_compact_shared"
                    }
                },
            },
            {
                "snapshot_id": "candidate_b",
                "manifest": '{"serving_index":{"table":"mrf.ptg2_serving_rate_compact_shared"}}',
            },
            {"snapshot_id": "", "manifest": {}},
            {"snapshot_id": "candidate_c", "manifest": "not-json"},
        ]
    )
    assert owners == {
        "ptg2_serving_rate_compact_shared": {"candidate_a", "candidate_b"}
    }


@pytest.mark.asyncio
async def test_snapshot_db_artifact_availability_requires_complete_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert await snapshot_cleanup._available_snapshot_db_artifact_ids(
        "candidate_schema", "candidate", set()
    ) == set()

    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=False))
    assert await snapshot_cleanup._available_snapshot_db_artifact_ids(
        "candidate_schema", "candidate", {"artifact_1"}
    ) == set()

    monkeypatch.setattr(
        snapshot_cleanup,
        "_table_exists",
        AsyncMock(side_effect=(True, True)),
    )
    monkeypatch.setattr(
        snapshot_cleanup.db,
        "all",
        AsyncMock(
            return_value=[
                {"artifact_id": "artifact_1"},
                SimpleNamespace(_mapping={"artifact_id": "artifact_2"}),
            ]
        ),
    )
    assert await snapshot_cleanup._available_snapshot_db_artifact_ids(
        "candidate_schema",
        "candidate",
        {"artifact_1", "artifact_2"},
    ) == {"artifact_1", "artifact_2"}


@pytest.mark.asyncio
async def test_provider_reference_materialization_handles_failure_and_group_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialize = AsyncMock(side_effect=RuntimeError("unavailable"))
    facade = SimpleNamespace(
        ptg2_temp_parent=lambda: tmp_path,
        materialize_json_source=materialize,
        _record_source_version=AsyncMock(),
        _push_ptg2_objects=AsyncMock(),
        push_objects=AsyncMock(),
        flush_error_log=AsyncMock(),
        TEST_PROVIDER_GROUPS=1,
    )
    monkeypatch.setattr(provider_references, "_ptg_facade", lambda: facade)
    classes = {
        "PTGProviderGroup": object,
        "PTGFile": object,
        "ImportLog": object,
    }
    assert await provider_references._process_provider_reference_file(
        "https://invalid.example/reference.json",
        classes,
        test_mode=False,
    ) == {}

    raw_artifact = SimpleNamespace()
    logical_artifact = SimpleNamespace(logical_path=tmp_path / "logical.json")
    materialize.side_effect = None
    materialize.return_value = (raw_artifact, logical_artifact)
    monkeypatch.setattr(
        provider_references,
        "load_json_artifact",
        lambda _path: {
            "version": "1",
            "provider_groups": [
                {
                    "provider_group_id": "7",
                    "tin": {"type": "ein", "value": "000000000"},
                    "npi": [1000000001],
                    "network_name": ["primary"],
                },
                {
                    "provider_group_ref": 8,
                    "npi": [1000000002],
                },
            ],
        },
    )

    provider_map = await provider_references._process_provider_reference_file(
        "https://invalid.example/reference.json",
        classes,
        test_mode=True,
        import_run_id="run_1",
    )

    assert list(provider_map) == [7]
    assert provider_map[7][0]["provider_group_id"] == 7
    facade._record_source_version.assert_awaited_once()
    facade._push_ptg2_objects.assert_awaited_once()
    facade.push_objects.assert_awaited_once()
    facade.flush_error_log.assert_awaited_once()


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

    candidate_keys = {1000000001: set()}
    retained_bytes = candidate_v4._candidate_map_retained_bytes(candidate_keys)
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
    budget.claim(retained_bytes, category="candidate projection")
    assert await candidate_v4.prove_v4_candidate_sets(
        object(),
        SimpleNamespace(),
        candidate_keys,
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
