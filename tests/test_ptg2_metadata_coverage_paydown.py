# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import json
from pathlib import Path
import struct
from types import SimpleNamespace

import pytest

from process.ptg_parts import copy_load
from process.ptg_parts import ptg2_manifest_artifacts as artifacts
from process.ptg_parts import rust_stage
from process.ptg_parts import snapshot_cleanup


def _valid_graph_entry(
    name: str,
    *,
    source_shard_id: str = "shard-a",
    owner_count: int = 0,
    record_format: str = artifacts.PTG2_MANIFEST_MEMBERSHIP_FORMAT,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "name": name,
        "source_shard_id": source_shard_id,
        "storage_uri": f"db://ptg2_artifact/{name}",
        "chunk_bytes": artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_CHUNK_BYTES,
        "membership_version": artifacts.PTG2_MANIFEST_VERSION,
        "owner_count": owner_count,
        "entry_count": owner_count,
        "member_count": 0,
        "record_format": record_format,
        "owner_index_fence_format": (
            artifacts.PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_FORMAT
        ),
        "owner_index_fence_stride": (
            artifacts.PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_STRIDE
        ),
        "owner_index_fence_owners": ["0" * 32] if owner_count else [],
    }
    if record_format == artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT:
        metadata["member_global_count"] = 0
    header, byte_count = artifacts._v3_graph_geometry(
        metadata,
        version=artifacts.PTG2_MANIFEST_VERSION,
        owner_count=owner_count,
        member_count=0,
    )
    metadata["membership_header_hex"] = header.hex()
    metadata["byte_count"] = byte_count
    return metadata


def _graph_manifest(*entries: dict[str, object]) -> dict[str, object]:
    return {
        "arch_version": "postgres_binary_v3",
        "provider_membership_graph": {
            "artifact_version": artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION,
            "artifact_names": sorted(
                artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES
            ),
            "storage": "postgresql_chunks_v1",
        },
        "artifacts": {"sidecars": list(entries)},
    }


def test_serving_sidecar_header_checks_format_counts_and_bounds():
    magic = b"PTG2TEST"
    header_bytes = json.dumps({"format": "rows_v1", "row_count": 2}).encode()
    sidecar_bytes = (
        magic + struct.pack("<I", len(header_bytes)) + header_bytes + b"rows"
    )

    header, header_end = artifacts._serving_sidecar_header(
        sidecar_bytes,
        magic,
        "rows_v1",
        {"byte_count": len(sidecar_bytes), "row_count": 2},
    )

    assert header == {"format": "rows_v1", "row_count": 2}
    assert header_end == 12 + len(header_bytes)

    invalid_payloads = [
        (b"short", None, "invalid magic"),
        (
            magic + struct.pack("<I", len(header_bytes) + 100) + header_bytes,
            None,
            "truncated",
        ),
        (
            magic + struct.pack("<I", 2) + b"[]",
            None,
            "JSON object",
        ),
        (
            magic
            + struct.pack("<I", len(header_bytes))
            + header_bytes,
            None,
            "unsupported.*rows_v1",
        ),
        (
            sidecar_bytes,
            {"byte_count": len(sidecar_bytes) + 1},
            "byte_count mismatch",
        ),
        (sidecar_bytes, {"row_count": 3}, "row count mismatch"),
    ]
    for candidate, metadata, message in invalid_payloads:
        expected_format = "rows_v2" if "unsupported" in message else "rows_v1"
        with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
            artifacts._serving_sidecar_header(
                candidate,
                magic,
                expected_format,
                metadata,
            )


def test_graph_geometry_and_required_integers_reject_malformed_metadata():
    assert artifacts._required_provider_graph_metadata_integer({"count": "2"}, "count") == 2
    for value in (None, "not-an-integer", -1):
        with pytest.raises(artifacts.PTG2ManifestArtifactError):
            artifacts._required_provider_graph_metadata_integer(
                {"count": value},
                "count",
            )

    dense = _valid_graph_entry(
        "provider_forward",
        record_format=artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
    )
    assert dense["byte_count"] == artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_HEADER_SIZE

    with pytest.raises(artifacts.PTG2ManifestArtifactError, match="invalid format"):
        artifacts._v3_graph_geometry(
            {"record_format": "old"},
            version=1,
            owner_count=0,
            member_count=0,
        )
    with pytest.raises(artifacts.PTG2ManifestArtifactError, match="member_global_count"):
        artifacts._v3_graph_geometry(
            {"record_format": artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT},
            version=1,
            owner_count=0,
            member_count=0,
        )


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda value: value.update(source_shard_id=""), "source shard"),
        (lambda value: value.update(name="unexpected"), "unexpected"),
        (lambda value: value.update(storage_uri="file:///tmp/graph"), "not in PostgreSQL"),
        (lambda value: value.update(chunk_bytes=1), "obsolete chunks"),
        (lambda value: value.update(membership_version=2), "invalid counts"),
        (lambda value: value.update(membership_header_hex="00"), "invalid geometry"),
        (lambda value: value.update(owner_index_fence_format="old"), "owner fences"),
        (lambda value: value.update(owner_index_fence_stride=1), "invalid owner fences"),
        (lambda value: value.update(owner_index_fence_owners="not-a-list"), "invalid owner fences"),
        (lambda value: value.update(owner_index_fence_owners=["bad"]), "invalid owner fences"),
        (lambda value: value.update(owner_index_fence_owners=[]), "invalid owner fences"),
    ],
)
def test_validate_v3_graph_db_entry_rejects_each_broken_contract(mutator, message):
    metadata = _valid_graph_entry("provider_forward", owner_count=1)
    mutator(metadata)

    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.validate_v3_graph_db_entry(metadata, "provider_forward")


def test_validate_v3_graph_db_entry_accepts_current_geometry():
    metadata = _valid_graph_entry("provider_forward", owner_count=1)

    assert (
        artifacts.validate_v3_graph_db_entry(metadata, "provider_forward")
        == "provider_forward"
    )


def test_named_graph_entries_accepts_named_maps_and_sidecar_lists():
    direct = _valid_graph_entry("provider_forward")
    sidecar = _valid_graph_entry("provider_inverted")

    assert artifacts._named_graph_entries({}) == []
    assert artifacts._named_graph_entries({"artifacts": []}) == []
    assert artifacts._named_graph_entries(
        {
            "artifacts": {
                "provider_forward": direct,
                "sidecars": [sidecar, "ignored"],
                "ignored": "not-a-map",
            }
        }
    ) == [
        ("provider_forward", direct),
        ("sidecar_0", sidecar),
    ]


def test_v3_graph_contract_reports_missing_duplicate_obsolete_and_invalid_entries():
    assert artifacts.v3_graph_contract_errors({"arch_version": "postgres_binary_v2"}) == []
    assert artifacts.v3_graph_contract_errors(
        {"arch_version": "postgres_binary_v3"}
    ) == [artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION]

    invalid_contract = _graph_manifest()
    invalid_contract["provider_membership_graph"]["artifact_names"] = []
    assert artifacts.v3_graph_contract_errors(invalid_contract) == [
        artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION
    ]

    valid_entries = [
        _valid_graph_entry(name)
        for name in sorted(artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES)
    ]
    assert artifacts.v3_graph_contract_errors(_graph_manifest(*valid_entries)) == []

    duplicate = copy.deepcopy(valid_entries[0])
    obsolete = _valid_graph_entry("provider_forward")
    obsolete["name"] = "provider_npi"
    unknown = _valid_graph_entry("provider_forward")
    unknown["name"] = "future_graph"
    invalid = copy.deepcopy(valid_entries[1])
    invalid["storage_uri"] = "file:///tmp/graph"
    errors = artifacts.v3_graph_contract_errors(
        _graph_manifest(valid_entries[0], duplicate, obsolete, unknown, invalid)
    )

    assert any(error.endswith(":duplicate") for error in errors)
    assert "obsolete:provider_npi" in errors
    assert any("not in PostgreSQL" in error for error in errors)
    assert any(error.endswith(":missing") for error in errors)


def test_rust_stage_selects_compact_columns_and_worker_lanes():
    assert rust_stage._ptg2_dictionary_select_columns(
        "price_atom", ["canonical_payload", "amount"]
    ) == 'NULL AS "canonical_payload", "amount"'
    assert rust_stage._ptg2_dictionary_select_columns(
        "price_set", ["price_atom_hashes", "id"]
    ) == 'NULL AS "price_atom_hashes", "id"'
    assert rust_stage._ptg2_dictionary_select_columns(
        "provider_set", ["npi", "id"]
    ) == 'NULL AS "npi", "id"'
    assert rust_stage._ptg2_dictionary_select_columns("other", ["id"]) == '"id"'

    stage_table_by_key = {
        "serving_rate_compact": "base",
        "serving_rate_compact_lane_0002": "lane-two",
        "serving_rate_compact_lane_0001": "lane-one",
        "serving_rate_compact_lane_0003": "",
        "unrelated": "ignored",
    }
    assert rust_stage._serving_stage_tables(stage_table_by_key) == [
        "base",
        "lane-one",
        "lane-two",
    ]
    assert rust_stage._serving_stage_tables({"unrelated": "ignored"}) == []
    assert (
        rust_stage._serving_stage_table_for_copy(
            stage_table_by_key,
            Path("serving.worker2.copy"),
        )
        == "lane-two"
    )
    assert (
        rust_stage._serving_stage_table_for_copy(
            stage_table_by_key,
            Path("serving.worker9.copy"),
        )
        == "base"
    )
    assert (
        rust_stage._serving_stage_table_for_copy({}, Path("serving.copy"))
        == "ptg2_serving_rate_compact"
    )


def test_copy_load_normalizes_values_and_selects_conflict_targets():
    assert copy_load._copy_record_values(("a\x00b", ["c\x00d"])) == (
        "ab",
        ["cd"],
    )
    assert copy_load._strip_postgres_nuls(
        ("a\x00", {"b\x00": "c\x00", "nested": ("d\x00",)})
    ) == ("a", {"b": "c", "nested": ("d",)})
    assert copy_load._strip_postgres_nuls(3) == 3

    initial_indexes = SimpleNamespace(
        __my_initial_indexes__=[{}, {"index_elements": ["plan_id", "year"]}],
        __my_index_elements__=["ignored"],
    )
    explicit_index = SimpleNamespace(
        __my_initial_indexes__=[],
        __my_index_elements__=["snapshot_id"],
    )
    primary_key = SimpleNamespace(
        __table__=SimpleNamespace(
            primary_key=[SimpleNamespace(name="id"), SimpleNamespace(name="part")]
        )
    )

    assert copy_load._ptg2_conflict_targets(initial_indexes) == ["plan_id", "year"]
    assert copy_load._ptg2_conflict_targets(explicit_index) == ["snapshot_id"]
    assert copy_load._ptg2_conflict_targets(primary_key) == ["id", "part"]


def test_snapshot_cleanup_flattens_manifest_artifact_maps():
    direct_artifact_by_field = {"storage_uri": "db://ptg2_artifact/direct"}
    sidecar_artifact_by_field = {"storage_uri": "db://ptg2_artifact/sidecar"}

    assert snapshot_cleanup._snapshot_artifact_entries({}) == []
    assert snapshot_cleanup._snapshot_artifact_entries({"artifacts": []}) == []
    assert snapshot_cleanup._snapshot_artifact_entries(
        {
            "artifacts": {
                "direct": direct_artifact_by_field,
                "sidecars": [sidecar_artifact_by_field, "ignored"],
                "ignored": "not-a-map",
            }
        }
    ) == [direct_artifact_by_field, sidecar_artifact_by_field]
