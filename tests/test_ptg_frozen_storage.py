# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Storage attribution proofs for frozen multipart control metadata."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from process.ptg_parts.frozen_rate_binding import (
    frozen_rate_binding_from_params,
)
from process.ptg_parts.frozen_rate_storage import (
    FROZEN_RATE_STORAGE_CONTRACT,
    FROZEN_RATE_ZERO_OWNED_STORAGE_FIELDS,
    frozen_rate_storage_measurement,
)
from scripts.ptg_v4_dev_canary_retained_artifacts import (
    RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT,
    collect_retained_raw_artifact_storage,
)
from scripts.ptg_v4_dev_canary_support import CanaryConfigurationError
from tests.ptg_frozen_test_support import (
    frozen_candidate_evidence,
    protected_control_payload,
)


def test_storage_definition_never_double_counts_shared_payload():
    measurement = frozen_rate_storage_measurement(
        binding_rows=3,
        binding_relation_bytes=65_536,
    )

    assert measurement["contract"] == FROZEN_RATE_STORAGE_CONTRACT
    assert measurement["attribution"] == "control_metadata_only"
    assert measurement["owned_payload_bytes"] == {
        field_name: 0
        for field_name in FROZEN_RATE_ZERO_OWNED_STORAGE_FIELDS
    }
    assert measurement["retained_metadata"] == {
        "binding_rows": 3,
        "binding_relation_total_bytes": 65_536,
        "candidate_audit_metadata": "measured_by_candidate_audit_gate",
        "retained_raw_artifacts": (
            "measured_by_whole_snapshot_retained_artifact_gate"
        ),
    }
    assert measurement["excluded_shared_storage"] == [
        "shared_layout",
        "logical_snapshot",
    ]


@pytest.mark.parametrize(
    ("binding_rows", "binding_relation_bytes"),
    [(-1, 0), (0, -1), (False, 0), (0, True)],
)
def test_storage_definition_rejects_ambiguous_measurements(
    binding_rows,
    binding_relation_bytes,
):
    with pytest.raises(ValueError, match="non-negative integers"):
        frozen_rate_storage_measurement(
            binding_rows=binding_rows,
            binding_relation_bytes=binding_relation_bytes,
        )


class _RetainedArtifactConnection:
    def __init__(self, database_rows):
        self.database_rows = database_rows

    async def fetch(self, query, version_ids, artifact_kind):
        assert "ptg2_artifact_manifest" in query
        assert version_ids == [
            row["source_file_version_id"] for row in self.database_rows
        ]
        assert artifact_kind == "raw"
        return self.database_rows


def _retained_fixture(artifact_root: Path):
    control = protected_control_payload()
    params = control["params"]
    binding = frozen_rate_binding_from_params(params)
    assert binding is not None
    manifest, _database_sources = frozen_candidate_evidence(
        params,
        binding,
    )
    database_rows = []
    for descriptor in params["frozen_rate_files"]:
        raw_sha256 = descriptor["raw_sha256"]
        retained_path = artifact_root / f"{raw_sha256}.json.gz"
        retained_path.write_bytes(
            b"x" * int(descriptor["content_length"])
        )
        database_rows.append(
            {
                "ordinal": descriptor["ordinal"],
                "source_file_version_id": descriptor[
                    "engine_source_file_version_id"
                ],
                "source_identity_hash": descriptor[
                    "engine_source_identity_hash"
                ],
                "source_type": descriptor["source_type"],
                "canonical_url": descriptor["canonical_url"],
                "raw_storage_uri": retained_path.as_uri(),
                "raw_sha256": raw_sha256,
                "logical_sha256": descriptor["logical_sha256"],
                "content_length": descriptor["content_length"],
                "etag": descriptor["etag"],
                "last_modified": descriptor["last_modified"],
                "verification_mode": "downloaded",
                "version_payload_json": json.dumps(
                    {
                        "raw_byte_count": descriptor["content_length"],
                        "logical_hash_deferred": False,
                    }
                ),
                "artifact_manifest_count": 1,
                "source_version_reference_count": 2,
            }
        )
    return manifest, database_rows


@pytest.mark.asyncio
async def test_retained_raw_artifact_gate_measures_exact_physical_files(
    tmp_path: Path,
):
    manifest, database_rows = _retained_fixture(tmp_path)

    evidence = await collect_retained_raw_artifact_storage(
        _RetainedArtifactConnection(database_rows),
        schema_name="mrf",
        snapshot_id="ptg2:202607:retained",
        snapshot_manifest=manifest,
        artifact_root=tmp_path,
    )

    assert (
        evidence["contract"]
        == RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT
    )
    assert evidence["source_file_version_count"] == 2
    assert evidence["distinct_artifact_count"] == 2
    assert evidence["referenced_raw_bytes"] == 20_003
    assert evidence["referenced_physical_bytes"] >= 20_003
    assert evidence["all_files_verified"] is True
    assert len(evidence["evidence_sha256"]) == 64
    assert all(
        artifact["artifact_manifest_count"] == 1
        and artifact["source_version_reference_count"] == 2
        for artifact in evidence["artifacts"]
    )


@pytest.mark.asyncio
async def test_retained_raw_artifact_gate_fails_closed_on_missing_manifest(
    tmp_path: Path,
):
    manifest, database_rows = _retained_fixture(tmp_path)
    database_rows[0]["artifact_manifest_count"] = 0

    with pytest.raises(
        CanaryConfigurationError,
        match="database evidence changed",
    ):
        await collect_retained_raw_artifact_storage(
            _RetainedArtifactConnection(database_rows),
            schema_name="mrf",
            snapshot_id="ptg2:202607:retained",
            snapshot_manifest=manifest,
            artifact_root=tmp_path,
        )


@pytest.mark.asyncio
async def test_retained_raw_artifact_gate_rejects_external_path(
    tmp_path: Path,
):
    artifact_root = tmp_path / "store"
    artifact_root.mkdir()
    manifest, database_rows = _retained_fixture(artifact_root)
    external_path = tmp_path / database_rows[0]["raw_sha256"]
    external_path.write_bytes(
        b"x" * int(database_rows[0]["content_length"])
    )
    database_rows[0]["raw_storage_uri"] = external_path.as_uri()

    with pytest.raises(
        CanaryConfigurationError,
        match="outside the measured artifact volume",
    ):
        await collect_retained_raw_artifact_storage(
            _RetainedArtifactConnection(database_rows),
            schema_name="mrf",
            snapshot_id="ptg2:202607:retained",
            snapshot_manifest=manifest,
            artifact_root=artifact_root,
        )
