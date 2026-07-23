from __future__ import annotations

from dataclasses import replace

import pytest

from process.ptg_parts.domain import (
    PTG2DownloadedJob,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
)
from process.ptg_parts import ptg2_shared_reuse as reuse


def _downloaded(
    *,
    raw_sha: str = "a" * 64,
    logical_sha: str = "b" * 64,
    raw_bytes: int = 100,
    logical_bytes: int = 1000,
    logical_hash_deferred: bool = False,
    plan_info: list[dict[str, object]] | None = None,
) -> PTG2DownloadedJob:
    raw_artifact = PTG2RawArtifact(
        original_url="https://example.invalid/rates.json.gz",
        canonical_url="https://example.invalid/rates.json.gz",
        raw_path="/tmp/input.gz",
        raw_storage_uri="file:///tmp/input.gz",
        raw_sha256=raw_sha,
        byte_count=raw_bytes,
    )
    logical_artifact = PTG2LogicalArtifact(
        logical_path="/tmp/input.gz",
        logical_sha256=logical_sha,
        byte_count=logical_bytes,
        compression="gzip",
        logical_hash_deferred=logical_hash_deferred,
    )
    return PTG2DownloadedJob(
        job={
            "type": "in_network",
            "plan_info": (
                [{"plan_id": "plan-1"}] if plan_info is None else plan_info
            ),
        },
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
    )


def _physical_input_identity(downloaded_jobs):
    return reuse.shared_physical_input_identity(
        downloaded_jobs,
        options={
            "snapshot_arch": "postgres_binary_v3",
            "snapshot_arch_variant": "shared_blocks_v3",
            "hash_mode": "checksum64",
        },
        scanner_canon_version={"version": 7},
    )


def test_shared_physical_identity_rejects_missing_kind():
    with pytest.raises(ValueError, match="identity is incomplete"):
        reuse.SharedPhysicalArtifactIdentity(
            source_type="in_network",
            identity_kind="",
            identity_sha256="a" * 64,
        )


def test_logical_plan_scope_rejects_missing_plan_id():
    with pytest.raises(ValueError, match="logical plan id"):
        reuse._logical_plan_scope({})


def test_logical_plan_fields_skip_blank_plan_ids():
    assert reuse.logical_plan_fields_for_job(
        {"plan_info": [{"plan_id": "  "}]}
    ) == ()


def test_downloaded_payload_requires_both_artifacts():
    downloaded = _downloaded()
    without_raw = replace(downloaded, raw_artifact=None)

    with pytest.raises(ValueError, match="both artifacts"):
        reuse._downloaded_artifact_payload(without_raw)


def test_deferred_logical_identity_requires_consistent_metadata():
    downloaded = _downloaded(
        logical_hash_deferred=True,
        logical_sha="a" * 64,
        raw_bytes=100,
        logical_bytes=101,
    )

    with pytest.raises(ValueError, match="deferred logical identity"):
        reuse._downloaded_artifact_payload(downloaded)


def test_logical_artifact_metadata_requires_both_artifacts():
    downloaded = replace(_downloaded(), logical_artifact=None)

    with pytest.raises(ValueError, match="missing artifact metadata"):
        reuse.shared_logical_artifact_metadata(downloaded)


def test_source_key_assignments_require_an_artifact():
    with pytest.raises(ValueError, match="requires an artifact"):
        reuse.deterministic_source_key_assignments(())


def test_logical_source_metadata_must_match_physical_identity():
    downloaded = _downloaded()
    identity = reuse.shared_physical_artifact_identity(downloaded)
    metadata = reuse.shared_logical_artifact_metadata(downloaded)

    with pytest.raises(ValueError, match="logical source metadata disagrees"):
        reuse.shared_snapshot_source_assignments(
            [
                {
                    **identity.as_dict(),
                    **metadata,
                    "logical_json_sha256": "c" * 64,
                    "source_trace_hash": "1" * 64,
                }
            ],
            expected_identities=(identity,),
        )


def test_deferred_source_metadata_must_match_physical_identity():
    downloaded = _downloaded(
        raw_sha="a" * 64,
        logical_sha="a" * 64,
        raw_bytes=100,
        logical_bytes=100,
        logical_hash_deferred=True,
    )
    identity = reuse.shared_physical_artifact_identity(downloaded)
    metadata = reuse.shared_logical_artifact_metadata(downloaded)

    with pytest.raises(ValueError, match="deferred source metadata disagrees"):
        reuse.shared_snapshot_source_assignments(
            [
                {
                    **identity.as_dict(),
                    **metadata,
                    "raw_container_sha256": "c" * 64,
                    "source_trace_hash": "1" * 64,
                }
            ],
            expected_identities=(identity,),
        )


def test_source_traces_must_match_expected_physical_inputs():
    downloaded = _downloaded()
    identity = reuse.shared_physical_artifact_identity(downloaded)

    with pytest.raises(ValueError, match="complete physical input set"):
        reuse.shared_snapshot_source_assignments(
            (),
            expected_identities=(identity,),
        )


def test_duplicate_identity_rejects_inconsistent_artifact_metadata():
    first = _downloaded(logical_bytes=1000)
    second = _downloaded(logical_bytes=1001)

    with pytest.raises(ValueError, match="artifact metadata is inconsistent"):
        _physical_input_identity((first, second))


def test_physical_input_identity_requires_a_downloaded_artifact():
    with pytest.raises(ValueError, match="at least one downloaded artifact"):
        _physical_input_identity(())


def test_physical_input_identity_requires_logical_plan_metadata():
    downloaded = _downloaded(plan_info=[])

    with pytest.raises(ValueError, match="missing logical plan metadata"):
        _physical_input_identity((downloaded,))
