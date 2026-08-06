# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Orchestration proofs for exact physical-source tax-sidecar binding."""

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts.ptg2_shared_reuse import (
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
)
from process.ptg_parts.ptg2_tax_identity_source_binding import (
    TaxIdentityRateSourceBindingError,
)

ptg = importlib.import_module("process.ptg")


def _assignment() -> SharedSnapshotSourceAssignment:
    identity = SharedPhysicalArtifactIdentity(
        source_type="in_network",
        identity_kind="logical_json_sha256_v1",
        identity_sha256="1" * 64,
    )
    return SharedSnapshotSourceAssignment(
        source_key=0,
        identity=identity,
        source_trace_set_hash="2" * 64,
        source_trace_hashes=("3" * 64,),
        raw_container_sha256="4" * 64,
        logical_json_sha256=identity.identity_sha256,
        logical_hash_deferred=False,
    )


def _file_result(*, sidecar: bool) -> dict[str, object]:
    assignment = _assignment()
    manifest_by_field: dict[str, object] = {
        "physical_artifact_identity": assignment.identity.as_dict(),
    }
    if sidecar:
        manifest_by_field["sidecars"] = {
            "provider_group_tax_identity": {
                "name": "provider_group_tax_identity",
                "path": "/private/ephemeral/source.ptg2tax",
                "sha256": "5" * 64,
            }
        }
    return {
        "file_id": 17,
        "summary": {"manifest": manifest_by_field},
    }


def test_fresh_orchestration_binds_exact_source_key_without_mutating_manifest():
    file_result = _file_result(sidecar=True)

    bound = ptg._bound_tax_identity_source_artifacts(
        (file_result,),
        (_assignment(),),
    )

    assert len(bound) == 1
    assert bound[0]["source_shard_id"] == "file:17"
    assert bound[0]["physical_source_binding"] == {
        "contract": "ptg2_tax_identity_rate_source_binding_v1",
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "1" * 64,
        "source_key": 0,
    }
    original = file_result["summary"]["manifest"]["sidecars"][
        "provider_group_tax_identity"
    ]
    assert "source_shard_id" not in original
    assert "physical_source_binding" not in original


def test_fresh_orchestration_fails_closed_on_missing_sidecar():
    with pytest.raises(
        TaxIdentityRateSourceBindingError,
        match="ptg2_tax_identity_rate_source_binding_invalid",
    ):
        ptg._bound_tax_identity_source_artifacts(
            (_file_result(sidecar=False),),
            (_assignment(),),
        )


def test_v4_publisher_fingerprint_includes_projection_contract_only_for_v4():
    process_root = Path(ptg.__file__).resolve().parent

    v3_sources = ptg._shared_v3_publisher_sources(
        process_root,
        include_provider_graph_v4=False,
    )
    v4_sources = ptg._shared_v3_publisher_sources(
        process_root,
        include_provider_graph_v4=True,
    )

    assert not any("tax_identity_source" in path.name for path in v3_sources)
    assert {path.name for path in v4_sources} >= {
        "ptg2_tax_identity_source_binding.py",
        "ptg2_tax_identity_source_artifact.py",
        "ptg2_tax_identity_source_aggregate_reuse.py",
        "ptg2_tax_identity_source_binding_vector.py",
        "ptg2_tax_identity_source_copy.py",
        "ptg2_tax_identity_source_files.py",
        "ptg2_tax_identity_source_observations.py",
        "ptg2_tax_identity_source_persisted.py",
        "ptg2_tax_identity_source_preflight.py",
        "ptg2_tax_identity_source_projection.py",
        "ptg2_tax_identity_source_publish.py",
        "ptg2_tax_identity_source_seal_validation.py",
        "ptg2_tax_identity_source_stage.py",
        "ptg2_tax_identity_source_target_preflight.py",
        "ptg2_tax_identity_source_validation.py",
    }


def _reuse_publication_and_evidence():
    publication = SimpleNamespace(
        layout_manifest={},
        source_key="source",
        shared_snapshot_key=17,
        expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
        coverage_scope_id=b"c" * 32,
        shared_input_identity=SimpleNamespace(source_count=1),
        snapshot_id="snapshot",
    )
    evidence = SimpleNamespace(
        source_trace_hashes={"3" * 64},
        network_names=set(),
        source_provenance_entries=(),
    )
    return publication, evidence


def _patched_reuse_fixture(monkeypatch):
    """Install deterministic reused-layout collaborators and return their proof."""

    metadata_by_field = {
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "content_digest": "5" * 64,
    }
    aggregate_metadata_by_field = {
        "contract": "ptg2_provider_group_tax_identity_v1",
        "content_digest": "6" * 64,
    }
    serving_index_by_field = {
        "source_count": 1,
        "provider_graph": {
            "provider_tax_identity": aggregate_metadata_by_field,
            "provider_tax_identity_source": metadata_by_field,
        },
    }
    assignment = _assignment()
    validate_projection = AsyncMock()
    validate_sources = AsyncMock()
    monkeypatch.setattr(
        ptg,
        "_reused_shared_v3_serving_index",
        lambda *_args, **_kwargs: dict(serving_index_by_field),
    )
    monkeypatch.setattr(
        ptg,
        "_shared_v3_source_set_metadata",
        lambda *_args, **_kwargs: {"source_count": 1},
    )
    monkeypatch.setattr(
        ptg,
        "_publish_shared_v3_source_dictionary",
        AsyncMock(return_value=(assignment,)),
    )
    monkeypatch.setattr(
        ptg,
        "validate_reused_tax_identity_source_projection",
        validate_projection,
    )
    monkeypatch.setattr(
        ptg,
        "validate_reused_snapshot_sources",
        validate_sources,
    )
    publication, evidence = _reuse_publication_and_evidence()
    return (
        metadata_by_field,
        aggregate_metadata_by_field,
        validate_projection,
        validate_sources,
        publication,
        evidence,
    )


@pytest.mark.asyncio
async def test_reuse_validates_sealed_source_projection_without_rescanning(
    monkeypatch,
):
    """Validate sealed source evidence without opening deleted sidecars."""

    (
        metadata_by_field,
        aggregate_metadata_by_field,
        validate_projection,
        validate_sources,
        publication,
        evidence,
    ) = _patched_reuse_fixture(monkeypatch)

    reused = await ptg._publish_reused_serving_metadata(publication, evidence)

    assert reused["source_set"] == {"source_count": 1}
    validate_projection.assert_awaited_once_with(
        schema_name=ptg.resolve_ptg2_schema(),
        snapshot_key=17,
        expected_bindings=(
            {
                "contract": "ptg2_tax_identity_rate_source_binding_v1",
                "source_type": "in_network",
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": "1" * 64,
                "source_key": 0,
            },
        ),
        sealed_metadata=metadata_by_field,
        aggregate_metadata=aggregate_metadata_by_field,
    )
    validate_sources.assert_awaited_once()
