# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control and candidate proofs for frozen multipart PTG dispatch."""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib

import pytest

from api import control
from process import ptg_control
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileValidationError,
    frozen_rate_file_set_sha256,
    normalize_frozen_rate_file_set,
)

ptg = importlib.import_module("process.ptg")


def _descriptor_by_ordinal(ordinal: int) -> dict[str, object]:
    return {
        "source_type": "in_network",
        "canonical_url": (
            f"https://rates.example.com/2026-07/part-{ordinal:03}.json.gz"
        ),
        "content_length": 10_000 + ordinal,
        "etag": f'"part-{ordinal:03}-v1"',
        "last_modified": "Mon, 27 Jul 2026 10:00:00 GMT",
        "raw_sha256": hashlib.sha256(f"raw:{ordinal}".encode()).hexdigest(),
        "logical_sha256": hashlib.sha256(
            f"logical:{ordinal}".encode()
        ).hexdigest(),
        "logical_hash_deferred": False,
        "engine_source_identity_hash": f"{ordinal:016x}",
        "engine_source_file_version_id": f"{ordinal + 1024:016x}",
        "ordinal": ordinal,
    }


def _frozen_set(count: int) -> tuple[list[dict[str, object]], str]:
    files = [
        _descriptor_by_ordinal(ordinal)
        for ordinal in range(1, count + 1)
    ]
    return files, frozen_rate_file_set_sha256(files)


def test_control_api_normalizes_internal_engine_envelope_before_persistence():
    files, digest = _frozen_set(2)
    request_payload = control._validated_control_import_payload(
        {
            "importer": "ptg",
            "params": {
                "frozen_rate_files": list(reversed(files)),
                "frozen_rate_file_set_sha256": digest,
            },
        }
    )

    assert [
        descriptor["ordinal"]
        for descriptor in request_payload["params"]["frozen_rate_files"]
    ] == [1, 2]
    assert request_payload["params"]["frozen_rate_file_set_sha256"] == digest


def test_public_single_file_adapter_does_not_accept_multipart_envelope():
    files, digest = _frozen_set(2)

    with pytest.raises(ValueError, match="internal import engine payload"):
        control._ptg_import_file_payload(
            {
                "params": {
                    "frozen_rate_files": files,
                    "frozen_rate_file_set_sha256": digest,
                }
            }
        )


def test_worker_revalidates_envelope_and_mutual_exclusion_before_run_claim():
    files, digest = _frozen_set(2)
    normalized_params = ptg_control._validated_frozen_rate_params(
        {
            "frozen_rate_files": list(reversed(files)),
            "frozen_rate_file_set_sha256": digest,
        }
    )
    assert [
        descriptor["ordinal"]
        for descriptor in normalized_params["frozen_rate_files"]
    ] == [1, 2]

    with pytest.raises(FrozenRateFileValidationError, match="mutually exclusive"):
        ptg_control._validated_frozen_rate_params(
            {
                **normalized_params,
                "in_network_url": (
                    "https://rates.example.com/scalar.json.gz"
                ),
            }
        )


def test_set_digest_and_count_bind_import_and_snapshot_identities():
    files, digest = _frozen_set(2)
    import_id = ptg._frozen_ptg2_import_id(
        ptg.normalize_import_month("2026-07"),
        "source-a",
        frozen_rate_file_set_sha256=digest,
        frozen_rate_file_count=2,
        arch_variant="shared_v4",
    )
    different_import_id = ptg._frozen_ptg2_import_id(
        ptg.normalize_import_month("2026-07"),
        "source-a",
        frozen_rate_file_set_sha256="f" * 64,
        frozen_rate_file_count=2,
        arch_variant="shared_v4",
    )
    snapshot_options = ptg._ptg2_snapshot_content_options(
        {
            "frozen_rate_files": files,
            "frozen_rate_file_set_sha256": digest,
            "frozen_rate_file_count": 2,
        }
    )

    assert import_id != different_import_id
    assert snapshot_options["frozen_rate_file_set_sha256"] == digest
    assert snapshot_options["frozen_rate_file_count"] == 2
    assert "frozen_rate_files" not in snapshot_options


def test_candidate_redelivery_keeps_complete_set_proof_for_v4_audit():
    files, digest = _frozen_set(2)
    normalized_files, _ = normalize_frozen_rate_file_set(files, digest)
    proof_rows = [
        {
            **descriptor,
            "contract": "ptg_frozen_rate_file_proof_v1",
            "raw_byte_count": descriptor["content_length"],
            "verification_mode": "downloaded",
        }
        for descriptor in normalized_files
    ]
    redelivery_result = ptg._already_published_result(
        snapshot_attributes={
            "import_run_id": "ptg2:frozen",
            "manifest": {
                "serving_index": {"serving_rates": 12},
                "source_file_versions": proof_rows,
                "frozen_rate_file_set_sha256": digest,
                "frozen_rate_file_count": 2,
                "frozen_rate_file_proof": proof_rows,
            },
        },
        snapshot_id="ptg2:202607:frozen",
        import_run_id="ptg2:frozen",
        source_key="source-a",
        import_month=dt.date(2026, 7, 1),
        pointer_reconciliation={"status": "current"},
    )

    assert redelivery_result["frozen_rate_file_set_sha256"] == digest
    assert redelivery_result["frozen_rate_file_count"] == 2
    assert redelivery_result["frozen_rate_file_proof"] == proof_rows
