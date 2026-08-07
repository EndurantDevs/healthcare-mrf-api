# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker and snapshot contracts for frozen multipart PTG dispatch."""

from __future__ import annotations

import datetime as dt
import importlib

import pytest

from api import control
from api import control_imports
from process.ptg_frozen_control import validated_frozen_rate_params
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
    frozen_rate_binding_from_params,
)
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileValidationError,
    normalize_frozen_rate_file_set,
)
from tests.ptg_frozen_test_support import (
    frozen_rate_file_set as _frozen_set,
    protected_control_payload as _protected_payload,
)


ptg = importlib.import_module("process.ptg")


def test_ptg_worker_adapter_preserves_outer_binding_ids():
    request = control._validated_control_import_payload(_protected_payload())
    run_values_by_name = {
        "run_id": "run-001",
        "importer": "ptg",
        "source_file_import_id": request["source_file_import_id"],
        "import_id": request["import_id"],
    }
    task_payload = control_imports._adapter_payload(
        {"payload": "ptg_control"},
        run_values_by_name,
        request["params"],
    )
    assert task_payload["source_file_import_id"] == "source-file-import-001"
    assert task_payload["import_id"] == "source-file-import-001"
    assert task_payload["params"]["source_file_import_id"] == (
        "source-file-import-001"
    )
    assert task_payload["params"]["import_id"] == "source-file-import-001"


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
    protected_params = _protected_payload()["params"]
    normalized_params = validated_frozen_rate_params(
        {
            **protected_params,
            "frozen_rate_files": list(
                reversed(protected_params["frozen_rate_files"])
            ),
        }
    )
    assert [
        descriptor["ordinal"]
        for descriptor in normalized_params["frozen_rate_files"]
    ] == [1, 2]
    with pytest.raises(FrozenRateFileValidationError, match="mutually exclusive"):
        validated_frozen_rate_params(
            {
                **normalized_params,
                "in_network_url": "https://rates.example.com/scalar.json.gz",
            }
        )


def test_set_digest_and_count_bind_import_and_snapshot_identities():
    files, digest = _frozen_set(2)
    binding = frozen_rate_binding_from_params(_protected_payload()["params"])
    assert binding is not None
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
            FROZEN_RATE_FILE_BINDING_OPTION: binding,
        }
    )
    assert import_id != different_import_id
    assert snapshot_options["frozen_rate_file_set_sha256"] == digest
    assert snapshot_options["frozen_rate_file_count"] == 2
    assert "frozen_rate_files" not in snapshot_options


def test_legacy_snapshot_identity_omits_absent_frozen_coordinates():
    legacy_options_by_name = {
        "toc_urls": ["https://rates.example.com/index.json"],
        "toc_list": None,
        "in_network_url": None,
        "allowed_url": None,
        "source_key": "source-a",
        "plan_ids": [],
        "plan_name_contains": [],
        "plan_market_types": [],
        "file_url_contains": [],
        "source_network_names": [],
        "max_files": 1,
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v4",
        "test_mode": False,
        "source_file_import_id": "legacy-source-file",
        "frozen_rate_file_set_contract": None,
        "frozen_rate_file_set_sha256": None,
        "frozen_rate_file_count": 0,
    }
    content_options = ptg._ptg2_snapshot_content_options(
        legacy_options_by_name
    )
    assert set(content_options) == set(
        ptg._PTG2_SNAPSHOT_CONTENT_OPTION_KEYS
    )
    assert not set(ptg._PTG2_FROZEN_SNAPSHOT_CONTENT_OPTION_KEYS).intersection(
        content_options
    )
    assert ptg._ptg2_deterministic_snapshot_id(
        import_month=dt.date(2026, 7, 1),
        import_id="legacy-import",
        option_by_name=legacy_options_by_name,
    ) == "ptg2:202607:4a1a9d98fa40"


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
