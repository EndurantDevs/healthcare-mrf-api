# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Runtime role and privacy checks for singleton-direct PTG input."""

from __future__ import annotations

import json

import pytest

from api.control_imports import _retry_child_params, normalize_run
from process.ptg import _direct_allowed_amounts_job, _direct_in_network_job
from process.ptg_control_failures import ptg_failure_error
from process.ptg_parts.source_download import _download_failure
from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_FIELD,
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    DIRECT_RATE_FILE_PUBLIC_MARKER,
    SingletonDirectValidationError,
    singleton_direct_failure_payload,
)
from tests.ptg_singleton_direct_test_support import _direct_params


def _direct_job(
    params_by_name: dict,
    selector_field: str,
    digest: str,
) -> dict:
    if selector_field == "allowed_url":
        return _direct_allowed_amounts_job(
            params_by_name[selector_field],
            plan_info=[{"plan_id": "plan-0"}],
            private_intent_sha256=digest,
        )
    return _direct_in_network_job(
        params_by_name[selector_field],
        plan_info=[{"plan_id": "plan-0"}],
        source_network_names=[],
        private_intent_sha256=digest,
    )


def _failed_run_projection(params_by_name: dict) -> dict:
    direct_intent = params_by_name[DIRECT_RATE_FILE_INTENT_FIELD]
    digest = params_by_name[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]
    return normalize_run(
        {
            "run_id": "run-singleton",
            "importer": "ptg",
            "status": "failed",
            "params": params_by_name,
            "progress": {
                "message": f"failed {direct_intent['canonical_url']}"
            },
            "metrics": {
                "source_file_versions": [direct_intent],
                DIRECT_RATE_FILE_INTENT_SHA256_FIELD: digest,
            },
            "error": {"message": f"failed {direct_intent['source_key']}"},
        }
    )


@pytest.mark.parametrize("source_type", ("in_network", "allowed_amounts"))
def test_direct_worker_progress_and_public_run_projection_are_private(
    source_type,
):
    """Hide both direct selectors from worker progress and API output."""

    params_by_name = _direct_params(source_type=source_type)
    direct_intent = params_by_name[DIRECT_RATE_FILE_INTENT_FIELD]
    digest = params_by_name[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]
    selector_field = (
        "allowed_url"
        if source_type == "allowed_amounts"
        else "in_network_url"
    )
    job = _direct_job(params_by_name, selector_field, digest)

    assert job["_ptg_progress_private"] is True
    assert job["_ptg_progress_label"] == f"direct-singleton-{digest[:12]}"
    assert params_by_name[selector_field] not in job["_ptg_progress_label"]

    normalized = _failed_run_projection(params_by_name)
    assert normalized["params"][DIRECT_RATE_FILE_PUBLIC_MARKER] is True
    assert normalized["params"][DIRECT_RATE_FILE_INTENT_SHA256_FIELD] == digest
    assert normalized["params"]["max_files"] == 1
    rendered = json.dumps(normalized, sort_keys=True)
    assert params_by_name[selector_field] not in rendered
    assert direct_intent["canonical_url"] not in rendered
    assert direct_intent["source_key"] not in rendered
    assert DIRECT_RATE_FILE_INTENT_FIELD not in normalized["params"]

    with pytest.raises(ValueError, match="cannot be retried"):
        _retry_child_params(
            {"importer": "ptg", "params": normalized["params"]},
            "run-singleton",
            {},
        )


def test_direct_allowed_job_uses_allowed_lane_and_private_progress():
    params_by_name = _direct_params(source_type="allowed_amounts")
    digest = params_by_name[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]

    job = _direct_allowed_amounts_job(
        params_by_name["allowed_url"],
        plan_info=[{"plan_id": "plan-0"}],
        private_intent_sha256=digest,
    )

    assert job["type"] == "allowed_amounts"
    assert job["url"] == params_by_name["allowed_url"]
    assert job["plan_info"] == [{"plan_id": "plan-0"}]
    assert job["_ptg_progress_private"] is True
    assert job["_ptg_progress_label"] == f"direct-singleton-{digest[:12]}"
    assert params_by_name["allowed_url"] not in job["_ptg_progress_label"]


def test_private_direct_download_failure_does_not_reflect_url():
    private_url = "https://files.example.test/private-rates.json.gz"
    job_by_field = {
        "type": "in_network",
        "url": private_url,
        "_ptg_progress_private": True,
        "_ptg_progress_label": "direct-singleton-deadbeef0000",
    }

    failure = _download_failure(
        job_by_field,
        RuntimeError(f"download failed for {private_url}"),
    )

    assert failure.error is not None
    assert private_url not in failure.error
    assert "direct-singleton-deadbeef0000" in failure.error


def test_direct_contract_failure_is_classified_without_private_reflection():
    private_selector = "https://files.example.test/private-selector.json.gz"
    error = SingletonDirectValidationError(private_selector)

    assert singleton_direct_failure_payload([error]) == {
        "code": "ptg_singleton_direct_contract_failed",
        "message": "protected singleton direct input is invalid",
        "retryable": False,
    }
    classified = ptg_failure_error(error)
    assert classified["code"] == "ptg_singleton_direct_contract_failed"
    assert private_selector not in repr(classified)


def test_direct_intent_digest_matches_cross_service_vector():
    digest = _direct_params()[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]
    allowed_digest = _direct_params(
        source_type="allowed_amounts"
    )[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]

    assert digest == (
        "bdcd799a22207de0d41aa72dd4339b6208d0ee312ff1b404f475bdd3d85e067d"
    )
    assert allowed_digest == (
        "2446d9402396c1c9e387d0c01b4a8f6c4b8f47c01174dbfa0ee9c438d22e3e93"
    )
