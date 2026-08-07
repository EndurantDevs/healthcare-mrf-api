# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Security and privacy checks for signed singleton-direct PTG input."""

from __future__ import annotations

import copy
import hashlib
import json

import pytest

from api.control_import_wave_attestation import ATTESTATION_VERSION
from api.control_import_waves import (
    MAX_ATTESTATION_CANONICAL_BYTES,
    MAX_INTENT_CANONICAL_BYTES,
    WORKER_LIMIT,
    sign_cohort_attestation,
    validate_import_wave_payload,
)
from api.control_imports import _retry_child_params, normalize_run
from process.ptg import _direct_in_network_job
from process.ptg_control_failures import ptg_failure_error
from process.ptg_parts.frozen_rate_files import FROZEN_RATE_FILE_SET_CONTRACT
from process.ptg_parts.source_download import _download_failure
from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_CONTRACT,
    DIRECT_RATE_FILE_INTENT_FIELD,
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    DIRECT_RATE_FILE_PUBLIC_MARKER,
    PTG_SMALL_RESOURCE_CONTRACT,
    SingletonDirectValidationError,
    normalize_protected_singleton_direct_params,
    singleton_direct_failure_payload,
    singleton_direct_intent_sha256,
    singleton_direct_main_kwargs,
    singleton_direct_source_key,
    validated_worker_singleton_direct_params,
)


_KEY = "test-singleton-direct-key"


def _direct_params(ordinal: int = 0) -> dict:
    source_file_id = f"file-singleton-{ordinal}"
    source_import_id = f"import-singleton-{ordinal}"
    content_version = f"content-singleton-{ordinal}"
    canonical_url = (
        f"https://files.example.test/rates-{ordinal}.json.gz"
    )
    source_key = singleton_direct_source_key(source_file_id)
    direct_intent_map = {
        "contract": DIRECT_RATE_FILE_INTENT_CONTRACT,
        "source_file_import_id": source_import_id,
        "source_file_id": source_file_id,
        "content_version": content_version,
        "source_type": "in_network",
        "canonical_url": canonical_url,
        "source_key": source_key,
        "content_file_count": 1,
    }
    return {
        "version": 2,
        "importer": "ptg",
        "operation_id": "wave-singleton",
        "source_file_import_id": source_import_id,
        "import_id": source_import_id,
        "source_file_id": source_file_id,
        "content_version": content_version,
        "import_month": "2026-08",
        "node_id": "node-singleton",
        "use_stored_catalog": True,
        DIRECT_RATE_FILE_INTENT_FIELD: direct_intent_map,
        DIRECT_RATE_FILE_INTENT_SHA256_FIELD: (
            singleton_direct_intent_sha256(direct_intent_map)
        ),
        "ptg_resource": copy.deepcopy(PTG_SMALL_RESOURCE_CONTRACT),
        "source_key": source_key,
        "plan_ids": [f"plan-{ordinal}"],
        "plan_market_types": ["group"],
        "in_network_url": canonical_url,
        "max_files": 1,
    }


def _payload(count: int = 1) -> dict:
    params = [_direct_params(ordinal) for ordinal in range(count)]
    intents = [
        {
            "ordinal": ordinal,
            "run_id": f"run-singleton-{ordinal}",
            "source_file_import_id": params_by_name["source_file_import_id"],
            "content_version": params_by_name["content_version"],
            "params": params_by_name,
        }
        for ordinal, params_by_name in enumerate(params)
    ]
    imported_digest = hashlib.sha256(
        "\0".join(
            f"{params_by_name['source_file_import_id']}\0"
            f"{params_by_name['content_version']}"
            for params_by_name in params
        ).encode("utf-8")
    ).hexdigest()
    unsigned_attestation_map = {
        "schema_version": ATTESTATION_VERSION,
        "wave_id": "wave-singleton",
        "idempotency_key": "wave-singleton-key",
        "snapshot": {
            "authorization_basis": (
                "complete_subscriptions_and_client_visible_bindings_v1"
            ),
            "authorization_digest": "7" * 64,
            "snapshot_digest": "a" * 64,
            "membership_digest": "b" * 64,
            "inventory_digest": "c" * 64,
            "subscription_coverage_digest": "d" * 64,
            "entitlement_coverage_count": 0,
            "entitlement_coverage_digest": "8" * 64,
            "catalog_generation": "9" * 64,
        },
        "partition": {
            "complete": True,
            "physical_coordinate_count": count,
            "physical_coordinate_digest": "e" * 64,
            "imported_coordinate_count": count,
            "imported_coordinate_digest": imported_digest,
            "reused_coordinate_count": 0,
            "reused_coordinate_digest": "0" * 64,
            "partition_digest": "f" * 64,
        },
        "intents": intents,
    }
    return {
        "cohort_attestation": {
            **unsigned_attestation_map,
            "signature": sign_cohort_attestation(
                unsigned_attestation_map,
                key=_KEY,
            ),
        }
    }


def _resign_payload(payload: dict) -> None:
    attestation = payload["cohort_attestation"]
    unsigned_attestation_map = {
        name: intent_field_value
        for name, intent_field_value in attestation.items()
        if name != "signature"
    }
    attestation["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )


def test_singleton_direct_contract_accepts_only_exact_matching_tuple():
    params = _direct_params()

    normalized = normalize_protected_singleton_direct_params(params)

    assert normalized == params
    assert normalized["max_files"] == 1
    assert normalized["in_network_url"] == (
        normalized[DIRECT_RATE_FILE_INTENT_FIELD]["canonical_url"]
    )
    assert validated_worker_singleton_direct_params(
        {
            "source_file_import_id": params["source_file_import_id"],
            "import_id": params["import_id"],
        },
        params,
    ) == params
    with pytest.raises(SingletonDirectValidationError, match="identities"):
        validated_worker_singleton_direct_params(
            {
                "source_file_import_id": "different-import",
                "import_id": params["import_id"],
            },
            params,
        )
    assert singleton_direct_main_kwargs(params) == {
        DIRECT_RATE_FILE_INTENT_SHA256_FIELD: params[
            DIRECT_RATE_FILE_INTENT_SHA256_FIELD
        ]
    }


def test_ordinary_scalar_direct_import_remains_outside_protected_contract():
    ordinary_params_by_name = {
        "in_network_url": "https://public.example.test/rates.json.gz",
        "max_files": 1,
        "source_key": "ordinary-source",
    }

    assert normalize_protected_singleton_direct_params(
        ordinary_params_by_name
    ) == ordinary_params_by_name
    assert validated_worker_singleton_direct_params(
        {}, ordinary_params_by_name
    ) == ordinary_params_by_name
    public_params = normalize_run(
        {
            "run_id": "ordinary-run",
            "importer": "ptg",
            "status": "queued",
            "params": ordinary_params_by_name,
        }
    )["params"]
    assert public_params == {"max_files": 1}
    assert DIRECT_RATE_FILE_PUBLIC_MARKER not in public_params
    assert DIRECT_RATE_FILE_INTENT_SHA256_FIELD not in public_params


@pytest.mark.parametrize(
    "mutation",
    (
        lambda params: params.pop(DIRECT_RATE_FILE_INTENT_SHA256_FIELD),
        lambda params: params.__setitem__("version", 1),
        lambda params: params.__setitem__("max_files", 2),
        lambda params: params.pop("in_network_url"),
        lambda params: params.__setitem__(
            "in_network_url",
            "https://files.example.test/changed.json.gz",
        ),
        lambda params: params[DIRECT_RATE_FILE_INTENT_FIELD].__setitem__(
            "canonical_url",
            "https://files.example.test/changed.json.gz",
        ),
        lambda params: params.__setitem__(
            "frozen_rate_file_set_contract",
            FROZEN_RATE_FILE_SET_CONTRACT,
        ),
        lambda params: params.__setitem__("toc_urls", []),
        lambda params: params[DIRECT_RATE_FILE_INTENT_FIELD].pop(
            "source_type"
        ),
        lambda params: params[DIRECT_RATE_FILE_INTENT_FIELD].__setitem__(
            "source_type",
            "unsupported",
        ),
        lambda params: params[DIRECT_RATE_FILE_INTENT_FIELD].__setitem__(
            "source_key",
            "ptg_wrong",
        ),
        lambda params: params[DIRECT_RATE_FILE_INTENT_FIELD].__setitem__(
            "source_file_id",
            "",
        ),
        lambda params: params[DIRECT_RATE_FILE_INTENT_FIELD].__setitem__(
            "canonical_url",
            "https://files.example.test:bad/rates.json.gz",
        ),
        lambda params: params[DIRECT_RATE_FILE_INTENT_FIELD].__setitem__(
            "canonical_url",
            "https://files.example.test:443/rates.json.gz",
        ),
    ),
)
def test_singleton_direct_contract_rejects_partial_tampered_or_mixed_input(
    mutation,
):
    params = copy.deepcopy(_direct_params())
    mutation(params)

    with pytest.raises(SingletonDirectValidationError):
        normalize_protected_singleton_direct_params(params)


def test_signed_wave_accepts_direct_intent_and_rejects_resigned_tamper():
    payload = _payload()
    validated = validate_import_wave_payload(payload, attestation_key=_KEY)
    assert validated["intents"][0]["params"] == _direct_params()

    changed = copy.deepcopy(payload)
    changed["cohort_attestation"]["intents"][0]["params"][
        "in_network_url"
    ] = "https://files.example.test/changed.json.gz"
    _resign_payload(changed)
    with pytest.raises(ValueError, match="conflicts"):
        validate_import_wave_payload(changed, attestation_key=_KEY)


def test_signed_wave_rejects_outer_direct_content_version_mismatch():
    changed = copy.deepcopy(_payload())
    changed["cohort_attestation"]["intents"][0][
        "content_version"
    ] = "different-content-version"
    _resign_payload(changed)

    with pytest.raises(ValueError, match="coordinate conflicts"):
        validate_import_wave_payload(changed, attestation_key=_KEY)


@pytest.mark.parametrize(
    "mutation",
    (
        lambda params: params.pop("ptg_resource"),
        lambda params: params.__setitem__(
            "operation_id",
            "different-wave",
        ),
        lambda params: params.__setitem__("synthetic_extra", True),
        lambda params: params.__setitem__(
            "plan_ids",
            ["plan-z", "plan-a"],
        ),
    ),
)
def test_signed_wave_rejects_inexact_direct_outer_contract(mutation):
    changed = copy.deepcopy(_payload())
    changed_params = changed["cohort_attestation"]["intents"][0][
        "params"
    ]
    mutation(changed_params)
    _resign_payload(changed)

    with pytest.raises(ValueError):
        validate_import_wave_payload(changed, attestation_key=_KEY)


def test_direct_marker_cannot_bypass_per_intent_limit_with_v1_version():
    changed = copy.deepcopy(_payload())
    changed_params = changed["cohort_attestation"]["intents"][0][
        "params"
    ]
    changed_params["version"] = 1
    changed_params["synthetic_padding"] = "x" * (
        MAX_INTENT_CANONICAL_BYTES + 1
    )
    _resign_payload(changed)

    with pytest.raises(ValueError, match="canonical byte limit"):
        validate_import_wave_payload(changed, attestation_key=_KEY)


def test_full_live_shape_fits_one_bounded_twelve_worker_attestation():
    payload = _payload(3_586)
    canonical_bytes = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")

    validated = validate_import_wave_payload(payload, attestation_key=_KEY)

    assert len(validated["intents"]) == 3_586
    assert len(canonical_bytes) < MAX_ATTESTATION_CANONICAL_BYTES
    assert WORKER_LIMIT == 12


def test_direct_worker_progress_and_public_run_projection_are_private():
    params = _direct_params()
    direct_intent = params[DIRECT_RATE_FILE_INTENT_FIELD]
    digest = params[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]
    job = _direct_in_network_job(
        params["in_network_url"],
        plan_info=[{"plan_id": "plan-0"}],
        source_network_names=[],
        private_intent_sha256=digest,
    )

    assert job["_ptg_progress_private"] is True
    assert job["_ptg_progress_label"] == f"direct-singleton-{digest[:12]}"
    assert params["in_network_url"] not in job["_ptg_progress_label"]

    normalized = normalize_run(
        {
            "run_id": "run-singleton",
            "importer": "ptg",
            "status": "failed",
            "params": params,
            "progress": {
                "message": f"failed {direct_intent['canonical_url']}"
            },
            "metrics": {
                "source_file_versions": [direct_intent],
                DIRECT_RATE_FILE_INTENT_SHA256_FIELD: digest,
            },
            "error": {
                "message": f"failed {direct_intent['source_key']}"
            },
        }
    )
    assert normalized["params"][DIRECT_RATE_FILE_PUBLIC_MARKER] is True
    assert normalized["params"][DIRECT_RATE_FILE_INTENT_SHA256_FIELD] == digest
    assert normalized["params"]["max_files"] == 1
    rendered = json.dumps(normalized, sort_keys=True)
    assert direct_intent["canonical_url"] not in rendered
    assert direct_intent["source_key"] not in rendered
    assert DIRECT_RATE_FILE_INTENT_FIELD not in normalized["params"]

    with pytest.raises(ValueError, match="cannot be retried"):
        _retry_child_params(
            {"importer": "ptg", "params": normalized["params"]},
            "run-singleton",
            {},
        )


def test_private_direct_download_failure_does_not_reflect_url():
    private_url = "https://files.example.test/private-rates.json.gz"
    job_map = {
        "type": "in_network",
        "url": private_url,
        "_ptg_progress_private": True,
        "_ptg_progress_label": "direct-singleton-deadbeef0000",
    }

    failure = _download_failure(
        job_map,
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

    assert digest == (
        "bdcd799a22207de0d41aa72dd4339b6208d0ee312ff1b404f475bdd3d85e067d"
    )
