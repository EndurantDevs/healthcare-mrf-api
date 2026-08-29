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
from api.control_imports import normalize_run
from process.ptg import _normalized_direct_frozen_params
from process.ptg_parts.frozen_rate_binding import (
    FrozenRateFileBindingMismatchError,
)
from process.ptg_parts.frozen_rate_binding_store import (
    insert_or_compare_frozen_binding,
    recheck_frozen_binding_on_connection,
)
from process.ptg_parts.frozen_rate_files import FROZEN_RATE_FILE_SET_CONTRACT
from process.ptg_parts.ptg2_invalid_price_exclusion import (
    invalid_price_exclusion_policy,
    invalid_price_exclusion_source,
    invalid_price_value_sha256,
)
from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_FIELD,
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    DIRECT_RATE_FILE_PUBLIC_MARKER,
    SingletonDirectValidationError,
    normalize_protected_singleton_direct_params,
)
from process.ptg_frozen_control import (
    singleton_direct_main_kwargs,
    validated_worker_singleton_direct_params,
)
from tests.ptg_singleton_direct_test_support import _direct_params


_KEY = "test-singleton-direct-key"


def _invalid_price_policy(raw_source_sha256: str = "a" * 64) -> dict:
    return invalid_price_exclusion_policy(
        [
            invalid_price_exclusion_source(
                raw_source_sha256=raw_source_sha256,
                entries=[
                    {
                        "object_ordinal": 1,
                        "rate_ordinal": 2,
                        "price_ordinal": 3,
                        "invalid_value_sha256": invalid_price_value_sha256(
                            "2027-02-30"
                        ),
                    }
                ],
                emptied_rate_count=0,
            )
        ]
    )


def _payload(count: int = 1, *, source_type: str = "in_network") -> dict:
    """Build one signed synthetic exact-wave payload."""

    params = [
        _direct_params(ordinal, source_type=source_type)
        for ordinal in range(count)
    ]
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
            "signature": sign_cohort_attestation(unsigned_attestation_map, key=_KEY),
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


@pytest.mark.parametrize(
    ("source_type", "selector_field", "other_selector"),
    (
        ("in_network", "in_network_url", "allowed_url"),
        ("allowed_amounts", "allowed_url", "in_network_url"),
    ),
)
def test_singleton_direct_contract_accepts_only_exact_matching_tuple(
    source_type,
    selector_field,
    other_selector,
):
    params = _direct_params(source_type=source_type)

    normalized = normalize_protected_singleton_direct_params(params)

    assert normalized == params
    assert normalized["max_files"] == 1
    assert normalized[selector_field] == (
        normalized[DIRECT_RATE_FILE_INTENT_FIELD]["canonical_url"]
    )
    assert other_selector not in normalized
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


def test_singleton_direct_contract_binds_one_private_exclusion_policy():
    params = _direct_params()
    policy = _invalid_price_policy()
    params["invalid_price_exclusion_policy"] = policy

    normalized = normalize_protected_singleton_direct_params(params)

    assert normalized["invalid_price_exclusion_policy"] == policy
    assert singleton_direct_main_kwargs(normalized) == {
        DIRECT_RATE_FILE_INTENT_SHA256_FIELD: params[
            DIRECT_RATE_FILE_INTENT_SHA256_FIELD
        ],
        "invalid_price_exclusion_policy": policy,
    }

    changed = copy.deepcopy(_payload())
    changed["cohort_attestation"]["intents"][0]["params"][
        "invalid_price_exclusion_policy"
    ] = policy
    _resign_payload(changed)
    validated = validate_import_wave_payload(changed, attestation_key=_KEY)
    assert validated["intents"][0]["params"][
        "invalid_price_exclusion_policy"
    ] == policy

    params["invalid_price_exclusion_policy"] = invalid_price_exclusion_policy(
        [
            policy["sources"][0],
            _invalid_price_policy("b" * 64)["sources"][0],
        ]
    )
    with pytest.raises(SingletonDirectValidationError, match="one source"):
        normalize_protected_singleton_direct_params(params)

    allowed_params = _direct_params(source_type="allowed_amounts")
    allowed_params["invalid_price_exclusion_policy"] = policy
    with pytest.raises(SingletonDirectValidationError, match="in-network"):
        normalize_protected_singleton_direct_params(allowed_params)


@pytest.mark.asyncio
async def test_direct_policy_checks_frozen_collision_without_inserting():
    class BindingConnection:
        row = None

        async def scalar(self, *_args, **_kwargs):
            return 1

        async def status(self, *_args, **_kwargs):
            raise AssertionError("direct input must not create a frozen binding")

        async def all(self, *_args, **_kwargs):
            return [self.row] if self.row is not None else []

    params_by_name = _direct_params()
    params_by_name["invalid_price_exclusion_policy"] = _invalid_price_policy()
    params_by_name = _normalized_direct_frozen_params(params_by_name)
    connection = BindingConnection()

    assert await insert_or_compare_frozen_binding(
        connection,
        params_by_name,
    ) is None
    connection.row = {"binding_payload": {}}
    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="cannot be replayed as legacy",
    ):
        await recheck_frozen_binding_on_connection(
            connection,
            params_by_name,
        )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda params: params.pop(DIRECT_RATE_FILE_INTENT_SHA256_FIELD),
        lambda params: params.__setitem__("version", 1),
        lambda params: params.__setitem__("max_files", 2),
        lambda params: params.pop("in_network_url"),
        lambda params: params.__setitem__(
            "allowed_url",
            params["in_network_url"],
        ),
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


def test_singleton_direct_contract_rejects_role_selector_mismatch():
    params = _direct_params(source_type="allowed_amounts")
    params["in_network_url"] = params.pop("allowed_url")

    with pytest.raises(SingletonDirectValidationError, match="conflicts"):
        normalize_protected_singleton_direct_params(params)


@pytest.mark.parametrize(
    ("source_type", "selector_field"),
    (
        ("in_network", "in_network_url"),
        ("allowed_amounts", "allowed_url"),
    ),
)
def test_signed_wave_accepts_direct_intent_and_rejects_resigned_tamper(
    source_type,
    selector_field,
):
    payload = _payload(source_type=source_type)
    validated = validate_import_wave_payload(payload, attestation_key=_KEY)
    assert validated["intents"][0]["params"] == _direct_params(
        source_type=source_type
    )

    changed = copy.deepcopy(payload)
    changed["cohort_attestation"]["intents"][0]["params"][selector_field] = (
        "https://files.example.test/changed.json.gz"
    )
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


def test_signed_wave_accepts_mixed_direct_rate_roles():
    payload = _payload(count=2)
    payload["cohort_attestation"]["intents"][1]["params"] = _direct_params(
        1,
        source_type="allowed_amounts",
    )
    _resign_payload(payload)

    validated = validate_import_wave_payload(payload, attestation_key=_KEY)

    assert [
        intent["params"][DIRECT_RATE_FILE_INTENT_FIELD]["source_type"]
        for intent in validated["intents"]
    ] == ["in_network", "allowed_amounts"]


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


@pytest.mark.parametrize("source_type", ("in_network", "allowed_amounts"))
def test_full_live_shape_fits_one_bounded_twelve_worker_attestation(
    source_type,
):
    payload = _payload(3_586, source_type=source_type)
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
