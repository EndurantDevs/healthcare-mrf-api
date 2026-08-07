# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
import json

import pytest

from api import billing_search_tin_policy


POLICY_ID = "ptg-tin-hmac-sha256-v1:synthetic-a"


def _synthetic_ein(*, formatted: bool = False) -> str:
    digits = f"{12:02d}{3_456_789:07d}"
    return f"{digits[:2]}-{digits[2:]}" if formatted else digits


def _environment(secret_file, **document_updates):
    document_by_field = {
        "contract": (
            billing_search_tin_policy.BILLING_SEARCH_TIN_POLICY_FILES_CONTRACT
        ),
        "policies": [
            {
                "token_policy_id": POLICY_ID,
                "secret_file": str(secret_file),
            }
        ],
    }
    document_by_field.update(document_updates)
    return {
        billing_search_tin_policy.BILLING_SEARCH_TIN_POLICY_FILES_ENV: (
            json.dumps(document_by_field, separators=(",", ":"), sort_keys=True)
        )
    }


def _secret_file(tmp_path):
    secret_file = tmp_path / "billing-search-token.key"
    secret_file.write_bytes(bytes(range(32)))
    secret_file.chmod(0o400)
    return secret_file


def test_loader_returns_only_the_requested_configured_policy(tmp_path):
    secret_file = _secret_file(tmp_path)

    policy = billing_search_tin_policy.load_billing_search_tin_policy(
        POLICY_ID,
        _environment(secret_file),
    )
    token = policy.tokenize_ein(_synthetic_ein(formatted=True))

    assert policy.token_policy_id == POLICY_ID
    assert token.token_policy_id == POLICY_ID
    assert len(token.tin_id_128) == 16
    assert len(token.tin_hmac_sha256) == 32
    assert _synthetic_ein() not in repr(policy)


@pytest.mark.parametrize(
    "environment",
    [
        {},
        {
            billing_search_tin_policy.BILLING_SEARCH_TIN_POLICY_FILES_ENV: "{}"
        },
        {
            billing_search_tin_policy.BILLING_SEARCH_TIN_POLICY_FILES_ENV: (
                '{"contract":"healthporta.billing-search-tin-policy-files.v1",'
                '"contract":"duplicate","policies":[]}'
            )
        },
    ],
)
def test_loader_fails_closed_without_one_exact_document(environment):
    with pytest.raises(
        billing_search_tin_policy.BillingSearchTinPolicyError,
        match="^billing_search_tin_policy_unavailable$",
    ):
        billing_search_tin_policy.load_billing_search_tin_policy(
            POLICY_ID,
            environment,
        )


def test_loader_rejects_unknown_policy_without_exposing_configuration(tmp_path):
    secret_file = _secret_file(tmp_path)

    with pytest.raises(
        billing_search_tin_policy.BillingSearchTinPolicyError,
        match="^billing_search_tin_policy_unavailable$",
    ) as error:
        billing_search_tin_policy.load_billing_search_tin_policy(
            "ptg-tin-hmac-sha256-v1:synthetic-b",
            _environment(secret_file),
        )

    assert str(secret_file) not in str(error.value)
    assert POLICY_ID not in str(error.value)


def test_loader_rejects_non_private_secret_file(tmp_path):
    secret_file = _secret_file(tmp_path)
    secret_file.chmod(0o600)

    with pytest.raises(
        billing_search_tin_policy.BillingSearchTinPolicyError,
        match="^billing_search_tin_policy_unavailable$",
    ):
        billing_search_tin_policy.load_billing_search_tin_policy(
            POLICY_ID,
            _environment(secret_file),
        )


def test_policy_document_rejects_duplicate_file_paths(tmp_path):
    secret_file = _secret_file(tmp_path)
    document_by_field = {
        "contract": (
            billing_search_tin_policy.BILLING_SEARCH_TIN_POLICY_FILES_CONTRACT
        ),
        "policies": [
            {
                "token_policy_id": POLICY_ID,
                "secret_file": str(secret_file),
            },
            {
                "token_policy_id": "ptg-tin-hmac-sha256-v1:synthetic-b",
                "secret_file": str(secret_file),
            },
        ],
    }
    environment_by_name = {
        billing_search_tin_policy.BILLING_SEARCH_TIN_POLICY_FILES_ENV: (
            json.dumps(document_by_field)
        )
    }

    with pytest.raises(billing_search_tin_policy.BillingSearchTinPolicyError):
        billing_search_tin_policy.load_billing_search_tin_policy(
            POLICY_ID,
            environment_by_name,
        )


def test_public_module_contains_no_secret_material(tmp_path):
    secret_file = _secret_file(tmp_path)
    policy = billing_search_tin_policy.load_billing_search_tin_policy(
        POLICY_ID,
        _environment(secret_file),
    )

    encoded_secret = base64.urlsafe_b64encode(bytes(range(32))).decode("ascii")
    assert encoded_secret not in repr(policy)
    assert encoded_secret not in repr(billing_search_tin_policy)


def test_loader_failure_drops_environment_document_from_traceback_frames() -> None:
    marker = "synthetic-policy-traceback-marker"
    raw_document = json.dumps(
        {
            "contract": (
                billing_search_tin_policy.BILLING_SEARCH_TIN_POLICY_FILES_CONTRACT
            ),
            "policies": [
                {
                    "secret_file": f"/tmp/{marker}",
                    "token_policy_id": POLICY_ID,
                }
            ],
        }
    )
    environment_by_name = {
        billing_search_tin_policy.BILLING_SEARCH_TIN_POLICY_FILES_ENV: (
            raw_document
        )
    }

    with pytest.raises(
        billing_search_tin_policy.BillingSearchTinPolicyError
    ) as captured:
        billing_search_tin_policy.load_billing_search_tin_policy(
            POLICY_ID,
            environment_by_name,
        )

    traceback = captured.value.__traceback__
    loader_locals = []
    while traceback is not None:
        if traceback.tb_frame.f_globals.get("__name__") == (
            "api.billing_search_tin_policy"
        ):
            loader_locals.append(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next
    assert loader_locals
    assert captured.value.__context__ is None
    assert all(
        all(
            retained is not environment_by_name and retained is not raw_document
            for retained in local_values.values()
        )
        and marker not in repr(local_values)
        for local_values in loader_locals
    )


def test_loader_rejects_nonmapping_environment_without_retention() -> None:
    with pytest.raises(billing_search_tin_policy.BillingSearchTinPolicyError):
        billing_search_tin_policy.load_billing_search_tin_policy(
            POLICY_ID,
            object(),
        )
