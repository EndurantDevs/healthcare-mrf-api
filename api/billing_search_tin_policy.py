# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed token-policy capabilities for raw billing-search EINs."""

from __future__ import annotations

from functools import lru_cache
import json
import os
from pathlib import Path
from typing import Mapping

from process.tin_npi_connector_security import (
    TinTokenProjector,
    canonical_token_policy_id,
    load_tin_token_policy,
)
from process.tin_npi_connector_support import TinNpiConnectorError


BILLING_SEARCH_TIN_POLICY_FILES_CONTRACT = (
    "healthporta.billing-search-tin-policy-files.v1"
)
BILLING_SEARCH_TIN_POLICY_FILES_ENV = (
    "HLTHPRT_BILLING_SEARCH_TIN_POLICY_FILES_JSON"
)

_INVALID = "billing_search_tin_policy_unavailable"
_MAX_DOCUMENT_BYTES = 8192
_MAX_POLICIES = 8
_DOCUMENT_FIELDS = frozenset({"contract", "policies"})
_POLICY_FIELDS = frozenset({"secret_file", "token_policy_id"})


class BillingSearchTinPolicyError(RuntimeError):
    """Value-free policy failure safe for the HTTP boundary."""


def _fail() -> BillingSearchTinPolicyError:
    return BillingSearchTinPolicyError(_INVALID)


def _unique_json_object(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value_by_key: dict[str, object] = {}
    for key, value in pairs:
        if key in value_by_key:
            raise _fail()
        value_by_key[key] = value
    return value_by_key


def _reject_json_number(_encoded_number: str) -> None:
    raise _fail()


def _parsed_document(raw_document: object) -> dict[str, object]:
    if (
        type(raw_document) is not str
        or not 1 <= len(raw_document.encode("utf-8")) <= _MAX_DOCUMENT_BYTES
        or not raw_document.isascii()
    ):
        raise _fail()
    try:
        parsed = json.loads(
            raw_document,
            object_pairs_hook=_unique_json_object,
            parse_int=_reject_json_number,
            parse_float=_reject_json_number,
            parse_constant=_reject_json_number,
        )
    except BillingSearchTinPolicyError:
        raise
    except (TypeError, ValueError, UnicodeError):
        raise _fail() from None
    if type(parsed) is not dict or frozenset(parsed) != _DOCUMENT_FIELDS:
        raise _fail()
    return parsed


def _canonical_secret_file(value: object) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 1024
        or value != value.strip()
        or not value.isascii()
        or not value.isprintable()
    ):
        raise _fail()
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise _fail()
    return str(path)


def _policy_files(document: dict[str, object]) -> dict[str, str]:
    if document.get("contract") != BILLING_SEARCH_TIN_POLICY_FILES_CONTRACT:
        raise _fail()
    entries = document.get("policies")
    if type(entries) is not list or not 1 <= len(entries) <= _MAX_POLICIES:
        raise _fail()
    policy_file_by_id: dict[str, str] = {}
    retained_file_paths: set[str] = set()
    for raw_entry in entries:
        if type(raw_entry) is not dict or frozenset(raw_entry) != _POLICY_FIELDS:
            raise _fail()
        try:
            policy_id = canonical_token_policy_id(
                raw_entry.get("token_policy_id")
            )
        except TinNpiConnectorError:
            raise _fail() from None
        secret_file = _canonical_secret_file(raw_entry.get("secret_file"))
        if policy_id in policy_file_by_id or secret_file in retained_file_paths:
            raise _fail()
        policy_file_by_id[policy_id] = secret_file
        retained_file_paths.add(secret_file)
    return policy_file_by_id


@lru_cache(maxsize=_MAX_POLICIES)
def _load_projector(policy_id: str, secret_file: str) -> TinTokenProjector:
    try:
        return load_tin_token_policy(
            token_policy_id=policy_id,
            secret_file=secret_file,
        )
    except TinNpiConnectorError:
        raise _fail() from None


def load_billing_search_tin_policy(
    token_policy_id: object,
    environment_map: Mapping[str, str] | None = None,
) -> TinTokenProjector:
    """Load one explicitly configured policy without exposing secret paths."""

    try:
        canonical_policy_id = canonical_token_policy_id(token_policy_id)
        environment = os.environ if environment_map is None else environment_map
        if not isinstance(environment, Mapping):
            raise _fail()
        raw_document = environment.get(BILLING_SEARCH_TIN_POLICY_FILES_ENV)
        policy_files = _policy_files(_parsed_document(raw_document))
        secret_file = policy_files.get(canonical_policy_id)
        if secret_file is None:
            raise _fail()
        return _load_projector(canonical_policy_id, secret_file)
    except BillingSearchTinPolicyError:
        raise
    except (TinNpiConnectorError, TypeError, ValueError):
        raise _fail() from None


__all__ = [
    "BILLING_SEARCH_TIN_POLICY_FILES_CONTRACT",
    "BILLING_SEARCH_TIN_POLICY_FILES_ENV",
    "BillingSearchTinPolicyError",
    "load_billing_search_tin_policy",
]
