# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed public evidence contract for the synthetic formulary seed canary."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import re
from typing import Any


CANARY_CONTRACT_VERSION = "formulary-fhir-synthetic-v1"
CANARY_ENABLED_ENV = "HLTHPRT_FHIR_FORMULARY_SYNTHETIC_CANARY_ENABLED"
SEED_PUBLICATION_ENABLED_ENV = (
    "HLTHPRT_FHIR_FORMULARY_SYNTHETIC_SEED_PUBLICATION_ENABLED"
)
CANARY_SOURCE_ID = "formulary-fhir-synthetic-v1"
CANARY_SOURCE_BASE = "https://formulary-fhir-canary.example.invalid/fhir"
CANARY_SOURCE_DISPLAY_NAME = "Synthetic Formulary Canary V1"
CANARY_RUN_ID = "formulary-fhir-synthetic-v1-seed"
CANARY_CUTOFF = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)
CANARY_LOCK_WAIT_SECONDS = 5.0
CANARY_LOCK_RETRY_SECONDS = 0.1
CANARY_TIMEOUT_SECONDS = 60
CANARY_PUBLICATION_TIMEOUT_SECONDS = 30
CANARY_FINAL_TABLE_COUNTS = {
    "fhir_formulary_source": 1,
    "fhir_formulary_dataset": 1,
    "fhir_formulary_current": 0,
    "fhir_formulary_coverage_plan": 1,
    "fhir_formulary_coverage_plan_version": 1,
    "fhir_formulary_dataset_coverage_plan": 1,
    "fhir_formulary_drug_plan_alias": 2,
    "fhir_formulary_drug_plan_alias_version": 2,
    "fhir_formulary_dataset_alias": 2,
    "fhir_formulary_medication": 2,
    "fhir_formulary_alias_membership": 2,
    "fhir_formulary_alternative": 1,
    "fhir_formulary_checkpoint": 2,
}
CANARY_PUBLISHED_TABLE_COUNTS = {
    **CANARY_FINAL_TABLE_COUNTS,
    "fhir_formulary_current": 1,
}
FIXTURE_ROOT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "smoke"
    / "fixtures"
    / "formulary_fhir"
)
EVIDENCE_HASH_FIELDS = (
    "source_configuration_hash",
    "acquisition_contract_hash",
    "coverage_hash",
    "membership_hash",
)
EVIDENCE_COUNT_FIELDS = (
    "list_count",
    "alias_count",
    "medication_membership_count",
    "request_count",
    "full_aliases",
    "reused_aliases",
    "resumed_aliases",
    "transient_retry_count",
    "throttle_count",
)
EXPECTED_EVIDENCE_COUNTS = {
    "list_count": 1,
    "alias_count": 2,
    "medication_membership_count": 2,
    "request_count": 9,
    "full_aliases": 2,
    "reused_aliases": 0,
    "resumed_aliases": 0,
    "transient_retry_count": 0,
    "throttle_count": 0,
}


class SyntheticCanaryContractError(RuntimeError):
    """Report one fixed synthetic contract mismatch without raw evidence."""


def canary_runtime_config() -> dict[str, int]:
    """Return an independent copy of the exact bounded runtime configuration."""

    return {
        "timeout_seconds": 5,
        "max_attempts": 1,
        "page_size": 10,
        "max_pages": 1,
        "max_total_resources": 10,
        "max_response_bytes": 65_536,
    }


def canary_metadata() -> dict[str, object]:
    """Return the exact marker that distinguishes this fixed synthetic row."""

    return {
        "canary_contract": CANARY_CONTRACT_VERSION,
        "synthetic": True,
    }


def fixture_object(file_name: str) -> dict[str, Any]:
    """Load one packaged exact JSON object by an internal fixed file name."""

    if file_name not in {
        "canary_expected_v1.json",
        "coverage_plan.json",
        "medication_a.json",
        "medication_b.json",
    }:
        raise SyntheticCanaryContractError("synthetic canary fixture is invalid")
    try:
        fixture_value = json.loads(
            (FIXTURE_ROOT / file_name).read_text(encoding="utf-8")
        )
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise SyntheticCanaryContractError(
            "synthetic canary fixture is unavailable"
        ) from None
    if type(fixture_value) is not dict:
        raise SyntheticCanaryContractError("synthetic canary fixture is invalid")
    return fixture_value


def expected_evidence() -> dict[str, Any]:
    """Load the checked-in exact result expected from the v1 fixture graph."""

    evidence_by_field = fixture_object("canary_expected_v1.json")
    expected_fields = {
        "contract_version",
        "dataset_id",
        *EVIDENCE_HASH_FIELDS,
        *EVIDENCE_COUNT_FIELDS,
    }
    has_exact_fields = set(evidence_by_field) == expected_fields
    has_exact_strings = (
        evidence_by_field.get("contract_version") == CANARY_CONTRACT_VERSION
        and re.fullmatch(
            r"ffd_[0-9a-f]{48}",
            str(evidence_by_field.get("dataset_id", "")),
        )
        is not None
        and all(
            type(evidence_by_field.get(field_name)) is str
            and re.fullmatch(r"[0-9a-f]{64}", evidence_by_field[field_name])
            is not None
            for field_name in EVIDENCE_HASH_FIELDS
        )
    )
    has_exact_counts = all(
        type(evidence_by_field.get(field_name)) is int
        and evidence_by_field[field_name] == EXPECTED_EVIDENCE_COUNTS[field_name]
        for field_name in EVIDENCE_COUNT_FIELDS
    )
    if not (has_exact_fields and has_exact_strings and has_exact_counts):
        raise SyntheticCanaryContractError(
            "synthetic canary evidence is invalid"
        )
    return evidence_by_field


__all__ = (
    "CANARY_CONTRACT_VERSION",
    "CANARY_CUTOFF",
    "CANARY_ENABLED_ENV",
    "CANARY_FINAL_TABLE_COUNTS",
    "CANARY_LOCK_RETRY_SECONDS",
    "CANARY_LOCK_WAIT_SECONDS",
    "CANARY_PUBLICATION_TIMEOUT_SECONDS",
    "CANARY_PUBLISHED_TABLE_COUNTS",
    "CANARY_RUN_ID",
    "CANARY_SOURCE_BASE",
    "CANARY_SOURCE_DISPLAY_NAME",
    "CANARY_SOURCE_ID",
    "CANARY_TIMEOUT_SECONDS",
    "SEED_PUBLICATION_ENABLED_ENV",
    "SyntheticCanaryContractError",
    "canary_metadata",
    "canary_runtime_config",
    "expected_evidence",
    "fixture_object",
)
