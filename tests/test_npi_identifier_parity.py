# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cross-language acceptance parity for frozen synthetic NPI vectors."""

from __future__ import annotations

import json
from pathlib import Path

from process.provider_directory_profile import is_valid_npi

_CONTRACT_ID = "healthporta.npi-identifier-classification.v1"
_VECTOR_PATH = (
    Path(__file__).parents[1]
    / "support"
    / "ptg2_scanner"
    / "tests"
    / "fixtures"
    / "npi_identifier_vectors_v1.json"
)


def test_python_validity_acceptance_matches_frozen_vectors():
    vector_contract = json.loads(_VECTOR_PATH.read_text(encoding="utf-8"))

    assert set(vector_contract) == {"contract_id", "version", "cases"}
    assert vector_contract["contract_id"] == _CONTRACT_ID
    assert vector_contract["version"] == 1
    classifications = set()
    case_ids = set()
    for vector_case in vector_contract["cases"]:
        assert set(vector_case) == {"id", "value", "classification"}
        assert isinstance(vector_case["value"], str)
        assert vector_case["id"] not in case_ids
        case_ids.add(vector_case["id"])
        classifications.add(vector_case["classification"])
        assert is_valid_npi(vector_case["value"]) is (
            vector_case["classification"] == "valid"
        )
    assert classifications == {
        "valid",
        "checksum_invalid",
        "structural_invalid",
        "invalid",
    }


def test_python_profile_input_compatibility_remains_adapter_scoped():
    canonical_value = "1000000491"

    assert is_valid_npi(f" {canonical_value} ")
    assert is_valid_npi(int(canonical_value))
    assert not is_valid_npi(float(canonical_value))
