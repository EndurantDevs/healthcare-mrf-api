# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from pathlib import Path

import pytest

from process.ext import address_canon


ptg2_address_canon = pytest.importorskip("ptg2_address_canon")

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


def _golden_cases():
    payload = json.loads((FIXTURE_DIR / "address_canonical_golden.json").read_text())
    return list(payload["explicit_cases"])


def test_pyo3_canon_version_matches_python():
    assert ptg2_address_canon.canon_version() == address_canon.current_canon_version()


def test_pyo3_address_canonical_golden_corpus_matches_frozen_expected_values():
    cases = _golden_cases()
    assert len(cases) >= 270
    rows = [
        tuple(case.get(key) for key in ("first_line", "second_line", "city", "state", "zip", "country"))
        for case in cases
    ]

    results = ptg2_address_canon.canonicalize_batch(rows)

    assert len(results) == len(cases)
    for case, result in zip(cases, results):
        assert result["identity_key"] == case["expected_identity_key"], case["id"]
        assert result["address_key"] == case["expected_address_key"], case["id"]
        assert result["premise_identity_key"] == case["expected_premise_identity_key"], case["id"]
        assert result["premise_key"] == case["expected_premise_key"], case["id"]


def test_pyo3_contact_canonical_batch_normalizes_us_and_international_values():
    if not hasattr(ptg2_address_canon, "canonicalize_contact_batch"):
        pytest.skip("installed ptg2_address_canon module does not include contact canonicalizer")
    results = ptg2_address_canon.canonicalize_contact_batch(
        [
            ("+1 (312) 555-0100 ext. 45", "312.555.0199 # 22", "US"),
            ("+44 20 7946 0958", None, "GB"),
            ("555-1212", None, "US"),
        ]
    )

    assert results[0]["phone_number"] == "3125550100"
    assert results[0]["phone_extension"] == "45"
    assert results[0]["phone_valid_for_fallback"] is True
    assert results[0]["fax_number"] == "3125550199"
    assert results[0]["fax_number_digits"] == "3125550199"
    assert results[0]["fax_extension"] == "22"
    assert results[1]["phone_number"] == "442079460958"
    assert results[1]["phone_is_international"] is True
    assert results[1]["phone_valid_for_fallback"] is False
    assert results[2]["phone_number"] is None
    assert results[2]["phone_valid_for_fallback"] is False
