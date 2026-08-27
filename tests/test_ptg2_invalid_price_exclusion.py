# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
from __future__ import annotations

import copy

import pytest

from process.ptg_parts.ptg2_invalid_price_exclusion import (
    invalid_price_exclusion_evidence,
    invalid_price_exclusion_policy,
    invalid_price_exclusion_source,
    invalid_price_exclusion_source_evidence,
    invalid_price_exclusion_source_expectation,
    invalid_price_value_sha256,
    validate_candidate_invalid_price_exclusion_evidence,
    validate_invalid_price_exclusion_evidence,
    validate_invalid_price_exclusion_policy,
    validate_invalid_price_exclusion_source_evidence,
    validated_candidate_invalid_price_exclusion_policy,
)


def _entry(object_ordinal: int, value: str) -> dict[str, object]:
    return {
        "object_ordinal": object_ordinal,
        "rate_ordinal": 2,
        "price_ordinal": 1,
        "invalid_value_sha256": invalid_price_value_sha256(value),
    }


def _source(raw_source_sha256: str, object_ordinal: int) -> dict[str, object]:
    return invalid_price_exclusion_source(
        raw_source_sha256=raw_source_sha256,
        entries=[_entry(object_ordinal, "2027-02-30")],
        emptied_rate_count=0,
    )


def test_policy_is_exact_canonical_private_and_fail_closed() -> None:
    """Require one private canonical policy and reject every mutation."""

    first = _source("11" * 32, 4)
    second = _source("22" * 32, 8)
    policy = invalid_price_exclusion_policy([second, first])

    assert validate_invalid_price_exclusion_policy(policy) == policy
    assert [source_by_field["raw_source_sha256"] for source_by_field in policy["sources"]] == [
        "11" * 32,
        "22" * 32,
    ]
    assert policy["excluded_price_count"] == 2
    assert policy["emptied_rate_count"] == 0
    expectation = invalid_price_exclusion_source_expectation(policy, "22" * 32)
    assert expectation == {
        "contract": "ptg2_invalid_price_exclusion_source_v1",
        "reason": "invalid_iso_calendar_date",
        "excluded_price_count": 1,
        **second,
    }
    source_evidence = invalid_price_exclusion_source_evidence(expectation)
    assert validate_invalid_price_exclusion_source_evidence(source_evidence) == (source_evidence)
    evidence = invalid_price_exclusion_evidence(policy)
    assert validate_invalid_price_exclusion_evidence(evidence) == evidence
    assert set(evidence) == {
        "contract",
        "reason",
        "excluded_price_count",
        "emptied_rate_count",
        "source_count",
        "sha256",
    }
    assert "sources" not in evidence

    for field, replacement in (
        ("sha256", "00" * 32),
        ("excluded_price_count", 3),
        ("source_count", 1),
    ):
        changed = copy.deepcopy(policy)
        changed[field] = replacement
        with pytest.raises(ValueError):
            validate_invalid_price_exclusion_policy(changed)

    changed_coordinate = copy.deepcopy(policy)
    changed_coordinate["sources"][0]["entries"][0]["price_ordinal"] = 2
    with pytest.raises(ValueError):
        validate_invalid_price_exclusion_policy(changed_coordinate)
    changed_value = copy.deepcopy(policy)
    changed_value["sources"][0]["entries"][0]["invalid_value_sha256"] = invalid_price_value_sha256("2028-02-30")
    with pytest.raises(ValueError):
        validate_invalid_price_exclusion_policy(changed_value)

    assert invalid_price_exclusion_source_expectation(policy, "33" * 32) is None


def test_candidate_evidence_is_exactly_bound_to_policy_and_sources() -> None:
    policy = invalid_price_exclusion_policy([_source("11" * 32, 4)])
    evidence = invalid_price_exclusion_evidence(policy)

    assert (
        validate_candidate_invalid_price_exclusion_evidence(
            policy,
            evidence,
            evidence,
            ("11" * 32, "22" * 32),
        )
        == evidence
    )
    assert (
        validate_candidate_invalid_price_exclusion_evidence(
            None,
            None,
            None,
            ("11" * 32,),
        )
        is None
    )
    with pytest.raises(ValueError, match="no exact policy"):
        validate_candidate_invalid_price_exclusion_evidence(
            None,
            evidence,
            evidence,
            ("11" * 32,),
        )
    with pytest.raises(ValueError, match="unbound source"):
        validate_candidate_invalid_price_exclusion_evidence(
            policy,
            evidence,
            evidence,
            ("22" * 32,),
        )
    changed_evidence_by_name = {**evidence, "sha256": "00" * 32}
    with pytest.raises(ValueError, match="changed after layout sealing"):
        validate_candidate_invalid_price_exclusion_evidence(
            policy,
            evidence,
            changed_evidence_by_name,
            ("11" * 32,),
        )


def test_candidate_policy_uses_exact_singleton_or_matching_frozen_binding() -> None:
    policy = invalid_price_exclusion_policy([_source("11" * 32, 4)])

    assert validated_candidate_invalid_price_exclusion_policy(
        policy,
        None,
        ("11" * 32,),
    ) == policy
    assert validated_candidate_invalid_price_exclusion_policy(
        policy,
        {"invalid_price_exclusion_policy": policy},
        ("11" * 32, "22" * 32),
    ) == policy
    with pytest.raises(ValueError, match="singleton.*source changed"):
        validated_candidate_invalid_price_exclusion_policy(
            policy,
            None,
            ("22" * 32,),
        )
    with pytest.raises(ValueError, match="singleton.*source changed"):
        validated_candidate_invalid_price_exclusion_policy(
            policy,
            None,
            ("11" * 32, "11" * 32),
        )
    with pytest.raises(ValueError, match="binding changed"):
        validated_candidate_invalid_price_exclusion_policy(
            None,
            {"invalid_price_exclusion_policy": policy},
            ("11" * 32,),
        )


def test_policy_rejects_source_larger_than_scanner_environment_transport() -> None:
    source = invalid_price_exclusion_source(
        raw_source_sha256="11" * 32,
        entries=[
            {
                "object_ordinal": ordinal,
                "rate_ordinal": 0,
                "price_ordinal": 0,
                "invalid_value_sha256": "22" * 32,
            }
            for ordinal in range(1_024)
        ],
        emptied_rate_count=0,
    )

    with pytest.raises(ValueError, match="scanner transport limit"):
        invalid_price_exclusion_policy([source])
