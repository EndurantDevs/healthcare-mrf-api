# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic business-grain contracts, independent of private source cells or databases."""

from copy import deepcopy
from uuid import UUID

import pytest

from process.registry_required_target_business_report import business_target_report
from process.registry_required_target_import import _parser
from process.registry_required_target_store import (
    BUSINESS_PARSER_VERSION,
    PARSER_VERSION,
    RegistryRequiredTargetEdition,
)


def _report():
    contexts = [
        {"plan_carrier": "Carrier A", "network_name": "Network", "plan_type": "PPO"},
        {"plan_carrier": "Carrier B", "network_name": "Network", "plan_type": "Dental"},
        {"plan_carrier": "Carrier A", "network_name": "", "plan_type": "Vision"},
    ]
    return {
        "targets": [
            {
                "target_key": str(i),
                "network_id": 42 if i < 2 else None,
                "mapping_status": "resolved" if i < 2 else "unresolved",
                "company_link_verified": i < 2,
            }
            for i in range(3)
        ],
        "totals": {"ledger_targets": 3, "company_link_verified": 2},
        "provenance": {
            "target_evidence": [
                {"target_key": str(i), "business_context": context, "source_row_count": 2 if i < 2 else 1}
                for i, context in enumerate(contexts)
            ]
        },
        "assessment": {"pricing": "not_assessed"},
    }


def test_business_report_preserves_context_denominators_and_generic_relationship_truth():
    original = _report()
    result = business_target_report(original, {"source_rows": 5})
    assert result["totals"] == {
        "ledger_targets": 3,
        "business_targets": 3,
        "source_rows": 5,
        "blank_network_targets": 1,
        "any_approved_company_link": 2,
    }
    assert "company_link_verified" not in result["totals"]
    assert [row["network_id"] for row in result["targets"]] == [42, 42, None]
    assert [row["source_row_count"] for row in result["targets"]] == [2, 2, 1]
    assert all(row["carrier_company_status"] == "not_assessed" for row in result["targets"])
    assert result["targets"][0]["any_approved_company_link"] is True
    assert "company_link_verified" not in result["targets"][0]
    assert result["assessment"]["company_link"] == "any_active_approved_relationship_not_target_carrier_verification"
    assert result["assessment"]["pricing"] == "not_assessed"
    assert original == _report()


@pytest.mark.parametrize(
    "corruption", ["missing", "duplicate", "extra-context", "bool-count", "lost-rows", "blank-resolved"]
)
def test_business_report_refuses_incomplete_or_relabelled_context(corruption):
    report = deepcopy(_report())
    rows = report["provenance"]["target_evidence"]
    match corruption:
        case "missing":
            rows.pop()
        case "duplicate":
            rows.append(deepcopy(rows[0]))
        case "extra-context":
            rows[0]["business_context"]["employer"] = "Context only"
        case "bool-count":
            rows[0]["source_row_count"] = True
        case "lost-rows":
            rows[0]["source_row_count"] = 1
        case "blank-resolved":
            report["targets"][2].update(network_id=42, mapping_status="resolved")
    with pytest.raises(ValueError, match="business_report_invalid"):
        business_target_report(report, {"source_rows": 5})


def test_business_import_is_explicit_and_legacy_edition_default_remains_unchanged():
    identity = UUID("34567890-3456-7890-8345-34567890abcd")
    arguments = [
        "--input-file",
        "synthetic.csv",
        "--snapshot-id",
        str(identity),
        "--source-url",
        "https://example.test/input.csv",
        "--input-sha256",
        "a" * 64,
    ]
    assert _parser().parse_args(arguments).target_contract == "source-reference-v2"
    assert (
        _parser().parse_args([*arguments, "--target-contract", "business-context-v1"]).target_contract
        == "business-context-v1"
    )
    assert (
        RegistryRequiredTargetEdition(identity, "https://example.test/input.csv", "a" * 64).parser_version
        == PARSER_VERSION
    )
    assert (
        RegistryRequiredTargetEdition(
            identity, "https://example.test/input.csv", "a" * 64, BUSINESS_PARSER_VERSION
        ).parser_version
        == BUSINESS_PARSER_VERSION
    )
    with pytest.raises(ValueError, match="parser_invalid"):
        RegistryRequiredTargetEdition(identity, "https://example.test/input.csv", "a" * 64, "unknown")
