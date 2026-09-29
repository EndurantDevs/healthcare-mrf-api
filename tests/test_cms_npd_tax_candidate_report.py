# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Restricted candidate reports retain absence and ambiguity without tax promotion."""

import json

import pytest

from process.cms_npd_tax_candidate_report import (
    CmsNpiTaxCandidate,
    CmsTaxCandidateOrganization,
    build_cms_tax_candidate_report,
)
from process.tin_npi_connector import FhirOrganizationEvidenceResult, FhirOrganizationEvidenceState


def _organization(resource_id, *npis):
    return CmsTaxCandidateOrganization(
        resource_id,
        "a" * 64,
        FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.MISSING_EIN,
            npi_candidates=tuple(sorted(npis)),
        ),
    )


def _report(organizations, matches):
    return build_cms_tax_candidate_report(
        dataset_id="dataset-a",
        release_id="release-a",
        tax_snapshot_key=17,
        tax_manifest_sha256="b" * 64,
        extraction_policy_sha256="c" * 64,
        extraction_cutoff="2026-09-24T00:00:00.000000Z",
        organizations=organizations,
        candidates_by_npi={npi: CmsNpiTaxCandidate(keys, len(keys), 0) for npi, keys in matches.items()},
    )


def test_report_preserves_complete_no_match_partial_and_ambiguous_evidence():
    organizations = (
        _organization("organization-a", 1000000004),
        _organization("organization-b", 1234567893, 1000000004),
        _organization("organization-c", 1234567893),
    )
    report = _report(organizations, {1000000004: (), 1234567893: (5, 7)})
    report_by_field = json.loads(report)
    assert report_by_field["source_id"] == "cms-npd"
    assert report_by_field["tax_snapshot_key"] == 17
    assert report_by_field["tax_manifest_sha256"] == "b" * 64
    assert [organization_row["state"] for organization_row in report_by_field["organizations"]] == [
        "no_match",
        "ambiguous",
        "ambiguous",
    ]
    assert report_by_field["organizations"][1]["partial_match"] is True
    assert report_by_field["organizations"][1]["npis"] == [
        {
            "npi": 1000000004,
            "candidate_tin_keys": [],
            "group_count": 0,
            "groups_without_ein_match": 0,
            "state": "no_match",
        },
        {
            "npi": 1234567893,
            "candidate_tin_keys": [5, 7],
            "group_count": 2,
            "groups_without_ein_match": 0,
            "state": "ambiguous",
        },
    ]
    assert all(organization_row["missing_real_ein"] for organization_row in report_by_field["organizations"])
    assert b"pseudo-ein" not in report and b"tax_id" not in report


def test_report_requires_complete_exact_snapshot_lookup():
    organizations = (_organization("organization-a", 1000000004, 1234567893),)
    with pytest.raises(ValueError, match="incomplete"):
        _report(organizations, {1000000004: ()})
    with pytest.raises(ValueError, match="coverage"):
        _report(organizations, {1000000004: (), 1234567893: (), 1003000126: ()})
    with pytest.raises(ValueError, match="lookup is invalid"):
        _report(organizations, {1000000004: (7, 5), 1234567893: ()})
    with pytest.raises(ValueError, match="Organization is invalid"):
        _report((_organization("organization-a", 1000000004),) * 2, {1000000004: ()})


def test_one_tax_key_is_still_only_a_candidate():
    report = json.loads(_report((_organization("organization-a", 1234567893),), {1234567893: (5,)}))
    row = report["organizations"][0]
    assert row["state"] == "single_candidate"
    assert row["missing_real_ein"] is True
    assert "confirmed_tax_identity" not in row


def test_partial_match_is_retained_when_only_one_npi_has_one_candidate():
    report = json.loads(
        _report(
            (_organization("organization-a", 1000000004, 1234567893),),
            {1000000004: (), 1234567893: (5,)},
        )
    )
    row = report["organizations"][0]
    assert row["state"] == "partial_match"
    assert row["partial_match"] is True


def test_group_level_partial_match_is_retained_with_one_candidate():
    report = build_cms_tax_candidate_report(
        dataset_id="dataset-a",
        release_id="release-a",
        tax_snapshot_key=17,
        tax_manifest_sha256="b" * 64,
        extraction_policy_sha256="c" * 64,
        extraction_cutoff="2026-09-24T00:00:00.000000Z",
        organizations=(_organization("organization-a", 1000000004),),
        candidates_by_npi={1000000004: CmsNpiTaxCandidate((5,), 2, 1)},
    )
    row = json.loads(report)["organizations"][0]
    assert row["state"] == "partial_match"
    assert row["partial_match"] is True
    assert row["npis"] == [
        {
            "npi": 1000000004,
            "candidate_tin_keys": [5],
            "group_count": 2,
            "groups_without_ein_match": 1,
            "state": "partial_match",
        }
    ]
