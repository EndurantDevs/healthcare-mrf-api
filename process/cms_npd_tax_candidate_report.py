# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Restricted CMS NPI-only tax candidates; never a tax-identity binding."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from process.tin_npi_connector_evidence import FhirOrganizationEvidenceResult
from process.tin_npi_connector_support import FhirOrganizationEvidenceState, TinNpiConnectorError
from process.tin_npi_connector_temporal import _normalize_npi, canonical_evidence_as_of

_HASH = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class CmsTaxCandidateOrganization:
    """One verified CMS Organization and its extraction result."""

    resource_id: str
    payload_sha256: str
    extraction: FhirOrganizationEvidenceResult


@dataclass(frozen=True)
class CmsNpiTaxCandidate:
    """Complete snapshot-local group outcome for one NPI."""

    tin_keys: tuple[int, ...]
    group_count: int
    groups_without_ein_match: int
    degree_overflow: bool = False


def _is_valid_text(candidate_text: object, *, limit: int) -> bool:
    return (
        type(candidate_text) is str
        and candidate_text == candidate_text.strip()
        and 0 < len(candidate_text) <= limit
        and candidate_text.isprintable()
    )


def _is_valid_npi(candidate_npi: object) -> bool:
    if type(candidate_npi) is not int:
        return False
    try:
        return _normalize_npi(str(candidate_npi)) == candidate_npi
    except TinNpiConnectorError:
        return False


def _candidate_state(candidates: tuple[CmsNpiTaxCandidate, ...]) -> str:
    if any(candidate.degree_overflow for candidate in candidates):
        return "degree_overflow"
    matched_keys = tuple(candidate.tin_keys for candidate in candidates if candidate.tin_keys)
    if not matched_keys:
        return "no_match"
    if len(set().union(*matched_keys)) != 1:
        return "ambiguous"
    if any(not candidate.tin_keys or candidate.groups_without_ein_match for candidate in candidates):
        return "partial_match"
    return "single_candidate"


def _npi_report_row(npi: int, candidate: CmsNpiTaxCandidate) -> dict[str, object]:
    if type(candidate) is not CmsNpiTaxCandidate:
        raise ValueError("CMS tax candidate lookup is invalid")
    keys = candidate.tin_keys
    if (
        type(keys) is not tuple
        or keys != tuple(sorted(set(keys)))
        or any(type(key) is not int or key < 0 for key in keys)
        or type(candidate.group_count) is not int
        or candidate.group_count < 0
        or type(candidate.groups_without_ein_match) is not int
        or not 0 <= candidate.groups_without_ein_match <= candidate.group_count
        or type(candidate.degree_overflow) is not bool
    ):
        raise ValueError("CMS tax candidate lookup is invalid")
    if candidate.degree_overflow:
        if keys or candidate.group_count == 0 or candidate.groups_without_ein_match:
            raise ValueError("CMS tax candidate overflow evidence is invalid")
        return {
            "npi": npi,
            "candidate_tin_keys": (),
            "group_count": None,
            "group_count_lower_bound": candidate.group_count,
            "groups_without_ein_match": None,
            "state": "degree_overflow",
        }
    if bool(keys) != (candidate.group_count > candidate.groups_without_ein_match):
        raise ValueError("CMS tax candidate lookup is invalid")
    return {
        "npi": npi,
        "candidate_tin_keys": keys,
        "group_count": candidate.group_count,
        "groups_without_ein_match": candidate.groups_without_ein_match,
        "state": (
            "ambiguous"
            if len(keys) > 1
            else "partial_match"
            if keys and candidate.groups_without_ein_match
            else "single_candidate"
            if keys
            else "no_match"
        ),
    }


def _organization_report_row(
    organization: CmsTaxCandidateOrganization,
    candidates_by_npi: Mapping[int, CmsNpiTaxCandidate],
    used_npis: set[int],
) -> dict[str, object]:
    if type(organization) is not CmsTaxCandidateOrganization:
        raise ValueError("CMS tax candidate Organization is invalid")
    extraction = organization.extraction
    if (
        not _is_valid_text(organization.resource_id, limit=256)
        or type(organization.payload_sha256) is not str
        or _HASH.fullmatch(organization.payload_sha256) is None
        or type(extraction) is not FhirOrganizationEvidenceResult
        or extraction.state is not FhirOrganizationEvidenceState.MISSING_EIN
        or not extraction.npi_candidates
        or extraction.evidence
    ):
        raise ValueError("CMS tax candidate Organization is invalid")
    npi_rows: list[dict[str, object]] = []
    candidates: list[CmsNpiTaxCandidate] = []
    for npi in extraction.npi_candidates:
        if npi not in candidates_by_npi:
            raise ValueError("CMS tax candidate lookup is incomplete")
        candidate = candidates_by_npi[npi]
        npi_rows.append(_npi_report_row(npi, candidate))
        used_npis.add(npi)
        candidates.append(candidate)
    return {
        "resource_id": organization.resource_id,
        "payload_sha256": organization.payload_sha256,
        "state": _candidate_state(tuple(candidates)),
        "partial_match": not any(candidate.degree_overflow for candidate in candidates)
        and any(candidate.tin_keys for candidate in candidates)
        and any(not candidate.tin_keys or candidate.groups_without_ein_match for candidate in candidates),
        "missing_real_ein": True,
        "npis": npi_rows,
    }


def _validate_report_pin(
    dataset_id: str,
    release_id: str,
    tax_snapshot_key: int,
    tax_manifest_sha256: str,
    extraction_policy_sha256: str,
    extraction_cutoff: str,
    candidates_by_npi: Mapping[int, CmsNpiTaxCandidate],
) -> None:
    if (
        not _is_valid_text(dataset_id, limit=128)
        or not _is_valid_text(release_id, limit=256)
        or type(tax_snapshot_key) is not int
        or tax_snapshot_key <= 0
        or type(tax_manifest_sha256) is not str
        or _HASH.fullmatch(tax_manifest_sha256) is None
        or type(extraction_policy_sha256) is not str
        or _HASH.fullmatch(extraction_policy_sha256) is None
        or not isinstance(candidates_by_npi, Mapping)
    ):
        raise ValueError("CMS tax candidate report pin is invalid")
    try:
        if canonical_evidence_as_of(extraction_cutoff) != extraction_cutoff:
            raise ValueError
    except ValueError, TinNpiConnectorError:
        raise ValueError("CMS tax candidate extraction cutoff is invalid") from None


def build_cms_tax_candidate_report(
    *,
    dataset_id: str,
    release_id: str,
    tax_snapshot_key: int,
    tax_manifest_sha256: str,
    extraction_policy_sha256: str,
    extraction_cutoff: str,
    organizations: Iterable[CmsTaxCandidateOrganization],
    candidates_by_npi: Mapping[int, CmsNpiTaxCandidate],
) -> bytes:
    """Require a complete pinned lookup and serialize only candidate evidence.

    ``candidates_by_npi`` must be the complete result of a restricted lookup
    against ``tax_snapshot_key``; empty keys mean an executed no-match.
    Snapshot-local keys are candidates, never confirmed EIN/TIN assignments.
    """

    _validate_report_pin(
        dataset_id,
        release_id,
        tax_snapshot_key,
        tax_manifest_sha256,
        extraction_policy_sha256,
        extraction_cutoff,
        candidates_by_npi,
    )
    organization_rows: list[dict[str, object]] = []
    used_npis: set[int] = set()
    seen_resource_ids: set[str] = set()
    for organization in organizations:
        if type(organization) is not CmsTaxCandidateOrganization or organization.resource_id in seen_resource_ids:
            raise ValueError("CMS tax candidate Organization is invalid")
        seen_resource_ids.add(organization.resource_id)
        organization_rows.append(_organization_report_row(organization, candidates_by_npi, used_npis))
    if set(candidates_by_npi) != used_npis or any(not _is_valid_npi(npi) for npi in candidates_by_npi):
        raise ValueError("CMS tax candidate lookup coverage is invalid")
    organization_rows.sort(key=lambda organization_row: str(organization_row["resource_id"]))
    return (
        json.dumps(
            {
                "contract": "cms-npd-tax-candidates-v1",
                "source_id": "cms-npd",
                "dataset_id": dataset_id,
                "release_id": release_id,
                "tax_snapshot_key": tax_snapshot_key,
                "tax_manifest_sha256": tax_manifest_sha256,
                "extraction_policy_sha256": extraction_policy_sha256,
                "extraction_cutoff": extraction_cutoff,
                "organizations": organization_rows,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
