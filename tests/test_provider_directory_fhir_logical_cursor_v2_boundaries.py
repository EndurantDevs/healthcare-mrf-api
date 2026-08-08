# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundary coverage for logical Smile census proof helpers."""

from __future__ import annotations

from copy import deepcopy

import pytest

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)
from process.provider_directory_fhir_census_execution import (
    current_version_census_checkpoint_proof,
    current_version_census_completed_proof,
    current_version_census_initial_proof,
    resolved_current_version_census_next_url,
    validated_current_version_census_completed_proof,
)
from process.provider_directory_fhir_census_page_geometry import (
    current_version_census_terminal_page_geometry,
    validate_census_page_entries,
    validate_current_version_census_checkpoint_geometry,
)


BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"
RESOURCE_TYPE = "Organization"


def _contract() -> CurrentVersionCensusContract:
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=(RESOURCE_TYPE,),
        expected_nonempty_resources=(RESOURCE_TYPE,),
        start_urls=((RESOURCE_TYPE, f"{BASE}/{RESOURCE_TYPE}?active=true"),),
        continuation_strategy=CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    )


def _cursor(offset: int, *, count: object = 2) -> str:
    return (
        f"{BASE}?_getpages=opaque&_getpagesoffset={offset}"
        f"&_count={count}&_pretty=true"
    )


def _initial_proof(*, pre_count: int = 5, page_count: int = 2):
    return current_version_census_initial_proof(
        _contract(),
        RESOURCE_TYPE,
        pre_count,
        expected_page_count=page_count,
    )


@pytest.mark.parametrize(
    ("entry_count", "page_count"),
    ((True, 2), (-1, 2), (3, 2), (0, True), (0, 0)),
)
def test_page_entry_validation_rejects_invalid_bounds(
    entry_count: object,
    page_count: object,
):
    with pytest.raises(ValueError, match="page_state_invalid"):
        validate_census_page_entries(entry_count, page_count)


@pytest.mark.parametrize("geometry", ({}, {"unexpected": 1}, "invalid"))
def test_checkpoint_geometry_requires_exact_shape(geometry: object):
    with pytest.raises(ValueError, match="page_geometry_invalid"):
        validate_current_version_census_checkpoint_geometry(
            {"page_geometry": geometry},
            pages_processed=0,
            rows_processed=0,
            expected_page_count=2,
        )


def test_checkpoint_advance_rejects_missing_geometry():
    with pytest.raises(ValueError, match="page_geometry_invalid"):
        current_version_census_checkpoint_proof(
            {"pre_count": 5, "page_geometry": "invalid"},
            pages_processed=1,
            rows_processed=1,
            page_entry_count=1,
            expected_page_count=2,
        )


@pytest.mark.parametrize(
    ("proof", "pages_processed", "rows_processed", "page_entry_count"),
    (
        (_initial_proof(), 0, 0, 0),
        (_initial_proof(), 1, 0, 1),
        (_initial_proof(pre_count=2), 1, 2, 2),
    ),
)
def test_checkpoint_advance_rejects_impossible_progress(
    proof: dict[str, object],
    pages_processed: int,
    rows_processed: int,
    page_entry_count: int,
):
    with pytest.raises(ValueError, match="page_geometry_invalid"):
        current_version_census_checkpoint_proof(
            proof,
            pages_processed=pages_processed,
            rows_processed=rows_processed,
            page_entry_count=page_entry_count,
            expected_page_count=2,
        )


def test_terminal_geometry_wraps_invalid_checkpoint_shape():
    with pytest.raises(ValueError, match="terminal_geometry_invalid"):
        current_version_census_terminal_page_geometry(
            {"pre_count": 5, "page_geometry": "invalid"},
            pages_processed=1,
            processed_rows=1,
            expected_page_count=2,
            terminal_page_entry_count=1,
        )


@pytest.mark.parametrize("pages_processed", (True, 0, -1))
def test_terminal_geometry_rejects_invalid_page_number(pages_processed: object):
    with pytest.raises(ValueError, match="terminal_geometry_invalid"):
        current_version_census_terminal_page_geometry(
            _initial_proof(),
            pages_processed=pages_processed,
            processed_rows=0,
            expected_page_count=2,
            terminal_page_entry_count=0,
        )


def test_terminal_geometry_rejects_window_beyond_precount():
    checkpoint_proof = current_version_census_checkpoint_proof(
        _initial_proof(),
        pages_processed=1,
        rows_processed=2,
        page_entry_count=2,
        expected_page_count=2,
    )
    checkpoint_proof["pre_count"] = 1
    with pytest.raises(ValueError, match="terminal_geometry_invalid"):
        current_version_census_terminal_page_geometry(
            checkpoint_proof,
            pages_processed=2,
            processed_rows=3,
            expected_page_count=2,
            terminal_page_entry_count=1,
        )


def test_cursor_wraps_malformed_current_cursor():
    with pytest.raises(ValueError, match="untrusted_current_version"):
        resolved_current_version_census_next_url(
            _contract(),
            RESOURCE_TYPE,
            _cursor(2, count="invalid"),
            _cursor(4),
            page_entry_count=1,
            expected_page_count=2,
            pre_total=5,
        )


@pytest.mark.parametrize("pre_total", (True, -1, "5"))
def test_cursor_rejects_invalid_precount(pre_total: object):
    with pytest.raises(ValueError, match="pre_count_invalid"):
        resolved_current_version_census_next_url(
            _contract(),
            RESOURCE_TYPE,
            _contract().start_url(RESOURCE_TYPE, 2),
            _cursor(2),
            page_entry_count=2,
            expected_page_count=2,
            pre_total=pre_total,
        )


def _completed_proof() -> dict[str, object]:
    return current_version_census_completed_proof(
        _initial_proof(pre_count=2),
        post_count=2,
        processed_rows=2,
        unique_candidate_rows=2,
        pages_processed=1,
        expected_page_count=2,
        terminal_page_entry_count=2,
    )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda proof: proof.pop("terminal_page_geometry"),
        lambda proof: proof["terminal_page_geometry"].update(
            {"terminal_page_entries": "invalid"}
        ),
        lambda proof: proof.update({"unreturned_count": True}),
    ),
)
def test_completed_proof_rejects_malformed_terminal_evidence(mutation):
    proof = deepcopy(_completed_proof())
    mutation(proof)
    with pytest.raises(ValueError, match="completed_proof_invalid"):
        validated_current_version_census_completed_proof(
            proof,
            _contract(),
            RESOURCE_TYPE,
            rows_processed=2,
            pages_processed=1,
        )


def test_completed_proof_removes_stale_terminal_attempt_fields():
    initial_proof = _initial_proof(pre_count=2)
    initial_proof["failure"] = "stale"
    initial_proof["last_terminal_page_geometry"] = {"stale": True}

    completed_proof = current_version_census_completed_proof(
        initial_proof,
        post_count=2,
        processed_rows=2,
        unique_candidate_rows=2,
        pages_processed=1,
        expected_page_count=2,
        terminal_page_entry_count=2,
    )

    assert completed_proof["verified"] is True
    assert "failure" not in completed_proof
    assert "last_terminal_page_geometry" not in completed_proof
