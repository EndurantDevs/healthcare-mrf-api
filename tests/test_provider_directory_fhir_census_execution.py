# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execution proof and opaque-cursor defenses for current-version census."""

from __future__ import annotations

from dataclasses import replace

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
    current_version_census_persisted_pre_count,
    current_version_census_proof_identity,
    resolved_current_version_census_next_url,
    validated_current_version_census_total,
)


BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"


def _contract() -> CurrentVersionCensusContract:
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=("Organization",),
        expected_nonempty_resources=("Organization",),
        start_urls=(("Organization", f"{BASE}/Organization?active=true"),),
        continuation_strategy=CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    )


def _cursor(
    *,
    token: str = "opaque-token",
    offset: int = 250,
    count: int = 250,
    path: str = "/fhir",
    suffix: str = "",
) -> str:
    return (
        f"https://directory.example.test{path}?"
        f"_getpages={token}&_getpagesoffset={offset}&_count={count}{suffix}"
    )


@pytest.mark.parametrize(
    "payload",
    (
        None,
        [],
        {"resourceType": "OperationOutcome", "type": "searchset", "total": 1},
        {"resourceType": "Bundle", "type": "collection", "total": 1},
        {"resourceType": "Bundle", "type": "searchset"},
        {"resourceType": "Bundle", "type": "searchset", "total": True},
        {"resourceType": "Bundle", "type": "searchset", "total": -1},
        {"resourceType": "Bundle", "type": "searchset", "total": "1"},
    ),
)
def test_exact_count_rejects_malformed_or_nonexact_payload(payload):
    with pytest.raises(ValueError):
        validated_current_version_census_total(payload)


def test_exact_count_accepts_zero_without_bool_coercion():
    assert validated_current_version_census_total(
        {"resourceType": "Bundle", "type": "searchset", "total": 0}
    ) == 0


def test_proof_identity_and_persisted_pre_count_bind_full_contract():
    contract = _contract()
    initial_proof = current_version_census_initial_proof(
        contract,
        "Organization",
        500,
        expected_page_count=250,
    )

    assert current_version_census_persisted_pre_count(
        initial_proof,
        contract,
        "Organization",
    ) == 500
    assert initial_proof["contract_identity"] == (
        current_version_census_proof_identity(contract)
    )
    assert current_version_census_persisted_pre_count(
        {},
        contract,
        "Organization",
    ) is None

    for drifted_contract in (
        replace(contract, cutoff="2026-08-01T13:00:00.000000Z"),
        replace(contract, source_id="other-source"),
        replace(contract, expected_nonempty_resources=()),
        replace(contract, continuation_strategy="smile-opaque-getpages-v1"),
    ):
        with pytest.raises(ValueError, match="checkpoint_identity_mismatch"):
            current_version_census_persisted_pre_count(
                initial_proof,
                drifted_contract,
                "Organization",
            )


@pytest.mark.parametrize("pre_count", (True, -1, 1.5, "1"))
def test_initial_proof_rejects_noninteger_pre_count(pre_count):
    with pytest.raises(ValueError, match="pre_count_invalid"):
        current_version_census_initial_proof(
            _contract(),
            "Organization",
            pre_count,
            expected_page_count=250,
        )


def test_completed_proof_requires_four_way_equality():
    initial_proof = current_version_census_initial_proof(
        _contract(),
        "Organization",
        500,
        expected_page_count=1000,
    )
    verified_proof = current_version_census_completed_proof(
        initial_proof,
        post_count=500,
        processed_rows=500,
        unique_candidate_rows=500,
        pages_processed=1,
        expected_page_count=1000,
        terminal_page_entry_count=500,
    )
    assert verified_proof["verified"] is True
    assert "failure" not in verified_proof

    cases = (
        ({"post_count": 499, "processed_rows": 500, "unique_candidate_rows": 500}, "census_drift"),
        ({"post_count": 500, "processed_rows": 499, "unique_candidate_rows": 499}, "cursor_loss"),
        ({"post_count": 500, "processed_rows": 501, "unique_candidate_rows": 500}, "duplicate_resource_ids"),
        ({"post_count": 500, "processed_rows": 501, "unique_candidate_rows": 501}, "processed_count_mismatch"),
    )
    for counts, expected_failure in cases:
        failed_proof = current_version_census_completed_proof(
            initial_proof,
            pages_processed=1,
            expected_page_count=1000,
            terminal_page_entry_count=counts["processed_rows"],
            **counts,
        )
        assert failed_proof["verified"] is False
        assert failed_proof["failure"] == expected_failure


@pytest.mark.parametrize(
    "counts",
    (
        {"post_count": True, "processed_rows": 1, "unique_candidate_rows": 1},
        {"post_count": 1, "processed_rows": -1, "unique_candidate_rows": 1},
        {"post_count": 1, "processed_rows": 1, "unique_candidate_rows": "1"},
    ),
)
def test_completed_proof_rejects_invalid_count_types(counts):
    initial_proof = current_version_census_initial_proof(
        _contract(),
        "Organization",
        1,
        expected_page_count=10,
    )
    with pytest.raises(ValueError, match="proof_count_invalid"):
        current_version_census_completed_proof(
            initial_proof,
            pages_processed=1,
            expected_page_count=10,
            terminal_page_entry_count=1,
            **counts,
        )


def test_first_and_later_smile_cursor_offsets_are_exact():
    contract = _contract()
    first_page = f"{BASE}/Organization?active=true&_count=250"
    first_continuation = resolved_current_version_census_next_url(
        contract,
        "Organization",
        first_page,
        _cursor(offset=250),
        page_entry_count=250,
        expected_page_count=250,
        pre_total=1000,
    )
    assert first_continuation.url == _cursor(offset=250)
    assert first_continuation.token == "opaque-token"
    assert first_continuation.offset == 250

    second_continuation = resolved_current_version_census_next_url(
        contract,
        "Organization",
        first_continuation.url,
        _cursor(offset=500),
        page_entry_count=250,
        expected_page_count=250,
        pre_total=1000,
    )
    assert second_continuation.offset == 500


def test_canonical_cursor_identity_detects_query_reordering():
    contract = _contract()
    current_url = f"{BASE}/Organization?_count=250"
    first = resolved_current_version_census_next_url(
        contract,
        "Organization",
        current_url,
        _cursor(offset=250, suffix="&_pretty=true"),
        page_entry_count=250,
        expected_page_count=250,
        pre_total=1000,
    )
    reordered = resolved_current_version_census_next_url(
        contract,
        "Organization",
        current_url,
        (
            "https://directory.example.test/fhir?"
            "_pretty=true&_count=250&_getpagesoffset=250&_getpages=opaque-token"
        ),
        page_entry_count=250,
        expected_page_count=250,
        pre_total=1000,
    )
    assert reordered.url != first.url
    assert reordered.identity == first.identity


@pytest.mark.parametrize(
    "next_link",
    (
        "http://directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1",
        "https://other.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1",
        "https://directory.example.test:8443/fhir?_getpages=x&_getpagesoffset=1&_count=1",
        "https://user@directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1",
        "https://directory.example.test/fhir/../private?_getpages=x&_getpagesoffset=1&_count=1",
        "https://directory.example.test/fhir%2Fprivate?_getpages=x&_getpagesoffset=1&_count=1",
        "https://directory.example.test/fhir%252Fprivate?_getpages=x&_getpagesoffset=1&_count=1",
        "https://directory.example.test/fhir//private?_getpages=x&_getpagesoffset=1&_count=1",
        "https://directory.example.test/private?_getpages=x&_getpagesoffset=1&_count=1",
        "https://directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1#fragment",
        "https://directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1&unknown=y",
        "https://directory.example.test/fhir?_getpages=&_getpagesoffset=1&_count=1",
        "https://directory.example.test/fhir?_getpages=x&_getpages=x&_getpagesoffset=1&_count=1",
        "https://directory.example.test/fhir?_getpages=x&_getpagesoffset=-1&_count=1",
        "https://directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=2",
        "https://directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1&_pretty=maybe",
        "https://directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1&_bundletype=history",
        "//directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1",
        " https://directory.example.test/fhir?_getpages=x&_getpagesoffset=1&_count=1",
    ),
)
def test_smile_cursor_rejects_untrusted_shapes(next_link):
    with pytest.raises(ValueError, match="untrusted_current_version"):
        resolved_current_version_census_next_url(
            _contract(),
            "Organization",
            f"{BASE}/Organization?_count=1",
            next_link,
            page_entry_count=1,
            expected_page_count=1,
            pre_total=2,
        )


@pytest.mark.parametrize(
    ("current_url", "next_link", "page_entries"),
    (
        (_cursor(token="a", offset=250), _cursor(token="b", offset=500), 250),
        (_cursor(offset=250), _cursor(offset=499), 250),
        (_cursor(offset=250), _cursor(offset=501), 250),
        (
            _cursor(offset=250),
            _cursor(offset=500, path="/fhir/Organization"),
            250,
        ),
        (
            _cursor(offset=250, suffix="&_pretty=true"),
            _cursor(offset=500),
            250,
        ),
        (f"{BASE}/Organization?_count=250", _cursor(offset=251), 250),
    ),
)
def test_smile_cursor_rejects_token_or_offset_drift(
    current_url,
    next_link,
    page_entries,
):
    with pytest.raises(ValueError, match="untrusted_current_version"):
        resolved_current_version_census_next_url(
            _contract(),
            "Organization",
            current_url,
            next_link,
            page_entry_count=page_entries,
            expected_page_count=250,
            pre_total=1000,
        )


@pytest.mark.parametrize("page_entry_count", (0, 4, 121, 248))
def test_smile_cursor_advances_one_logical_window_for_sparse_pages(
    page_entry_count,
):
    continuation = resolved_current_version_census_next_url(
        _contract(),
        "Organization",
        f"{BASE}/Organization?_count=250",
        _cursor(offset=250),
        page_entry_count=page_entry_count,
        expected_page_count=250,
        pre_total=501,
    )

    assert continuation.offset == 250


def test_sparse_checkpoint_geometry_tracks_rows_separately_from_offset():
    proof = current_version_census_initial_proof(
        _contract(),
        "Organization",
        501,
        expected_page_count=250,
    )
    proof = current_version_census_checkpoint_proof(
        proof,
        pages_processed=1,
        rows_processed=0,
        page_entry_count=0,
        expected_page_count=250,
    )
    proof = current_version_census_checkpoint_proof(
        proof,
        pages_processed=2,
        rows_processed=121,
        page_entry_count=121,
        expected_page_count=250,
    )

    assert proof["page_geometry"] == {
        "version": 2,
        "page_count": 250,
        "checkpointed_pages": 2,
        "checkpointed_rows": 121,
        "logical_next_offset": 500,
        "sparse_pages": 2,
        "empty_pages": 1,
    }


def test_smile_cursor_rejects_next_offset_at_advertised_total():
    with pytest.raises(ValueError, match="untrusted_current_version"):
        resolved_current_version_census_next_url(
            _contract(),
            "Organization",
            f"{BASE}/Organization?_count=250",
            _cursor(offset=250),
            page_entry_count=0,
            expected_page_count=250,
            pre_total=250,
        )
