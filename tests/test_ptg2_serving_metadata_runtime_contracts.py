# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Runtime metadata and bounded-probe contracts adjacent to provider serving."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)


_PROVIDER_SET_ID = "01" * 16
_ADDRESS_KEY = "00000000-0000-0000-0000-000000000001"


@pytest.mark.asyncio
async def test_sparse_directory_corroboration_preserves_absent_contacts(
    monkeypatch,
):
    """Overlay sparse directory evidence without inventing optional contacts."""

    monkeypatch.setattr(
        serving,
        "_ptg2_provider_directory_corroboration_table",
        AsyncMock(return_value="mrf.synthetic_corroboration"),
    )
    provider_by_field = {
        "npi": 1234567890,
        "address_key": _ADDRESS_KEY,
        "address_payload": {
            "first_line": "1 Test Way",
            "address_sources": ["provider_directory_fhir"],
        },
    }
    corroboration_by_field = {
        "npi": 1234567890,
        "address_key": _ADDRESS_KEY,
        "provider_directory_plan_context_matched": True,
        "address_network_binding": "payer_directory_corroborated_location",
        "provider_directory_source_id": "synthetic-directory",
    }
    session = FakeSession([FakeResult([corroboration_by_field])])

    overlaid = await serving._overlay_provider_directory_corroboration(
        session,
        [provider_by_field],
        plan_id="synthetic-plan",
    )

    assert overlaid[0]["location_source"] == "provider_directory_fhir"
    assert overlaid[0]["address_payload"]["address_sources"] == [
        "provider_directory_fhir"
    ]
    assert "telephone_number" not in overlaid[0]
    assert "phone_number" not in overlaid[0]["address_payload"]


@pytest.mark.asyncio
async def test_address_table_selection_prefers_unified_and_guards_legacy(
    monkeypatch,
):
    """Prefer a compatible unified table and fail closed if legacy is required."""

    monkeypatch.setattr(serving, "_is_unified_address_requested", lambda: True)
    has_columns = AsyncMock(side_effect=(True, False))
    monkeypatch.setattr(serving, "_has_table_columns", has_columns)
    relation_available = AsyncMock(return_value=False)
    monkeypatch.setattr(serving, "_is_relation_available", relation_available)

    unified = await serving._ptg2_address_serving_table(
        object(), {"npi", "address_payload"}
    )
    unavailable = await serving._ptg2_address_serving_table(
        object(),
        {"npi", "address_payload"},
        require_legacy_available=True,
    )
    monkeypatch.setattr(serving, "_is_unified_address_requested", lambda: False)
    legacy = await serving._ptg2_address_serving_table(
        object(), {"npi", "address_payload"}
    )

    assert unified == f"{serving.PTG2_SCHEMA}.entity_address_unified"
    assert unavailable is None
    assert legacy == f"{serving.PTG2_SCHEMA}.npi_address"


def test_reported_code_filter_handles_absent_untyped_and_unmapped_codes(
    monkeypatch,
):
    """Preserve exact untyped codes and reject unmapped typed identities."""

    filters: list[str] = []
    params_by_name: dict[str, object] = {}
    serving._append_manifest_reported_code_filter(
        filters,
        params_by_name,
        code=None,
        code_system=None,
    )
    assert filters == []

    serving._append_manifest_reported_code_filter(
        filters,
        params_by_name,
        code="custom-code",
        code_system=None,
    )
    assert filters == ["reported_code = :reported_code"]
    assert params_by_name["reported_code"] == "CUSTOM-CODE"

    monkeypatch.setattr(serving, "_normalize_code_system", lambda _value: "UNMAPPED")
    monkeypatch.setattr(serving, "canonical_catalog_code", lambda _system, code: str(code))
    monkeypatch.setattr(serving, "_external_catalog_lookup_pairs", lambda _context: set())
    monkeypatch.setattr(serving, "catalog_code_system_lookup_values", lambda _system: ())
    monkeypatch.setattr(serving, "catalog_code_lookup_values", lambda _system, _code: ())
    unmapped_filters: list[str] = []
    serving._append_manifest_reported_code_filter(
        unmapped_filters,
        {},
        code="typed-code",
        code_system="UNMAPPED",
    )
    assert unmapped_filters == ["FALSE"]


@pytest.mark.parametrize(
    ("record_fields", "error_match"),
    [
        (
            {
                "provider_set_global_id_128": _PROVIDER_SET_ID,
                "provider_set_key": None,
            },
            None,
        ),
        (
            {
                "provider_set_global_id_128": _PROVIDER_SET_ID,
                "provider_set_key": 7,
                "provider_count": -1,
            },
            "negative provider count",
        ),
        (
            {
                "provider_set_global_id_128": _PROVIDER_SET_ID,
                "provider_set_key": 7,
                "provider_count": 1,
                "prefix_member_count": 1,
            },
            "prefix metadata is incomplete",
        ),
        (
            {
                "provider_set_global_id_128": _PROVIDER_SET_ID,
                "provider_set_key": 7,
                "provider_count": 1,
                "prefix_member_count": 1,
                "prefix_member_digest": b"short",
            },
            "prefix metadata is invalid",
        ),
    ],
)
def test_provider_set_metadata_rejects_incomplete_or_invalid_rows(
    record_fields,
    error_match,
):
    """Ignore rows without identity and reject invalid cardinality metadata."""

    if error_match:
        with pytest.raises(serving.PTG2ManifestArtifactError, match=error_match):
            serving._provider_set_metadata_from_fields(record_fields)
        return
    assert serving._provider_set_metadata_from_fields(record_fields) is None


def test_rate_scope_membership_handles_absent_unknown_and_known_groups():
    """Require a normalized group ID and exact binary membership."""

    group_id = "01" * 16
    group_bytes = serving._ptg2_manifest_id_bytes(group_id)
    rate_scope = serving._ManifestRateScope(
        group_ids=(group_id,),
        group_id_bytes=frozenset({group_bytes}),
        id_count=1,
    )

    assert serving._has_rate_scope_group(rate_scope, None) is False
    assert serving._has_rate_scope_group(rate_scope, "02" * 16) is False
    assert serving._has_rate_scope_group(rate_scope, group_id) is True


@pytest.mark.asyncio
async def test_graph_probe_state_distinguishes_no_match_and_bounded_matches(
    monkeypatch,
):
    """Only report success after appended matches satisfy the active filter."""

    append_matches = AsyncMock(side_effect=(0, 1, 1))
    monkeypatch.setattr(serving, "_append_rate_matched_locations", append_matches)
    empty_state = serving._GraphLocationProbeState()
    assert await empty_state.has_enough_after_append(
        object(), strict_v3_tables(), {}, frozenset({7}), [],
        taxonomy_filter_requested=False,
        candidate_limit=1,
    ) is False

    direct_state = serving._GraphLocationProbeState(
        matched_location_rows=[{"npi": 1234567890}],
        provider_set_keys_by_npi={1234567890: {7}},
    )
    assert await direct_state.has_enough_after_append(
        object(), strict_v3_tables(), {}, frozenset({7}), [],
        taxonomy_filter_requested=False,
        candidate_limit=1,
    ) is True

    filtered = serving._GraphLocationCandidates(
        [{"npi": 1234567890}],
        {1234567890: {7}},
        taxonomy_filtered=True,
    )
    monkeypatch.setattr(
        serving,
        "_taxonomy_filtered_candidates",
        AsyncMock(return_value=filtered),
    )
    taxonomy_state = serving._GraphLocationProbeState()
    assert await taxonomy_state.has_enough_after_append(
        object(), strict_v3_tables(), {}, frozenset({7}), [],
        taxonomy_filter_requested=True,
        candidate_limit=1,
    ) is True
    assert taxonomy_state.observed_match_count(taxonomy_filter_requested=True) == 1
    assert taxonomy_state.result(taxonomy_filter_requested=True) is filtered


def test_graph_probe_growth_and_exhaustion_are_explicit():
    """Grow sparse probes and honor both explicit and inferred exhaustion."""

    no_match_growth = serving._next_graph_location_probe_limit(
        10,
        batch_size=10,
        max_candidates=100,
        observed_matches=0,
        required_matches=2,
    )
    density_growth = serving._next_graph_location_probe_limit(
        10,
        batch_size=10,
        max_candidates=100,
        observed_matches=1,
        required_matches=2,
    )

    assert no_match_growth == 40
    assert density_growth >= 20
    assert serving._is_graph_location_source_exhausted(
        [{"_ptg_source_exhausted": False}], 10
    ) is False
    assert serving._is_graph_location_source_exhausted([{"npi": 1}], 10) is True
