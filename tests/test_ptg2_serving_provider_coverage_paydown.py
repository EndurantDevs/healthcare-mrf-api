# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)


def test_graph_providers_by_set_deduplicates_npis_per_provider_set():
    candidates = serving._GraphLocationCandidates(
        location_rows=[
            {"npi": 1, "location_hash": "first"},
            {"npi": 1, "location_hash": "duplicate"},
            {"npi": 2, "location_hash": "second"},
            {"npi": 3, "location_hash": "unmapped"},
        ],
        provider_set_keys_by_npi={1: {10, 11}, 2: {10}},
    )

    provider_set_ids, providers_by_set = serving._graph_providers_by_set(
        candidates,
        {1: {"npi": 1, "provider_name": "One"}},
        {10: "set-a", 11: "set-b"},
        "npi_address",
    )

    assert provider_set_ids == {"set-a", "set-b"}
    assert [provider_row["npi"] for provider_row in providers_by_set["set-a"]] == [
        1,
        2,
    ]
    assert [provider_row["npi"] for provider_row in providers_by_set["set-b"]] == [1]
    assert providers_by_set["set-a"][0]["provider_name"] == "One"
    assert providers_by_set["set-a"][1]["provider_name"] == "TiC provider"


@pytest.mark.asyncio
async def test_provider_rows_for_sets_deduplicates_and_supplies_fallback_rows(
    monkeypatch,
):
    session = object()
    provider_npis = AsyncMock(
        return_value={
            "01" * 16: (11, 12),
            "02" * 16: (12,),
        }
    )
    enrich = AsyncMock(return_value=[{"npi": 11, "provider_name": "Eleven"}])
    monkeypatch.setattr(serving, "_provider_npis_for_sets", provider_npis)
    monkeypatch.setattr(serving, "_enriched_provider_rows_for_npis", enrich)
    monkeypatch.setattr(
        serving, "_is_ptg2_provider_filter_requested", lambda _args: False
    )

    assert (
        await serving._provider_rows_for_sets(
            session, strict_v3_tables(), (), limit_per_set=None
        )
        == {}
    )
    rows_by_set = await serving._provider_rows_for_sets(
        session,
        strict_v3_tables(),
        ("01" * 16, "01" * 16, "02" * 16),
        limit_per_set=None,
        args={"plan_external_id": " plan-a ", "snapshot_id": " snapshot-a "},
    )

    assert rows_by_set["01" * 16] == [
        {"npi": 11, "provider_name": "Eleven"},
        {"npi": 12, "provider_name": "TiC provider"},
    ]
    assert rows_by_set["02" * 16] == [{"npi": 12, "provider_name": "TiC provider"}]
    enrich.assert_awaited_once_with(
        session,
        npis=(11, 12),
        limit=2,
        plan_id="plan-a",
        snapshot_id="snapshot-a",
        source_key="coverage-source",
    )


@pytest.mark.asyncio
async def test_provider_rows_for_sets_filters_before_per_set_limit(monkeypatch):
    session = object()
    provider_set_id = "03" * 16
    provider_npis = AsyncMock(return_value={provider_set_id: (11, 12, 13)})
    taxonomy_filter = AsyncMock(return_value=(12, 13))
    enrich = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_provider_npis_for_sets", provider_npis)
    monkeypatch.setattr(serving, "_filter_npis_by_taxonomy", taxonomy_filter)
    monkeypatch.setattr(serving, "_enriched_provider_rows_for_npis", enrich)
    monkeypatch.setattr(
        serving, "_is_ptg2_provider_filter_requested", lambda _args: True
    )

    provider_rows_result = await serving._provider_rows_for_sets(
        session,
        strict_v3_tables(source_key=None),
        (provider_set_id,),
        limit_per_set=1,
        args={"provider_sex_code": "F", "source_key": " request-source "},
    )

    assert provider_rows_result is None
    assert provider_npis.await_args.kwargs["limit_per_set"] == 1000
    taxonomy_filter.assert_awaited_once_with(
        session,
        {"provider_sex_code": "F", "source_key": " request-source "},
        (11, 12, 13),
        limit=1,
    )
    enrich.assert_awaited_once_with(
        session,
        npis=(12,),
        limit=1,
        plan_id=None,
        snapshot_id=None,
        source_key="request-source",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("network_snapshots", "expected"),
    [
        ([("source-a", "snapshot-a"), ("source-b", "snapshot-b")], "multi"),
        ([("source-a", "snapshot-a")], "snapshot-a"),
        ([], None),
    ],
)
async def test_provider_procedure_search_routes_plan_networks(
    monkeypatch, network_snapshots, expected
):
    monkeypatch.setattr(
        serving,
        "current_network_snapshots_for_plan",
        AsyncMock(return_value=network_snapshots),
    )
    multi = AsyncMock(return_value={"route": "multi"})
    snapshot = AsyncMock(
        side_effect=lambda *_args, snapshot_id, **_kwargs: {"route": snapshot_id}
    )
    monkeypatch.setattr(serving, "_search_multi_ptg2_provider_procedures", multi)
    monkeypatch.setattr(serving, "_search_ptg2_provider_procedures_snapshot", snapshot)

    search_response = await serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_id": "plan-a"},
        SimpleNamespace(limit=10, offset=0),
    )

    assert search_response == (None if expected is None else {"route": expected})


@pytest.mark.asyncio
@pytest.mark.parametrize("resolved_snapshot", [None, "snapshot-current"])
async def test_provider_procedure_search_resolves_explicit_scope(
    monkeypatch, resolved_snapshot
):
    resolver = AsyncMock(return_value=resolved_snapshot)
    snapshot = AsyncMock(return_value={"route": resolved_snapshot})
    monkeypatch.setattr(serving, "resolve_current_ptg2_snapshot_id", resolver)
    monkeypatch.setattr(serving, "_search_ptg2_provider_procedures_snapshot", snapshot)

    search_response = await serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_id": "plan-a", "source_key": "source-a"},
        SimpleNamespace(limit=10, offset=0),
    )

    if resolved_snapshot is None:
        assert search_response is None
        snapshot.assert_not_awaited()
    else:
        assert search_response == {"route": "snapshot-current"}
        snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_enriched_provider_rows_handles_empty_and_unavailable_relations(
    monkeypatch,
):
    session = object()
    assert (
        await serving._enriched_provider_rows_for_npis(session, npis=(0, -1), limit=5)
        == []
    )

    monkeypatch.setattr(
        serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        serving, "_is_relation_available", AsyncMock(return_value=False)
    )
    taxonomy = AsyncMock(
        return_value={
            11: {
                "taxonomy_codes": ["207Q00000X"],
                "specialties": ["Family Medicine"],
            }
        }
    )
    monkeypatch.setattr(serving, "_taxonomy_rows_for_npis", taxonomy)

    provider_rows = await serving._enriched_provider_rows_for_npis(
        session, npis=(12, 11, 11), limit=2
    )

    assert [provider_row["npi"] for provider_row in provider_rows] == [11, 12]
    assert provider_rows[0]["taxonomy_codes"] == ["207Q00000X"]
    assert provider_rows[1]["taxonomy_codes"] == []
    taxonomy.assert_awaited_once_with(session, (11, 12))


@pytest.mark.asyncio
@pytest.mark.parametrize("unified", [False, True])
async def test_enriched_provider_rows_executes_available_address_query(
    monkeypatch, unified
):
    address_table = "mrf.entity_address_unified" if unified else "mrf.npi_address"
    monkeypatch.setattr(
        serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value=address_table),
    )
    monkeypatch.setattr(serving, "_is_relation_available", AsyncMock(return_value=True))
    monkeypatch.setattr(
        serving,
        "_ptg2_table_columns",
        AsyncMock(return_value=frozenset({"telephone_number"})),
    )
    overlay = AsyncMock(return_value=[{"npi": 11, "overlaid": True}])
    monkeypatch.setattr(serving, "_overlay_provider_directory_corroboration", overlay)
    session = FakeSession([FakeResult([{"npi": 11, "provider_name": "Eleven"}])])

    provider_rows = await serving._enriched_provider_rows_for_npis(
        session,
        npis=(11,),
        limit=1,
        plan_id="plan-a",
        snapshot_id="snapshot-a",
        source_key="source-a",
    )

    assert provider_rows == [{"npi": 11, "overlaid": True}]
    statement = str(session.calls[0][0][0])
    if unified:
        assert "fallback_addresses AS MATERIALIZED" in statement
        assert "na.telephone_number" in statement
        assert "NULL::varchar AS fax_number" in statement
    else:
        assert "fallback_addresses AS MATERIALIZED" not in statement
    overlay.assert_awaited_once_with(
        session,
        [{"npi": 11, "provider_name": "Eleven"}],
        plan_id="plan-a",
        snapshot_id="snapshot-a",
        source_key="source-a",
    )


@pytest.mark.parametrize(
    ("provider_overrides", "location_overrides", "address_overrides", "expected"),
    [
        (
            {"source_trace": [{"source_file_version_id": "version-a"}]},
            {"location_source": "payer_confirmed_location"},
            {"address_verification_evidence": {"source_record_id": "record-a"}},
            "payer_confirmed_location",
        ),
        ({}, {}, {"address_precision": "city_zip"}, "city_zip_fallback"),
        (
            {},
            {},
            {"address_sources": ["mrf", "nppes"], "source_count": 2},
            "multi_source_direct_mrf_address",
        ),
        ({}, {}, {"address_source_mask": 2}, "direct_mrf_address"),
        ({}, {}, {"multi_source_confirmed": True}, "multi_source_provider_address"),
        ({}, {"location_source": "npi_address"}, {}, "nppes_provider_address"),
        (
            {},
            {"location_source": "entity_address_unified"},
            {},
            "unified_provider_address",
        ),
        ({}, {}, {}, "unknown"),
    ],
)
def test_address_verification_classifies_address_provenance(
    provider_overrides, location_overrides, address_overrides, expected
):
    provider_data_map = {"provider_name": "Coverage Provider", **provider_overrides}
    location_data_map = dict(location_overrides)
    address_data_map = {
        "first_line": "100 Coverage Street",
        **address_overrides,
    }

    verification = serving._address_verification_payload(
        provider_data_map, location_data_map, address_data_map
    )

    assert verification["address_evidence_level"] == expected
    assert verification["displayed_address_present"] is True
    assert verification["requires_location_confirmation"] is (
        expected != "payer_confirmed_location"
    )


def test_address_verification_marks_missing_display_address_unknown():
    verification = serving._address_verification_payload({}, {}, {})

    assert verification == {
        "rate_network_binding": "tic_provider_group_npi_tin",
        "address_network_binding": "inferred_from_provider_identity",
        "address_evidence_level": "unknown",
        "requires_location_confirmation": True,
        "reason": "PTG proves the provider identity is in network, but no displayable address is available.",
        "displayed_address_present": False,
        "network_bound_address": False,
    }


def test_provider_directory_network_matches_aliases_and_pre_shaped_rows():
    provider_data_map = {"network_names": ["Coverage PPO", "Second Network"]}
    address_data_map = {
        "provider_directory_org_name": "Coverage Health",
        "provider_directory_network_matches": [
            "not-a-network-row",
            {
                "ptg_network_name": "Coverage PPO",
                "provider_directory_network_name": "Coverage Preferred",
            },
            {
                "ptg_network_name": "Coverage PPO",
                "provider_directory_network_name": "Duplicate Preferred",
            },
            {
                "resource_id": "network-2",
                "ref": "Organization/network-2",
                "name": "unrelated",
                "aliases": ["Second-Network", "Second Network"],
            },
        ],
    }

    matches = serving._provider_directory_network_name_matches(
        provider_data_map, address_data_map
    )

    assert [match["ptg_network_name"] for match in matches] == [
        "Coverage PPO",
        "Second Network",
    ]
    assert matches[1]["provider_directory_network_resource_id"] == "network-2"
    assert matches[1]["provider_directory_issuer_key"] == "coveragehealth"
    assert serving._provider_directory_network_name_matches({}, address_data_map) == []


def test_payload_helpers_cover_invalid_and_nested_guard_paths():
    assert serving._safe_table_name(None) is None
    with pytest.raises(ValueError, match="mode must be"):
        serving.normalize_ptg2_mode("unsupported")

    assert serving._is_truthy_payload(1) is True
    assert serving._optional_bool_payload(0) is False
    assert serving._optional_bool_payload("unknown") is None
    assert serving._coerce_str_list_payload({"not": "a list"}) == []
    assert serving._coerce_str_list_payload(["", "Coverage PPO"]) == [
        "Coverage PPO"
    ]
    assert serving._has_displayed_address_payload(
        {}, {"address": {"city": "Chicago", "state": "IL"}}
    )


def test_provider_directory_helpers_reject_malformed_evidence():
    malformed_evidence_by_field = {
        "address_verification_evidence": '["unexpected"]'
    }

    assert serving._has_plan_context_match(malformed_evidence_by_field) is False
    assert serving._provider_directory_network_match_context_payload(
        malformed_evidence_by_field
    ) == {}
    assert (
        serving._has_direct_payer_location_record_evidence(malformed_evidence_by_field)
        is False
    )
    assert (
        serving._provider_directory_address_verification_evidence(
            malformed_evidence_by_field, []
        )
        is None
    )
    assert serving._has_source_file_version_trace({"source_trace": "invalid"}) is False


def test_provider_directory_helpers_cover_network_evidence_edges():
    matches = serving._provider_directory_network_name_matches(
        {"network_names": ["Coverage PPO"]},
        {
            "provider_directory_network_matches": [
                {"name": "Coverage PPO", "aliases": {"invalid": "shape"}}
            ]
        },
    )

    assert [match["ptg_network_name"] for match in matches] == ["Coverage PPO"]
    assert serving._provider_directory_address_verification_evidence(
        {
            "address_verification_evidence": {
                "matched_on": "npi_address_key_role_location_network_name"
            }
        },
        matches,
    )["matched_on"] == "npi_address_key_role_location_network_name"
    assert serving._provider_directory_address_verification_evidence(
        {
            "address_verification_evidence": {
                "matched_on": "npi_address_key_role_location_network_name"
            }
        },
        [],
    )["matched_on"] == "npi_address_key_role_location"


def test_strict_v3_metadata_helpers_reject_missing_values():
    with pytest.raises(serving.PTG2ManifestArtifactError, match="shared-block"):
        serving._required_shared_snapshot_key(
            strict_v3_tables(shared_snapshot_key=None)
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="source_count"):
        serving._required_source_count(
            SimpleNamespace(uses_shared_blocks=True, source_count=None)
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="logical snapshot"):
        serving._required_logical_snapshot_id(strict_v3_tables(snapshot_id="  "))
