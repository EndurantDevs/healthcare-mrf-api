# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused edge contracts for PTG provider-response shaping."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from api import ptg2_serving as serving
from api.ptg2_types import PTG2ServingTables


def test_contact_and_address_helpers_reject_nullish_or_non_object_payloads():
    """Normalize source sentinels without treating arrays as addresses."""

    assert serving._non_nullish_contact_value(" NULL ") is None
    assert serving._ptg2_row_address_key({"address_payload": "[]"}) is None
    assert serving._selected_location_requests(
        [{"address_payload": "[]"}]
    ) == []


def test_shared_code_scope_keeps_market_filter_without_a_plan_filter():
    """Allow a logical snapshot and market filter without inventing a plan."""

    _, _, query_parameters_by_name, _ = serving._shared_v3_code_scope_sql(
        PTG2ServingTables(snapshot_id="ptg2:synthetic"),
        requested_plan="",
        plan_market_type="group",
    )

    assert query_parameters_by_name == {
        "logical_snapshot_id": "ptg2:synthetic",
        "plan_market_type": "group",
    }


def test_manifest_source_fields_prefer_explicit_artifact_identity():
    """Do not replace an explicit artifact key with its logical source key."""

    source_fields_by_name = serving._manifest_provider_source_fields(
        {"source_artifact_key": 7, "source_key": 9},
        {},
    )

    assert source_fields_by_name["source_artifact_key"] == 7


def test_empty_inferred_taxonomy_rule_produces_no_sql(monkeypatch):
    """Keep an empty inferred rule from producing an invalid predicate."""

    monkeypatch.setattr(
        serving,
        "_inferred_provider_taxonomy_rule",
        lambda _args: SimpleNamespace(taxonomy_codes=()),
    )
    query_parameters_by_name = {}

    assert serving._inferred_provider_taxonomy_code_sql(
        {},
        nt_alias="taxonomy",
        schema="synthetic",
        params=query_parameters_by_name,
        param_prefix="inferred",
    ) == ""
    assert query_parameters_by_name == {}


class _UnexpectedQuerySession:
    async def execute(self, *_args, **_kwargs):
        raise AssertionError("taxonomy-free filtering must not query the database")


@pytest.mark.asyncio
async def test_taxonomy_free_npi_filter_is_bounded_without_a_query():
    """Return sorted candidates directly when no provider predicate exists."""

    filtered_npis = await serving._filter_npis_by_taxonomy(
        _UnexpectedQuerySession(),
        {},
        [2, 1],
        limit=1,
    )

    assert filtered_npis == (1,)


def test_provenance_identifiers_normalize_scalars_blanks_and_duplicates():
    """Retain stable provenance IDs while removing empty duplicates."""

    assert serving._coerce_provenance_id_list("   ") == []
    assert serving._coerce_provenance_id_list(7) == ["7"]
    assert serving._coerce_provenance_id_list(["run", "run", ""]) == ["run"]


def test_provenance_retrieval_time_handles_empty_and_utc_timestamp_runs():
    """Accept an unambiguous ISO run timestamp and reject an empty one."""

    assert serving._provenance_run_retrieved_at(None) is None
    assert serving._provenance_run_retrieved_at("2026-08-04T01:02:03Z") == (
        "2026-08-04T01:02:03+00:00"
    )


def test_explicit_price_tolerance_matches_a_rate_inside_the_window():
    """Honor a caller-supplied numeric tolerance without replacing it."""

    assert serving._is_price_filter_match(
        {"negotiated_rate": 100},
        {"negotiated_rate": "100", "rate_tolerance": "0.50"},
    )


def test_price_hydration_skips_missing_atom_memberships():
    """Keep the price bucket empty when its atom payload is unavailable."""

    assert serving._version_three_price_rows(
        (1,),
        {1: (99,)},
        {},
        {},
        {},
    ) == {1: []}


def test_address_provenance_index_deduplicates_complete_lineage():
    """Return one public lineage entry for one repeated source tuple."""

    provenance_row_by_field = {
        "location_key": "synthetic-location",
        "source_id": 8,
        "source_record_key": "provider_directory_fhir:synthetic",
        "source_run_id": "20260731",
    }

    provenance_by_location_key = serving._index_address_provenance(
        [provenance_row_by_field, dict(provenance_row_by_field)]
    )

    assert len(provenance_by_location_key["synthetic-location"]) == 1


def test_non_object_address_payloads_fail_closed_by_evidence_state():
    """Drop claimed GEO evidence and redact an otherwise usable rate row."""

    location_rows = [
        {
            "npi": 1234567890,
            "address_payload": "[]",
            "_geo_evidence_level": "nppes_anchor",
        },
        {
            "npi": 1234567891,
            "address_payload": "[]",
            "city": "Example",
            "state": "EX",
            "zip5": "00000",
        },
    ]

    serving._apply_address_provenance(location_rows, {})

    assert location_rows == [
        {
            "npi": 1234567891,
            "address_payload": "{}",
            "_ptg_unproven_address": True,
        }
    ]


def test_unscoped_rate_rows_remain_untouched_without_atomic_options():
    """Preserve non-provider rows without minting incomplete option refs."""

    unscoped_rate_by_field = {"prices": []}

    assert serving._merge_ptg2_provider_rate_items(
        [unscoped_rate_by_field]
    ) == [unscoped_rate_by_field]
