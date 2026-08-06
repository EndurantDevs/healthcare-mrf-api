# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace

import pytest
from sanic import response
from sanic.exceptions import InvalidUsage

from api import formulary_fhir_serving as serving
from api.endpoint import formulary


FHIR_ID = "fhir_abcdefghijklmnopqrstuvwxyz"


def _request(args=None):
    return SimpleNamespace(
        args=args or {},
        ctx=SimpleNamespace(sa_session=object()),
    )


def _payload(http_response):
    return json.loads(http_response.body)


class _SQLResult:
    def __init__(self, *, rows=(), scalar_value=None, first_value=None):
        self._rows = list(rows)
        self._scalar_value = scalar_value
        self._first_value = first_value

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar_value

    def first(self):
        return self._first_value


class _PagingSession:
    def __init__(self):
        self.calls = []

    async def execute(self, statement, params):
        sql = str(statement)
        self.calls.append((sql, dict(params)))
        if sql.startswith("SELECT COUNT(*)"):
            return _SQLResult(scalar_value=2)
        return _SQLResult(rows=[{"rxnorm_id": "200"}, {"rxnorm_id": "100"}])


def test_source_selection_preserves_legacy_default_and_source_filter_selects_fhir():
    assert serving.source_selection({}) == "legacy"
    assert serving.source_selection({"source_type": "all"}) == "all"
    assert serving.source_selection({"source_id": "synthetic-source"}) == "fhir"
    assert serving.source_selection({"source_plan_identifier": "alias-a"}) == "fhir"
    with pytest.raises(InvalidUsage):
        serving.source_selection(
            {"source_type": "legacy", "source_id": "synthetic-source"}
        )
    with pytest.raises(InvalidUsage, match="annual legacy filters"):
        serving.source_selection(
            {"source_type": "fhir", "year": "2026"}
        )


@pytest.mark.asyncio
async def test_fhir_id_dispatches_detail_without_legacy_year_decoding(monkeypatch):
    async def _fhir_detail(_request, formulary_id):
        return response.json({"formulary_id": formulary_id, "year": None})

    monkeypatch.setattr(serving, "get_fhir_formulary", _fhir_detail)

    result = await formulary.get_formulary(_request(), FHIR_ID)

    assert _payload(result) == {"formulary_id": FHIR_ID, "year": None}


async def _disagreement_header(
    _session,
    _formulary_id,
    *,
    source_plan_identifier=None,
):
    assert source_plan_identifier is None
    return {
        "public_id": FHIR_ID,
        "source_id": "synthetic-source",
        "upstream_list_id": "synthetic-list",
        "upstream_version_id": "7",
        "status": "current",
        "upstream_last_updated": dt.datetime(2026, 8, 1, tzinfo=dt.UTC),
        "metadata_json": {},
        "dataset_id": "synthetic-dataset",
        "cutoff_at": dt.datetime(2026, 8, 2, tzinfo=dt.UTC),
        "published_at": dt.datetime(2026, 8, 3, tzinfo=dt.UTC),
        "coverage_hash": "c" * 64,
        "membership_hash": "m" * 64,
    }


async def _disagreement_variants(*_args, **_kwargs):
    return [
        {
            "rxnorm_id": "100001",
            "drug_name": "Synthetic drug",
            "upstream_medication_id": "drug-a",
            "upstream_version_id": "1",
            "upstream_last_updated": dt.datetime(2026, 8, 1, tzinfo=dt.UTC),
            "codings_json": [],
            "alternatives_json": [
                {
                    "raw_reference": "MedicationKnowledge/missing",
                    "corrected_reference": None,
                    "resolved_medication_id": None,
                    "resolved": False,
                    "rule_version": None,
                    "evidence": {"same_source": True},
                }
            ],
            "source_plan_identifier": "alias-a",
            "drug_tier": "Tier 1",
            "prior_authorization": False,
            "step_therapy": False,
            "quantity_limit": False,
        },
        {
            "rxnorm_id": "100001",
            "drug_name": "Synthetic drug",
            "upstream_medication_id": "drug-b",
            "upstream_version_id": "2",
            "upstream_last_updated": dt.datetime(2026, 8, 2, tzinfo=dt.UTC),
            "codings_json": [],
            "source_plan_identifier": "alias-b",
            "drug_tier": "Tier 2",
            "prior_authorization": True,
            "step_therapy": False,
            "quantity_limit": False,
        },
    ]


@pytest.mark.asyncio
async def test_alias_disagreement_is_preserved_and_conflicting_scalars_are_null(
    monkeypatch,
):
    monkeypatch.setattr(serving, "_formulary_header", _disagreement_header)
    monkeypatch.setattr(serving, "_variant_rows", _disagreement_variants)

    http_response = await serving.get_fhir_formulary_drug(
        _request(),
        FHIR_ID,
        "100001",
    )
    response_by_field = _payload(http_response)

    assert response_by_field["year"] is None
    assert response_by_field["drug_tier"] is None
    assert response_by_field["prior_authorization"] is None
    assert response_by_field["step_therapy"] is False
    assert response_by_field["dataset"]["dataset_id"] == "synthetic-dataset"
    assert [
        variant_by_field["source_plan_identifier"]
        for variant_by_field in response_by_field["coverage_variants"]
    ] == [
        "alias-a",
        "alias-b",
    ]
    assert response_by_field["coverage_variants"][0]["alternatives"] == [
        {
            "raw_reference": "MedicationKnowledge/missing",
            "corrected_reference": None,
            "resolved_medication_id": None,
            "resolved": False,
            "rule_version": None,
            "evidence": {"same_source": True},
        }
    ]


def test_source_all_cross_formulary_merge_preserves_both_stores():
    legacy = response.json(
        {"rxnorm_id": "100001", "formularies": [{"formulary_id": "PLAN:2026"}]}
    )
    fhir = response.json(
        {"rxnorm_id": "100001", "formularies": [{"formulary_id": FHIR_ID}]}
    )

    payload = _payload(
        serving.merge_cross_formulary_responses("100001", legacy, fhir)
    )

    assert {
        formulary_data["formulary_id"]
        for formulary_data in payload["formularies"]
    } == {
        "PLAN:2026",
        FHIR_ID,
    }


def test_fhir_id_shape_is_strict():
    assert serving.is_fhir_formulary_id(FHIR_ID)
    assert not serving.is_fhir_formulary_id("fhir_UPPERCASE")
    assert not serving.is_fhir_formulary_id("PLAN:2026")


@pytest.mark.asyncio
async def test_fhir_drug_page_is_grouped_and_filtered_in_sql():
    session = _PagingSession()

    ids, total = await serving._paged_rxnorm_ids(
        session,
        FHIR_ID,
        source_plan_identifier="alias-a",
        args={
            "tier": "Tier 1",
            "authorization_required": "true",
        },
        limit=2,
        offset=4,
        sort_field="tier",
        order="desc",
    )

    assert ids == ["200", "100"]
    assert total == 2
    count_sql, count_params = session.calls[0]
    page_sql, page_params = session.calls[1]
    assert "GROUP BY m.rxnorm_id" in count_sql
    assert "BOOL_OR(m.drug_tier = :tier)" in count_sql
    assert "BOOL_OR(m.prior_authorization = :expected_prior_authorization)" in count_sql
    assert "a.source_plan_identifier = :source_plan_identifier" in count_sql
    assert "ORDER BY COALESCE(sort_tier, '') DESC" in page_sql
    assert count_params["source_plan_identifier"] == "alias-a"
    assert page_params["limit"] == 2
    assert page_params["offset"] == 4


@pytest.mark.asyncio
async def test_variant_query_preserves_alternative_reference_evidence():
    session = _PagingSession()

    await serving._variant_rows(
        session,
        FHIR_ID,
        source_plan_identifier=None,
        rxnorm_id="100001",
    )

    sql, params = session.calls[0]
    assert "fhir_formulary_alternative" in sql
    assert "AS alternatives_json" in sql
    assert params["rxnorm_id"] == "100001"


class _SummarySession:
    def __init__(self):
        self.calls = []

    async def execute(self, statement, params):
        sql = str(statement)
        self.calls.append((sql, dict(params)))
        if "SELECT COUNT(*) AS total_drugs" in sql:
            return _SQLResult(
                first_value={
                    "total_drugs": 3,
                    "prior_true": 1,
                    "prior_false": 1,
                    "prior_unknown": 1,
                    "step_true": 0,
                    "step_false": 3,
                    "step_unknown": 0,
                    "quantity_true": 1,
                    "quantity_false": 2,
                    "quantity_unknown": 0,
                }
            )
        return _SQLResult(
            rows=[
                {"tier_label": "CONFLICTING_OR_UNKNOWN", "drug_count": 1},
                {"tier_label": "Tier 1", "drug_count": 2},
            ]
        )


@pytest.mark.asyncio
async def test_summary_consensus_is_aggregated_in_postgresql():
    session = _SummarySession()

    summary, tiers = await serving._summary_statistics(
        session,
        FHIR_ID,
        source_plan_identifier="alias-a",
    )

    assert summary["total_drugs"] == 3
    assert tiers == [("CONFLICTING_OR_UNKNOWN", 1), ("Tier 1", 2)]
    assert len(session.calls) == 2
    assert all("GROUP BY m.rxnorm_id" in sql for sql, _params in session.calls)
    assert all(
        "a.source_plan_identifier = :source_plan_identifier" in sql
        for sql, _params in session.calls
    )


@pytest.mark.asyncio
async def test_cross_store_filters_do_not_leak_into_incompatible_store(monkeypatch):
    calls = []

    async def _legacy(_request, rxnorm_id):
        calls.append(("legacy", rxnorm_id))
        return response.json(
            {"rxnorm_id": rxnorm_id, "formularies": [{"formulary_id": "PLAN:2026"}]}
        )

    async def _fhir(_request, rxnorm_id):
        calls.append(("fhir", rxnorm_id))
        return response.json(
            {"rxnorm_id": rxnorm_id, "formularies": [{"formulary_id": FHIR_ID}]}
        )

    monkeypatch.setattr(formulary, "_legacy_cross_formulary_drug", _legacy)
    monkeypatch.setattr(serving, "cross_fhir_formulary_drug", _fhir)

    source_filtered = await formulary.cross_formulary_drug(
        _request({"source_type": "all", "source_id": "synthetic-source"}),
        "100001",
    )
    assert [
        formulary_data["formulary_id"]
        for formulary_data in _payload(source_filtered)["formularies"]
    ] == [FHIR_ID]
    assert calls == [("fhir", "100001")]

    calls.clear()
    year_filtered = await formulary.cross_formulary_drug(
        _request({"source_type": "all", "year": "2026"}),
        "100001",
    )
    assert [
        formulary_data["formulary_id"]
        for formulary_data in _payload(year_filtered)["formularies"]
    ] == ["PLAN:2026"]
    assert calls == [("legacy", "100001")]
