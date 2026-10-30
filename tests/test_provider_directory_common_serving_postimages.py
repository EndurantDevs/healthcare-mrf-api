# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Expected scoped row projections and canonical hash-fold compatibility."""

import importlib
import json
import re
import sqlite3
from types import SimpleNamespace

import pytest

from process import entity_address_snapshot_receipt as snapshots
from process import provider_directory_common_serving_postimages as postimages
from tests.test_provider_directory_common_serving_recovery_document import retained

native = importlib.import_module("process.entity_address_unified")


def evaluate(query, tables):
    database = sqlite3.connect(":memory:")
    database.execute("ATTACH DATABASE ':memory:' AS unit")
    for table, table_rows in tables.items():
        database.execute(
            f"CREATE TABLE unit.{table}(row_json TEXT,npi BIGINT,location_key TEXT,candidate_id TEXT,source_id TEXT)"
        )
        for projected_row in table_rows:
            database.execute(
                f"INSERT INTO unit.{table} VALUES(?,?,?,?,?)",
                (
                    json.dumps(projected_row, sort_keys=True),
                    projected_row.get("npi"),
                    projected_row.get("location_key"),
                    projected_row.get("candidate_id"),
                    projected_row.get("source_id"),
                ),
            )
    sql = (
        query.query_sql.replace("to_jsonb(live)", "json(live.row_json)")
        .replace("jsonb_build_object", "json_object")
        .replace("::timestamptz", "")
        .replace("::timestamp", "")
        .replace("::jsonb", "")
    )
    sql = sql.replace("SELECT row_json || CASE", "SELECT json_patch(row_json, CASE").replace(
        "END AS row_json", "END) AS row_json"
    )
    sql = re.sub(r"=ANY\(CAST\(:(\w+) AS varchar\[\]\)\)", r" IN (SELECT value FROM json_each(:\1))", sql)
    parameters_by_name = {key: json.dumps(parameter_value) for key, parameter_value in query.parameters}
    projected_rows = [json.loads(projected_row[0]) for projected_row in database.execute(sql, parameters_by_name)]
    database.close()
    return sorted(projected_rows, key=lambda projected_row: json.dumps(projected_row, sort_keys=True))


def descriptor(generated=True):
    return SimpleNamespace(
        schema="unit",
        stage_table="stage_unified",
        support_stages=(
            ("entity_address_evidence", "stage_evidence", 10),
            ("facility_anchor_npi_candidate", "stage_facility", 11),
        ),
        transition_at="2026-01-03T03:06:07.123456+00:00",
        facility_generated=generated,
    )


def test_profile_delta_postimage_keeps_sibling_source_evidence_within_affected_npis():
    base = postimages.ProjectedRows(
        "unit",
        "provider_directory_profile_evidence",
        "SELECT to_jsonb(live) AS row_json FROM unit.evidence live WHERE live.npi=1",
    )
    queries_by_family = {"evidence": base}
    delta = SimpleNamespace(
        schema="unit",
        refresh_source_ids=("changed",),
        removed_source_ids=("gone",),
        profile_stage="profile_stage",
        evidence_stage="evidence_stage",
    )
    prepared = SimpleNamespace(
        fhir=SimpleNamespace(_schema=lambda: "unit", ProviderDirectoryPreparedProfileDelta=SimpleNamespace),
        profile_delta=delta,
        profile_bundle=SimpleNamespace(stages=[]),
    )
    postimages._profile_queries(prepared, queries_by_family)
    actual = evaluate(
        queries_by_family["evidence"],
        {
            "evidence": [
                dict(npi=1, source_id="sibling", value="kept"),
                dict(npi=1, source_id="changed", value="old"),
                dict(npi=1, source_id="gone", value="removed"),
                dict(npi=2, source_id="other", value="outside"),
            ],
            "evidence_stage": [dict(npi=1, source_id="changed", value="new")],
        },
    )
    assert actual == [dict(npi=1, source_id="changed", value="new"), dict(npi=1, source_id="sibling", value="kept")]
    assert "profile_stage" in queries_by_family["profile"].query_sql


def test_evidence_retirement_projection_preserves_old_history_and_exact_preparation_time():
    base = postimages.ProjectedRows(
        "unit", "entity_address_evidence", "SELECT to_jsonb(live) AS row_json FROM unit.evidence live"
    )
    query = postimages._address_evidence_query(native, descriptor(), base)
    actual = evaluate(
        query,
        {
            "evidence": [dict(evidence_id=1, retired_at=None), dict(evidence_id=2, retired_at="2025-01-01")],
            "stage_evidence": [dict(evidence_id=3, retired_at=None)],
        },
    )
    assert actual == [
        dict(evidence_id=1, retired_at="2026-01-03T03:06:07.123456+00:00"),
        dict(evidence_id=2, retired_at="2025-01-01"),
        dict(evidence_id=3, retired_at=None),
    ]


@pytest.mark.parametrize("generated", [False, True])
def test_facility_projection_matches_survivor_reviews_and_displaced_history(generated):
    base = postimages.ProjectedRows(
        "unit", "facility_anchor_npi_candidate", "SELECT to_jsonb(live) AS row_json FROM unit.facility live"
    )
    query = postimages._facility_query(native, descriptor(generated), base)
    previous_candidates = [
        dict(
            candidate_id="same",
            location_key="same",
            candidate_status="single",
            review_status="approved",
            updated_at="previous_candidates",
        ),
        dict(
            candidate_id="gone",
            location_key="gone",
            candidate_status="single",
            review_status="rejected",
            updated_at="previous_candidates",
        ),
    ]
    staged_candidates = [
        dict(
            candidate_id="same",
            location_key="same",
            candidate_status="single",
            review_status="approved",
            updated_at="new",
        )
    ]
    actual = evaluate(
        query,
        {
            "facility": previous_candidates,
            "stage_facility": staged_candidates,
            "stage_unified": [dict(location_key="same")],
        },
    )
    expected_candidates = [
        dict(
            candidate_id="gone",
            location_key="gone",
            candidate_status="inactive",
            review_status="rejected",
            updated_at="2026-01-03T03:06:07.123456",
        ),
        staged_candidates[0] if generated else previous_candidates[0],
    ]
    assert actual == expected_candidates


def test_recovery_queries_use_only_retained_scope_names_and_fixed_roots():
    queries = postimages.current_postimage_queries("unit", retained(), ["changed", "gone"])
    assert set(queries) == set(postimages.documents.FAMILIES)
    assert all(
        "pd_location_scope_before_" in query.query_sql
        for family, query in queries.items()
        if family not in {"profile", "evidence", "overlay"}
    )
    assert all("pd_npi_scope_before_" in queries[family].query_sql for family in ["profile", "evidence"])
    assert dict(queries["overlay"].parameters) == {"changed_source_ids": ("changed", "gone")}


@pytest.mark.asyncio
async def test_subquery_projection_reuses_existing_chunk_digest_bytes():
    receipts = [dict(chunk_ordinal=0, chunk_row_count=3, chunk_sha256="a" * 64)]

    class Session:
        async def execute(self, statement, parameters):
            self.statement, self.parameters = str(statement), parameters
            return SimpleNamespace(mappings=lambda: receipts)

    direct, query = Session(), Session()
    baseline = await snapshots._projected_row_identity(direct, "unit", "live", row_json_sql="to_jsonb(row_value)")
    projected = await snapshots._projected_row_identity(
        query,
        "unit",
        "live",
        row_json_sql="row_value.row_json",
        row_source_sql="(SELECT to_jsonb(live) AS row_json FROM unit.stage live)",
    )
    assert baseline == projected
    assert baseline[0] == 3 and len(baseline[1]) == 64
    assert 'FROM "unit"."live" AS row_value' in direct.statement
    assert "FROM (SELECT to_jsonb(live) AS row_json FROM unit.stage live) AS row_value" in query.statement
    assert direct.parameters == query.parameters
