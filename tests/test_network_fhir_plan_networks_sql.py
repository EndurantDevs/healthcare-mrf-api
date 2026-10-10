# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scoped plan evidence stays reusable without changing complete preview guards."""

from types import SimpleNamespace

import pytest

from process import network_fhir_membership_source as source
from process import registry_imported_selection_preview as preview


def _pin(source_id):
    return source.PinnedFHIRMembershipSource(
        "synthetic_fhir",
        source_id,
        "synthetic-endpoint",
        "synthetic-dataset",
        "a" * 64,
        "synthetic-release",
        1,
        "synthetic-scope",
        "2026-10-01",
    )


@pytest.mark.parametrize("source_id", ["cms-npd", "synthetic-fhir"])
@pytest.mark.parametrize("reviewed,approved_only", [(False, False), (True, False), (True, True)])
def test_plan_evidence_is_scoped_once_and_preserves_null_references(source_id, reviewed, approved_only):
    sql = source._extraction_sql(_pin(source_id), '"synthetic_registry"', reviewed, approved_only)
    plan_cte = sql.split(", plan_networks AS MATERIALIZED (", 1)[1].split("), providers AS (", 1)[0]
    expanded = sql.split(", expanded AS MATERIALIZED (", 1)[1].split("), selected_sites AS (", 1)[0]
    dataset, source_parameter, release_parameter = (8, 12, 13) if reviewed else (1, 5, 6)
    assert 'SELECT DISTINCT ON (reference COLLATE "C") reference FROM page CROSS JOIN LATERAL' in plan_cte
    assert "THEN payload->'insurance_plan_refs' ELSE '[]'::jsonb END" in plan_cte
    assert f"plan.dataset_id=${dataset} AND plan.resource_type='InsurancePlan'" in plan_cte
    assert "AND reference ~ '^InsurancePlan/[A-Za-z0-9.-]{1,64}$'" in plan_cte
    assert "AND plan.resource_id=split_part(reference,'/',2)" in plan_cte
    assert f"evidence.source_id=${source_parameter} AND evidence.release_id=${release_parameter}" in plan_cte
    assert "evidence.insurance_plan_resource_id=plan.resource_id" in plan_cte
    assert "plan_network.reference ~ '^Organization/[A-Za-z0-9.-]{1,64}$'" in plan_cte
    assert "evidence.network_resource_id=split_part(plan_network.reference,'/',2)" in plan_cte
    assert "LEFT JOIN" in plan_cte and " WHERE " not in plan_cte
    assert sql.count(".provider_directory_insurance_network_plan_evidence evidence") == 1
    assert "UNION\n          SELECT plan_networks.network_resource_id" in expanded
    assert (
        'LEFT JOIN plan_networks ON plan_networks.insurance_plan_reference COLLATE "C"=refs.reference COLLATE "C"'
        in expanded
    )
    assert ".provider_directory_insurance_network_plan_evidence" not in expanded
    if source_id == "cms-npd":
        for fragment in (
            "plan_witness.dataset_id=plan.dataset_id",
            "plan_witness.resource_type=plan.resource_type",
            "plan_witness.resource_id=plan.resource_id",
            f"plan_witness.source_id=${source_parameter}",
            f"plan_witness.release_id=${release_parameter}",
            "plan_witness.normalized_payload_hash=plan.payload_hash",
            "plan.acquired_resource_sha256=plan_witness.raw_payload_sha256",
            "evidence.plan_payload_sha256=plan_witness.raw_payload_sha256",
        ):
            assert fragment in plan_cte
    else:
        assert "plan_witness" not in plan_cte
        assert "evidence.plan_payload_sha256=plan.acquired_resource_sha256" in plan_cte


def test_complete_preview_keeps_reusable_plan_evidence_and_full_accounting():
    recipe = SimpleNamespace(source_pin=_pin("cms-npd"))
    sql = preview._impact_sql(recipe, '"synthetic_registry"', '"synthetic_selection"', "synthetic_registry")
    assert ", plan_networks AS MATERIALIZED (" in sql
    assert "plan.dataset_id=$8" in sql
    assert "evidence.source_id=$9 AND evidence.release_id=$10" in sql
    assert "plan_witness.source_id=$9 AND plan_witness.release_id=$10" in sql
    assert "LIMIT" not in sql and ", encoded AS (" not in sql
    assert "count(*) FILTER(WHERE NOT unresolved AND NOT omitted) AS mapped_rows" in sql
    assert "count(*) FILTER(WHERE omitted) AS omitted_rows" in sql
    assert "count(*) FILTER(WHERE unresolved AND NOT omitted) AS unresolved_rows" in sql
    assert preview.PREVIEW_DEADLINE_SECONDS == 2.5


@pytest.mark.parametrize("drift", ["source", "expanded", "cursor"])
def test_preview_still_refuses_extraction_shape_drift(monkeypatch, drift):
    original = preview.fhir_extraction_sql

    def changed_sql(*arguments, **options):
        sql = original(*arguments, **options)
        if drift == "source":
            return sql.replace("LIMIT $11", "LIMIT $11::bigint")
        if drift == "expanded":
            return sql.replace("LIMIT 5001", "LIMIT 5002")
        return sql.replace("FROM page", "FROM page WHERE $9::text IS NULL")

    monkeypatch.setattr(preview, "fhir_extraction_sql", changed_sql)
    with pytest.raises(preview.RegistryImportedSelectionUnavailable, match="selection_unavailable"):
        preview._impact_sql(
            SimpleNamespace(source_pin=_pin("cms-npd")),
            '"synthetic_registry"',
            '"synthetic_selection"',
            "synthetic_registry",
        )
