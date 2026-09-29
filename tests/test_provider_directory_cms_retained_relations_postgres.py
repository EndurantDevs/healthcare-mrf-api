# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse real intake dependencies without writes; reject mutable physical edge or payload drift."""

from types import SimpleNamespace

import pytest
from sqlalchemy import text

from process import provider_directory_cms_desired_fence as dependencies
from tests import cms_npd_admission_postgres_support as support
from tests.cms_npd_admission_postgres_support import cms_artifact_root, fhir
from tests.test_cms_npd_intake_postgres import _admit


def _execution(result):
    descriptor = result["cms_serving_candidate"]
    pair = descriptor["desired_cms_dataset"]
    return SimpleNamespace(
        attestation=SimpleNamespace(
            operation="publish",
            pairs=(pair,),
            desired_cms_dataset=pair,
            expected_cms_incumbent=descriptor["expected_cms_incumbent"],
        )
    )


async def _state(database):
    """Compare complete physical dependency rows and parent proof bytes before and after reuse."""
    return [
        await database.scalar(
            f"SELECT jsonb_agg(to_jsonb(row_data) ORDER BY to_jsonb(row_data)::text) FROM mrf.{table} row_data"
        )
        for table in (
            "provider_directory_endpoint_dataset",
            "provider_directory_dataset_network_plan",
            "provider_directory_dataset_affiliation_organization",
            "provider_directory_dataset_insurance_plan",
        )
    ]


async def _prepare(database, result):
    """Use the production desired resolver and proof readers in a real read-only snapshot."""
    metrics_map = {}
    async with database.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        fence = await dependencies.prepare_desired_fence(
            fhir,
            _execution(result),
            run_id="serving-run",
            metrics=metrics_map,
            publish_targets={"dataset_network_plan", "dataset_affiliation_organization"},
        )
    assert len(fence.datasets) == 1
    return metrics_map


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_type", [None, "InsurancePlan", "OrganizationAffiliation"])
async def test_real_intake_relations_reuse_without_mutation(monkeypatch, cms_artifact_root, empty_type):
    """Complete zero-edge families and unresolved references retain their existing proof semantics."""
    directory, receipt = support.retained_release(cms_artifact_root, empty_resource_type=empty_type)
    async with support.admission_database(monkeypatch) as database:
        result = await _admit(directory, receipt, "owner")
        before = await _state(database)
        metrics = await _prepare(database, result)
        assert await _state(database) == before
        for key in ("dataset_network_plan", "dataset_affiliation_organization"):
            assert metrics[key]["complete"] is True and metrics[key]["dataset_count"] == 1
            assert metrics[key]["build_run_id"] == "serving-run"
            assert metrics[key]["datasets"][0]["build_run_id"] == "owner"
        if empty_type:
            key = "dataset_network_plan" if empty_type == "InsurancePlan" else "dataset_affiliation_organization"
            assert metrics[key]["edge_count"] == 0
        else:
            assert metrics["dataset_network_plan"]["datasets"][0]["unresolved_reference_count"] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["missing_network", "wrong_network", "wrong_affiliation", "plan_payload"])
async def test_dependency_rows_cannot_drift_behind_complete_metadata(monkeypatch, cms_artifact_root, corruption):
    """Deleting an edge or substituting same-count keys fails without modifying retained metadata."""
    directory, receipt = support.retained_release(cms_artifact_root)
    async with support.admission_database(monkeypatch) as database:
        result = await _admit(directory, receipt, "owner")
        metadata_before = (await _state(database))[0]
        statements_by_corruption_map = {
            "missing_network": "DELETE FROM mrf.provider_directory_dataset_network_plan WHERE network_resource_id='network-1'",
            "wrong_network": "UPDATE mrf.provider_directory_dataset_network_plan SET network_resource_id='changed' WHERE network_resource_id='network-1'",
            "wrong_affiliation": "UPDATE mrf.provider_directory_dataset_affiliation_organization SET participating_organization_resource_id='changed'",
            "plan_payload": "UPDATE mrf.provider_directory_dataset_insurance_plan SET payload_json=jsonb_set(payload_json::jsonb,'{name}','\"changed\"'::jsonb)::json",
        }
        await database.status(statements_by_corruption_map[corruption])
        corrupted = await _state(database)
        with pytest.raises(RuntimeError, match="(dependency_edges_changed|insurance_plan_incomplete)"):
            await _prepare(database, result)
        assert await _state(database) == corrupted and corrupted[0] == metadata_before
