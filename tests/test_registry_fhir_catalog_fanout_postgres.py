# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Large valid plan pages cross native observation and bounded COPY staging."""

import hashlib
import json
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_sync as sync
from process.registry_fhir_catalog_extraction import DEFAULT_BYTES, MAX_EDGES, MAX_ROWS
from process.registry_fhir_catalog_observed_source import collect_fhir_catalog_observation
from process.registry_fhir_catalog_reference_rollup import fhir_catalog_reference_workspace
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from tests.test_registry_fhir_catalog_observation_sync_postgres import (
    metadata_custody as metadata_custody,
    observation_case as observation_case,
    observed_source_db as observed_source_db,
)

pytestmark = pytest.mark.asyncio


async def _replace_plan_resources(case, plan_count, reference_count):
    network_refs = [f"Organization/network-{index:05d}" for index in range(reference_count)]
    definitions = [
        ("Organization", f"network-{index:05d}", {"name": f"Network {index}", "type_codes": ["ntwk"]})
        for index in range(11)
    ]
    definitions.extend(
        ("InsurancePlan", f"plan-{index:04d}", {"name": f"Choice {index}", "network_refs": network_refs})
        for index in range(plan_count)
    )
    source_records = []
    for kind, identifier, document in definitions:
        encoded = json.dumps(document, sort_keys=True, separators=(",", ":"))
        source_records.append(("dataset-one", kind, identifier, hashlib.sha256(encoded.encode()).hexdigest(), encoded))
    relation = f'"{case.source.schema}".provider_directory_dataset_resource'
    await case.source.admin.execute(f"DELETE FROM {relation} WHERE dataset_id='dataset-one'")
    await case.source.admin.copy_records_to_table(
        "provider_directory_dataset_resource", schema_name=case.source.schema, records=source_records
    )
    await case.source.admin.execute(
        f'UPDATE "{case.source.schema}".provider_directory_endpoint_dataset SET resource_count=$1 '
        "WHERE dataset_id='dataset-one'",
        len(source_records),
    )


async def _observe(case):
    async with (
        case.source_pool.acquire() as reader,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        await reader.execute("SET LOCAL search_path=pg_catalog,pg_temp,public")
        return await collect_fhir_catalog_observation(
            reader, case.source.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )


async def test_large_plan_page_observes_and_stages_every_edge_in_order(observation_case, monkeypatch):
    case = observation_case
    await _replace_plan_resources(case, 1000, 11)
    admitted = await _observe(case)
    assert admitted.status == "accepted"
    families = admitted.descriptor["admission"]["families"]
    assert families["InsurancePlan"]["rows"] == 1000 and families["Organization"]["rows"] == 11
    wire_keys = []
    batch_sizes = []
    write_rows = sync.write_fhir_catalog_rows

    async def capture_write(loader, target, table, observation_rows, **options):
        if table == "registry_fhir_catalog_observation":
            assert len(observation_rows) <= MAX_EDGES + MAX_ROWS
            assert len(json.dumps(observation_rows, separators=(",", ":")).encode()) <= DEFAULT_BYTES
            batch_sizes.append(len(observation_rows))
            wire_keys.extend(tuple(observation[1:5]) for observation in observation_rows)
        return await write_rows(loader, target, table, observation_rows, **options)

    monkeypatch.setattr(sync, "write_fhir_catalog_rows", capture_write)
    async with (
        case.source_pool.acquire() as reader,
        fhir_catalog_reference_workspace(reader) as workspace,
        reader.transaction(isolation="repeatable_read", readonly=True),
        case.fixture.loader.transaction(),
    ):
        await reader.execute("SET LOCAL search_path=pg_catalog,pg_temp,public")
        accounting = await sync.stage_fhir_catalog_source(
            reader,
            case.fixture.loader,
            case.fixture.target,
            admitted,
            loader_role=case.fixture.roles["loader"],
            control_schema=case.fixture.control_schema,
            reference_workspace=workspace,
        )
    expected_keys = [
        ("InsurancePlan", f"plan-{plan:04d}", "edge", ordinal) for plan in range(1000) for ordinal in range(11)
    ] + [("Organization", f"network-{index:05d}", "organization", 0) for index in range(11)]
    assert wire_keys == expected_keys and len(batch_sizes) >= 3
    assert accounting["source_rows"] == 1011 and accounting["observation_rows"] == 11011
    staged = await case.fixture.loader.fetch(
        f"SELECT resource_type,resource_id,observation_kind,ordinal,document_json::text AS document "
        f'FROM "{case.fixture.target.schema_name}".registry_fhir_catalog_observation '
        'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",observation_kind COLLATE "C",ordinal'
    )
    assert [
        tuple(entry[key] for key in ("resource_type", "resource_id", "observation_kind", "ordinal")) for entry in staged
    ] == expected_keys
    assert all(
        json.loads(entry["document"])["network_ref"] == f"Organization/network-{entry['ordinal']:05d}"
        for entry in staged
        if entry["resource_type"] == "InsurancePlan"
    )


async def test_single_resource_overflow_refuses_observation_and_staging(observation_case):
    case = observation_case
    await _replace_plan_resources(case, 1, 11)
    admitted = await _observe(case)
    await _replace_plan_resources(case, 1, MAX_EDGES + 1)
    with pytest.raises(ValueError, match="output_bounds"):
        await _observe(case)
    emitted = AsyncMock()
    async with (
        case.source_pool.acquire() as reader,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        await reader.execute("SET LOCAL search_path=pg_catalog,pg_temp,public")
        with pytest.raises(ValueError, match="output_bounds"):
            await sync._stage_plan_observations(
                reader,
                case.source.schema,
                "dataset-one",
                admitted.descriptor,
                None,
                emitted,
                fhir_catalog_source_generation(admitted.descriptor),
            )
    emitted.assert_not_awaited()
    assert (
        await case.fixture.loader.fetchval(
            f'SELECT count(*) FROM "{case.fixture.target.schema_name}".registry_fhir_catalog_observation'
        )
        == 0
    )
