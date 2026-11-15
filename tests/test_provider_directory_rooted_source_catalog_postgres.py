# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native named catalog reads preserve the current publication across refused direct writes."""

from contextlib import asynccontextmanager

import asyncpg
import pytest

from process.provider_directory_rooted_graph_publication import publish_provider_directory_rooted_graph_dataset
from process.provider_directory_rooted_graph_single_root_contract import derive_single_root_identity
from process.provider_directory_rooted_graph_twin_store import admit_rooted_graph_single_root
from process.provider_directory_rooted_graph_worker import rooted_graph_acquisition_descriptor
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch, iter_fhir_catalog_pages
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile, iter_fhir_catalog_sources
from tests.provider_directory_rooted_graph_rotation_pg_support import locked_exact_current, publish_legacy_root
from tests.test_provider_directory_rooted_graph_acquisition_postgres import _complete_success
from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope


@pytest.mark.asyncio
async def test_native_catalog_descriptor_reads_exact_root_without_write_locks(monkeypatch):
    async with _lifecycle_scope(monkeypatch) as context:
        current = await publish_legacy_root(context.database)
        descriptor = await rooted_graph_acquisition_descriptor(context.database)
        assert descriptor["root_dataset_id"] == current.dataset_id
        assert descriptor["root_dataset_hash"] == current.dataset_hash
        assert descriptor["root_content_proof_sha256"] == current.root_content_proof_sha256
        assert descriptor["root_cohort_id"] == current.root_cohort_id
        assert context.database._transaction_binding() is None


async def _refuse_single_rooted_edition(context, current):
    identity = derive_single_root_identity(current, operation_key="7" * 64)
    await _complete_success(context.database, identity.candidate)
    await admit_rooted_graph_single_root(
        identity.candidate.acquisition_id, acquisition_operation_key=identity.operation_key, database=context.database
    )
    with pytest.raises(RuntimeError, match="prepared named generation through the Source operator"):
        await publish_provider_directory_rooted_graph_dataset(
            identity.candidate.acquisition_id, database=context.database
        )
    assert await locked_exact_current(context.database) == current


@asynccontextmanager
async def _registered_catalog_reader(context):
    component = context.component
    for table, columns in (
        ("provider_directory_source", "tableoid,source_id,endpoint_id"),
        ("provider_directory_api_endpoint", "tableoid,endpoint_id,canonical_api_base"),
    ):
        await context.connection.execute(f'GRANT SELECT({columns}) ON mrf.{table} TO "{component.reader}"')
    url = context.url.set(drivername="postgresql", username=component.reader)
    reader = await asyncpg.connect(url.render_as_string(hide_password=False))
    try:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await reader.execute("SET LOCAL search_path=pg_catalog,mrf")
            yield reader
    finally:
        await reader.close()


async def _read_current_catalog(context, current):
    """Read the full published resource family through the granted named catalog path."""
    profile = FHIRCatalogSourceReadProfile("mrf")
    async with _registered_catalog_reader(context) as reader:
        dispositions = [
            disposition
            async for page in iter_fhir_catalog_sources(
                reader, profile, observation_contract=CATALOG_OBSERVATION_CONTRACT
            )
            for disposition in page
            if disposition.dataset_id == current.dataset_id
        ]
        assert len(dispositions) == 1
        accepted = dispositions[0]
        assert accepted.status == "accepted" and accepted.reason is None
        descriptor = accepted.descriptor
        assert descriptor["coordinates"]["dataset_id"] == current.dataset_id
        assert descriptor["dataset_sha256"] == current.dataset_hash
        assert descriptor["source_aliases"] == [current.source_id]
        assert descriptor["admission"]["kind"] == "catalog_observation"
        pages = [
            page async for page in iter_fhir_catalog_pages(reader, schema_name="mrf", dataset_id=current.dataset_id)
        ]
        batches = [
            extract_fhir_catalog_batch(page, profile="published_normalized", source_uri=descriptor["source_uri"])
            for page in pages
        ]
        resources = await reader.fetch(
            "SELECT resource_type,resource_id,to_jsonb(resource) AS complete_row "
            "FROM mrf.provider_directory_dataset_resource resource WHERE dataset_id=$1 "
            "ORDER BY resource_type,resource_id",
            current.dataset_id,
        )
        assert len(resources) == current.resource_count
        assert {resource["resource_type"] for resource in resources} == {"Practitioner"}
        # This predecessor has no organization/plan resources for membership extraction.
        assert sum(batch.source_rows for batch in batches) == 0
        return descriptor, resources, pages, batches


@pytest.mark.asyncio
async def test_refused_direct_rooted_publication_preserves_granted_catalog_data(monkeypatch):
    """The supported predecessor remains fully readable after an unsupported direct rooted attempt."""
    async with _lifecycle_scope(monkeypatch) as context:
        current = await publish_legacy_root(context.database)
        before = await _read_current_catalog(context, current)
        await _refuse_single_rooted_edition(context, current)
        # A fresh read transaction must observe the same descriptor and every original row.
        assert await _read_current_catalog(context, current) == before
