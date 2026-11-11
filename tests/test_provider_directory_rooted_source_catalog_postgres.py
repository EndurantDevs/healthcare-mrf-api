# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The native acquisition descriptor reads current root custody in READ ONLY."""

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
from tests.provider_directory_rooted_graph_rotation_pg_support import publish_legacy_root
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


async def _publish_single_rooted_edition(context):
    current = await publish_legacy_root(context.database)
    identity = derive_single_root_identity(current, operation_key="7" * 64)
    await _complete_success(context.database, identity.candidate)
    await admit_rooted_graph_single_root(
        identity.candidate.acquisition_id, acquisition_operation_key=identity.operation_key, database=context.database
    )
    published = await publish_provider_directory_rooted_graph_dataset(
        identity.candidate.acquisition_id, database=context.database
    )
    assert published.replayed is False
    return published.readiness


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


@pytest.mark.asyncio
async def test_single_rooted_publication_enters_granted_catalog_observation_and_extraction(monkeypatch):
    async with _lifecycle_scope(monkeypatch) as context:
        published = await _publish_single_rooted_edition(context)
        profile = FHIRCatalogSourceReadProfile(
            "mrf", context.component.owner, tuple(sorted(context.component.policy.runtime_roles))
        )
        async with _registered_catalog_reader(context) as reader:
            dispositions = [
                disposition
                async for page in iter_fhir_catalog_sources(
                    reader, profile, observation_contract=CATALOG_OBSERVATION_CONTRACT
                )
                for disposition in page
                if disposition.dataset_id == published.dataset_id
            ]
            assert len(dispositions) == 1
            accepted = dispositions[0]
            assert accepted.status == "accepted" and accepted.reason is None
            descriptor = accepted.descriptor
            assert descriptor["coordinates"]["dataset_id"] == published.dataset_id
            assert descriptor["dataset_sha256"] == published.dataset_hash
            assert descriptor["source_aliases"] == [published.source_id]
            assert descriptor["admission"]["kind"] == "catalog_observation"
            batches = [
                extract_fhir_catalog_batch(page, profile="published_normalized", source_uri=descriptor["source_uri"])
                async for page in iter_fhir_catalog_pages(reader, schema_name="mrf", dataset_id=published.dataset_id)
            ]
            assert sum(batch.source_rows for batch in batches) == 3
            assert {organization.resource_id for batch in batches for organization in batch.organizations} == {
                "org.synthetic-1"
            }
            assert {edge.network_resource_id for batch in batches for edge in batch.edges} == {"org.synthetic-1"}
