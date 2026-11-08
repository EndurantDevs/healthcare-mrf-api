# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct fact anchors against genuine admitted-source and independent candidate custody."""

from dataclasses import asdict
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import registry_source_contribution_artifacts as artifacts
from process.registry_source_contribution_store import (
    RegistrySourceContributionError,
    _digest,
    create_contribution_stage,
    verify_registry_source_contributions,
)
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from tests.test_network_provider_only_read_postgres import _compose
from tests.test_network_provider_only_read_postgres import cms_recipe_database as cms_recipe_database
from tests.test_network_provider_only_read_postgres import cms_recipe_template as cms_recipe_template
from tests.test_network_provider_only_read_postgres import custom_db as custom_db
from tests.test_network_provider_only_read_postgres import provider_only_db as _provider_only_db
from tests.test_network_provider_only_read_postgres import provider_only_source as provider_only_source
from tests.test_network_provider_only_read_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


class RollbackProbe(RuntimeError):
    """Roll back exact synthetic fault injection after checking its refusal."""


@pytest.fixture
async def provider_only_db(_provider_only_db):
    """Publish a real successor after the first immutable fact-owning generation."""
    context = _provider_only_db
    context.original = context.manifest
    context.manifest = await _compose(
        context, revision=context.original.approved_custom_revision, source_manifest=context.original
    )
    yield context


async def _direct_anchor(context):
    fixture = context.fixture
    candidate_by_field = dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(context.manifest.candidate_id),
        )
    )
    assert verify_source_recipe_metadata(candidate_by_field).inventory is None
    contribution = verify_registry_source_contributions(candidate_by_field)
    assert contribution["revision"] == 2 and len(contribution["sources"]) == 1
    descriptor = contribution["sources"][0]
    reference = artifacts.source_fact_artifact_reference(descriptor)
    assert reference is not None and reference.fact_candidate_id == context.original.candidate_id
    identity = UUID(reference.fact_candidate_id)
    seal = await fixture.connection.fetchval(
        f'SELECT seal_sha256 FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1', identity
    )
    oid = await fixture.connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", context.original.schema_name + ".network_source_contribution"
    )
    assert reference == artifacts.SourceFactArtifactReference(str(identity), oid, seal)
    return descriptor, {**descriptor, **asdict(reference)}, reference


async def test_direct_original_anchor_survives_mapping_receipts_without_fact_copies(provider_only_db):
    context, fixture = provider_only_db, provider_only_db.fixture
    descriptor, selected_by_field, reference = await _direct_anchor(context)
    temporary = "source_artifact_empty_" + uuid4().hex
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await create_contribution_stage(fixture.connection, temporary)
        try:
            for mapping in ("1" * 64, "2" * 64, "3" * 64):
                selected_by_field = {
                    **selected_by_field,
                    "mapping_sha256": mapping,
                    "source_generation": _digest([descriptor["capture_sha256"], mapping]),
                }
                search_path = await fixture.connection.fetchval("SHOW search_path")
                statements = []
                fixture.connection.add_query_logger(statements.append)
                try:
                    resolved = await artifacts.resolve_anchored_source_facts(
                        fixture.connection, (selected_by_field,), control_schema=fixture.control_schema
                    )
                finally:
                    fixture.connection.remove_query_logger(statements.append)
                artifact = resolved[descriptor["recipe_sha256"]]
                assert artifact.reference == reference
                assert artifact.relation == f'"{context.original.schema_name}".network_source_contribution'
                selected_by_field = artifacts.anchored_source_fact_descriptor(selected_by_field, artifact)
                assert selected_by_field["fact_candidate_id"] == context.original.candidate_id
                assert await fixture.connection.fetchval("SHOW search_path") == search_path
                assert all(
                    query.query == "SET LOCAL search_path TO pg_catalog,public"
                    or query.query.lstrip().split(None, 1)[0].upper() in {"SELECT", "WITH", "SHOW", "LOCK"}
                    for query in statements
                )
                assert await fixture.connection.fetchval(f'SELECT count(*) FROM pg_temp."{temporary}"') == 0
            await fixture.connection.execute(
                f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE candidate_id=$1',
                UUID(context.original.candidate_id),
            )
            assert await artifacts.resolve_anchored_source_facts(
                fixture.connection, (selected_by_field,), control_schema=fixture.control_schema
            )
        finally:
            await fixture.connection.execute(f'DROP TABLE pg_temp."{temporary}"')


async def test_anchor_rejects_old_seal_wrong_oid_same_count_tamper_and_restores(provider_only_db):
    context, fixture = provider_only_db, provider_only_db.fixture
    descriptor, selected_by_field, reference = await _direct_anchor(context)
    relation = f'"{context.original.schema_name}".network_source_contribution'
    async with fixture.connection.transaction(isolation="repeatable_read"):
        for field, changed in (("fact_seal_sha256", "f" * 64), ("fact_table_oid", reference.fact_table_oid + 1)):
            with pytest.raises(RegistrySourceContributionError, match="physical_identity_changed"):
                await artifacts.resolve_anchored_source_facts(
                    fixture.connection, ({**selected_by_field, field: changed},), control_schema=fixture.control_schema
                )
        before = await fixture.connection.fetchval(f"SELECT count(*) FROM {relation}")
        with pytest.raises(RollbackProbe):
            async with fixture.connection.transaction():
                await fixture.connection.execute(
                    f"UPDATE {relation} SET fact_json=jsonb_set(fact_json,'{{provider_id}}','\"1999999999\"') "
                    "WHERE fact_json->>'omission_reason' IS NULL"
                )
                assert await fixture.connection.fetchval(f"SELECT count(*) FROM {relation}") == before
                with pytest.raises(RegistrySourceContributionError, match="content_changed"):
                    await artifacts.resolve_anchored_source_facts(
                        fixture.connection, (selected_by_field,), control_schema=fixture.control_schema
                    )
                raise RollbackProbe
        assert (
            await artifacts.resolve_anchored_source_facts(
                fixture.connection, (selected_by_field,), control_schema=fixture.control_schema
            )
        )[descriptor["recipe_sha256"]].reference == reference
        async with fixture.connection.transaction():
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with fixture.connection.transaction():
                    await fixture.connection.execute(f"UPDATE {relation} SET fact_json=fact_json")
