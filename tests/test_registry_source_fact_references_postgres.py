# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connected source fact references, historical reads and exact retention boundaries."""

import json
from uuid import UUID, uuid4

import pytest

from process import network_compact_retention as retention
from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_recipe_composition as composition
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.registry_source_contribution_store import (
    registry_source_contribution_receipt,
    verify_registry_source_contributions,
)
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_recipe_composition import SourceRecipeReadContext
from tests.test_network_provider_only_read_postgres import _compose, _read, _review_source_operation
from tests.test_network_provider_only_read_postgres import cms_recipe_database as cms_recipe_database
from tests.test_network_provider_only_read_postgres import cms_recipe_template as cms_recipe_template
from tests.test_network_provider_only_read_postgres import custom_db as custom_db
from tests.test_network_provider_only_read_postgres import provider_only_db as _provider_only_db
from tests.test_network_provider_only_read_postgres import provider_only_source as provider_only_source
from tests.test_network_provider_only_read_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_source_contribution_artifacts_postgres import RollbackProbe
from tests.test_registry_source_contribution_artifacts_postgres import provider_only_db as provider_only_db

pytestmark = pytest.mark.asyncio


async def _candidate(context, manifest):
    return dict(
        await context.fixture.connection.fetchrow(
            f'SELECT * FROM "{context.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(manifest.candidate_id),
        )
    )


async def _assert_reference(context, current, owner):
    candidate = await _candidate(context, current)
    assert verify_source_recipe_metadata(candidate).inventory is None
    contribution = verify_registry_source_contributions(candidate)
    assert contribution["revision"] == 2 and len(contribution["sources"]) == 1
    descriptor = contribution["sources"][0]
    assert descriptor["fact_candidate_id"] == owner.candidate_id
    assert (
        await context.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{current.schema_name}".network_source_contribution'
        )
        == 0
    )
    assert (
        await context.fixture.connection.fetchval(
            "SELECT bool_and(indisvalid AND indisready) FROM pg_index WHERE indexrelid=to_regclass($1)",
            current.schema_name + ".source_provider_only_idx",
        )
        is True
    )
    return descriptor


async def _no_dml_replay(context, manifest):
    candidate = await _candidate(context, manifest)
    report = json.loads(candidate["validation_json"])
    source = PinnedAddressSource(**report["address_source"])
    statements = []
    context.fixture.connection.add_query_logger(statements.append)
    try:
        replayed = await prepare_and_publish_network_candidate(
            context.fixture.connection,
            context.copy_targets[-1],
            source,
            **context.fixture.writer_roles,
            control_schema=context.fixture.control_schema,
            seal_candidate=context.fixture.seal_candidate,
        )
    finally:
        context.fixture.connection.remove_query_logger(statements.append)
    assert replayed["generation_id"] == manifest.generation_id
    assert not any(
        query.query.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "COPY"))
        for query in statements
    )


def _record_retained_custody_checks(context, monkeypatch):
    """Observe genuine retained custody without permitting raw extraction or fact copies."""
    require = composition._require_recipe_custody
    checked_revisions = []

    async def checked(connection, recipe, approved, control_schema, *arguments, **options):
        assert recipe.source_pin == context.source[0][2]
        assert control_schema == context.fixture.control_schema
        result = await require(connection, recipe, approved, control_schema, *arguments, **options)
        checked_revisions.append(approved.approved_revision)
        return result

    monkeypatch.setattr(composition, "_require_recipe_custody", checked)
    return checked_revisions


async def _compose_with_checked_custody(context, revision, source_manifest, checked_revisions):
    before = len(checked_revisions)
    current = await _compose(context, revision=revision, source_manifest=source_manifest)
    assert checked_revisions[before:] and set(checked_revisions[before:]) == {revision}
    return current


async def test_three_generations_keep_original_artifact_and_historical_mapping(provider_only_db, monkeypatch):
    context, connection = provider_only_db, provider_only_db.fixture.connection
    owner, previous = context.original, context.manifest
    before = await _read(context)

    async def forbidden(*args, **kwargs):
        pytest.fail("unchanged source payload extraction or fact copying was attempted")

    monkeypatch.setattr(composition, "_read_recipe_page", forbidden)
    monkeypatch.setattr(composition, "reuse_fhir_facts", forbidden)
    checked_revisions = _record_retained_custody_checks(context, monkeypatch)
    second = await _compose_with_checked_custody(
        context, previous.approved_custom_revision, previous, checked_revisions
    )
    await _assert_reference(context, second, owner)
    _, actor, networks, binding, _, _ = context.source
    changed = await _review_source_operation(
        connection, context.fixture.control_schema, actor, binding, "rebind", networks
    )
    approved = await _approve(
        connection,
        context.fixture.control_schema,
        await _command(connection, context.fixture.control_schema, changed),
        actor,
    )
    third = await _compose_with_checked_custody(context, approved["approved_revision"], second, checked_revisions)
    await _assert_reference(context, third, owner)
    assert await _read(context) == before
    assert (await _read(context, manifest=third, network_ids=(context.other_network_id,)))["total_count"] == 2
    await connection.execute(
        f'UPDATE "{context.fixture.control_schema}".network_serving_manifest SET eligible=false WHERE candidate_id=$1',
        UUID(owner.candidate_id),
    )
    fourth = await _compose_with_checked_custody(context, approved["approved_revision"], third, checked_revisions)
    await _assert_reference(context, fourth, owner)
    with monkeypatch.context() as no_payload_hash:
        no_payload_hash.setattr(artifacts, "contribution_fact_sha256", forbidden)
        assert (await _read(context, manifest=fourth, network_ids=(context.other_network_id,)))["total_count"] == 2
    await _no_dml_replay(context, fourth)


async def test_removed_source_stage_has_no_fact_access_or_copy(provider_only_db, monkeypatch):
    context, fixture = provider_only_db, provider_only_db.fixture

    async def forbidden(*args, **kwargs):
        pytest.fail("removed source facts were accessed")

    monkeypatch.setattr(artifacts, "verify_anchored_source_fact_contents", forbidden)
    monkeypatch.setattr(composition, "_read_recipe_page", forbidden)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            fixture.connection,
            approved_revision=context.manifest.approved_custom_revision,
            control_schema=fixture.control_schema,
        )
        stage = await composition.stage_registry_source_recipes(
            fixture.connection,
            (),
            approved,
            request_id=uuid4(),
            control_schema=fixture.control_schema,
            read_context=SourceRecipeReadContext(source_manifest=context.manifest),
        )
        assert stage.membership_rows == stage.own_fact_rows == stage.reused_fact_rows == 0
        assert stage.contribution_table_name is None and registry_source_contribution_receipt(stage) is None
        await fixture.connection.execute(f'DROP TABLE pg_temp."{stage.table_name}"')


async def _retention_probe(context, oids):
    async with context.fixture.connection.transaction():
        await retention.require_network_compact_artifacts_unreferenced(
            context.fixture.connection, oids, control_schema=context.fixture.control_schema
        )


async def test_retention_protects_references_and_fails_closed_on_missing_anchor(provider_only_db):
    context, fixture = provider_only_db, provider_only_db.fixture
    owner, previous = context.original, context.manifest
    current = await _compose(context, revision=previous.approved_custom_revision, source_manifest=previous)
    descriptor = await _assert_reference(context, current, owner)
    own_oid = await fixture.connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", current.schema_name + ".network_source_contribution"
    )
    for oid in (own_oid, descriptor["fact_table_oid"]):
        with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
            await _retention_probe(context, [oid])
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE candidate_id=$1',
        UUID(owner.candidate_id),
    )
    with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
        await _retention_probe(context, [descriptor["fact_table_oid"]])
    unrelated = "source_unreferenced_" + uuid4().hex
    await fixture.connection.execute(f'CREATE TEMP TABLE "{unrelated}"(id integer)')
    try:
        oid = await fixture.connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + unrelated)
        await _retention_probe(context, [oid])
        with pytest.raises(RollbackProbe):
            async with fixture.connection.transaction():
                await fixture.connection.execute(
                    f'DELETE FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
                    UUID(owner.candidate_id),
                )
                with pytest.raises(retention.NetworkCompactRetentionError):
                    await retention.require_network_compact_artifacts_unreferenced(
                        fixture.connection, [oid], control_schema=fixture.control_schema
                    )
                raise RollbackProbe
        await _retention_probe(context, [oid])
        with pytest.raises(RollbackProbe):
            async with fixture.connection.transaction():
                retained_name = "fact_oid_probe_" + uuid4().hex
                await fixture.connection.execute(
                    f'ALTER TABLE "{owner.schema_name}".network_source_contribution RENAME TO "{retained_name}"'
                )
                await fixture.connection.execute(
                    f'CREATE TABLE "{owner.schema_name}".network_source_contribution '
                    f'(LIKE "{owner.schema_name}"."{retained_name}" INCLUDING ALL)'
                )
                with pytest.raises(retention.NetworkCompactRetentionError, match="source_anchor_missing"):
                    await retention.require_network_compact_artifacts_unreferenced(
                        fixture.connection, [oid], control_schema=fixture.control_schema
                    )
                raise RollbackProbe
        await _retention_probe(context, [oid])
    finally:
        await fixture.connection.execute(f'DROP TABLE pg_temp."{unrelated}"')
