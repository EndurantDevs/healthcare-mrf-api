# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native source-bound entity reads with real seals and immutable identity storage."""

import asyncio
import json
import os
from dataclasses import asdict, replace
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api import provider_directory_cms_network_scope as scoped
from api.provider_directory_cms_entities import read_cms_entities
from api.provider_directory_entities_contract import CURSOR_KEY_ENV, DirectoryRead, DirectoryReadError
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_serving_read import PinnedNetworkServingManifest
from process.provider_directory_resource_identity import bind_resource_identity_batch, source_resource_uuid
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.provider_directory_cms_postgres_support import (
    NETWORK_ID,
    ORG_ID,
    SITE_ID,
    cms_database,
    publication_summary,
    reseal,
    seed_resource_identities,
)
from tests.provider_directory_entities_postgres_support import _database_url


@pytest.mark.parametrize("port", (5432, 5440))
def test_entities_fixture_accepts_explicit_local_ports(monkeypatch, port):
    monkeypatch.setenv(
        "HLTHPRT_DIRECTORY_ENTITIES_TEST_DSN",
        f"postgresql+asyncpg://test_role@localhost:{port}/hc_directory_entities_" + "1" * 32,
    )
    assert _database_url().port == port


@pytest.mark.parametrize(
    "authority, database",
    [
        ("localhost", "hc_directory_entities_" + "1" * 32),
        ("localhost:0", "hc_directory_entities_" + "1" * 32),
        ("localhost:65536", "hc_directory_entities_" + "1" * 32),
        ("example.invalid:5440", "hc_directory_entities_" + "1" * 32),
        ("localhost:5440", "shared_database"),
    ],
)
def test_entities_fixture_rejects_non_disposable_connections(monkeypatch, authority, database):
    monkeypatch.setenv("HLTHPRT_DIRECTORY_ENTITIES_TEST_DSN", f"postgresql+asyncpg://test_role@{authority}/{database}")
    with pytest.raises(pytest.fail.Exception, match="UUID-owned local PostgreSQL test database"):
        _database_url()


def cms_query(kind="organizations", shape="entities", entity_id=None, limit=1):
    return DirectoryRead(kind, "cms-npd", shape, entity_id, None, limit)


async def read(sessions, query):
    async with sessions() as session:
        payload = await read_cms_entities(session, query)
        assert not session.in_transaction()
        return payload


async def change(sessions, statement):
    async with sessions() as session, session.begin():
        await session.execute(text(statement))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,entity_id",
    [
        ("organizations", ORG_ID),
        ("sites", SITE_ID),
        ("networks", NETWORK_ID),
        ("plans", str(source_resource_uuid("cms-npd", "InsurancePlan", "plan-example"))),
        ("practitioner-roles", str(source_resource_uuid("cms-npd", "PractitionerRole", "role-example"))),
    ],
)
async def test_all_kinds_list_detail_and_only_explicit_relationships(monkeypatch, kind, entity_id):
    """Each supported kind uses the accepted release, with no source or tax-ID disclosure."""
    async with cms_database(monkeypatch) as sessions:
        page = await read(sessions, cms_query(kind))
        assert len(page["items"]) == 1 and page["next_cursor"] is None
        entity = page["items"][0]
        assert entity["id"] == entity_id and entity["kind"] == kind
        detail = await read(sessions, cms_query(kind, "entity", entity_id))
        assert detail["item"] == entity and detail["generation_id"] == page["generation_id"]
        relationships = await read(sessions, cms_query(kind, "relationships", entity_id, 100))
        assert relationships["items"]
        assert all(item["source_id"] == "cms-npd" for item in relationships["items"])
        serialized = json.dumps([page, detail, relationships])
        for excluded in (
            "synthetic-tax-value",
            "org-example",
            "plan-example",
            "role-example",
            "example.invalid",
            "1234567890",
        ):
            assert excluded not in serialized
        if kind == "networks":
            assert {entry["resource_type"] for entry in entity["evidence"]} == {"Organization", "InsurancePlan"}
        if kind == "plans":
            assert entity["effective_start"] == "2026-01-01"
            assert {item["status"] for item in relationships["items"]} == {"resolved", "unresolved"}
            assert all(item["target_kind"] != "payers" for item in relationships["items"])


@pytest.mark.asyncio
async def test_semantic_dataset_without_raw_hash_keeps_exact_release_links(monkeypatch):
    """Semantic retained rows have no raw SHA; the sealed release still scopes their links."""

    async def prepare(session):
        await session.execute(text("UPDATE provider_directory_dataset_resource SET acquired_resource_sha256=NULL"))

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        for kind in ("organizations", "sites", "networks", "plans", "practitioner-roles"):
            assert len((await read(sessions, cms_query(kind)))["items"]) == 1


@pytest.mark.asyncio
async def test_candidate_only_alias_does_not_hide_current_entity(monkeypatch):
    async def prepare(session):
        await session.execute(
            text(
                "UPDATE provider_directory_entity_source_binding SET organization_id=:organization_id "
                "WHERE resource_id='candidate-only'"
            ),
            {"organization_id": ORG_ID},
        )
        await session.execute(
            text(
                "INSERT INTO provider_directory_insurance_network_source_binding VALUES "
                "('cms-npd', 'Organization', 'candidate-only', :network_id)"
            ),
            {"network_id": NETWORK_ID},
        )

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        for kind in ("organizations", "networks"):
            assert len((await read(sessions, cms_query(kind)))["items"]) == 1


@pytest.mark.asyncio
async def test_reviewed_old_id_returns_canonical_redirect_and_invalidates_generation(monkeypatch):
    old_id = "00000000-0000-0000-0000-000000000099"
    async with cms_database(monkeypatch) as sessions:
        before = await read(sessions, cms_query())
        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO provider_directory_entity_redirect_decision VALUES "
                    "('00000000-0000-0000-0000-000000000077', 'cms-npd')"
                )
            )
            await session.execute(
                text(
                    "INSERT INTO provider_directory_entity_redirect VALUES "
                    "('cms-npd', 'Organization', :old_id, :canonical_id)"
                ),
                {"old_id": old_id, "canonical_id": ORG_ID},
            )
        stale = replace(cms_query("organizations", "entity", old_id), generation_id=before["generation_id"])
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, stale)
        assert caught.value.status == 409
        detail = await read(sessions, cms_query("organizations", "entity", old_id))
        relations = await read(sessions, cms_query("organizations", "relationships", old_id))
        assert (
            detail
            == relations
            == {
                "generation_id": detail["generation_id"],
                "redirect": {
                    "source_id": "cms-npd",
                    "kind": "organizations",
                    "requested_id": old_id,
                    "canonical_id": ORG_ID,
                },
            }
        )
        assert detail["generation_id"] != before["generation_id"]
        invalid_cursor = replace(
            cms_query("organizations", "relationships", old_id),
            generation_id=detail["generation_id"],
            cursor="invalid",
        )
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, invalid_cursor)
        assert caught.value.status == 409
        assert (await read(sessions, cms_query("organizations", "entity", ORG_ID)))["item"]["id"] == ORG_ID


@pytest.mark.asyncio
async def test_reviewed_redirect_does_not_resolve_to_unpublished_target(monkeypatch):
    unpublished_id = "00000000-0000-0000-0000-000000000099"
    async with cms_database(monkeypatch) as sessions:
        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO provider_directory_entity_redirect_decision VALUES "
                    "('00000000-0000-0000-0000-000000000078', 'cms-npd')"
                )
            )
            await session.execute(
                text(
                    "INSERT INTO provider_directory_entity_redirect VALUES "
                    "('cms-npd', 'Organization', :old_id, :canonical_id)"
                ),
                {"old_id": ORG_ID, "canonical_id": unpublished_id},
            )
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query("organizations", "entity", ORG_ID))
        assert caught.value.status == 503


@pytest.mark.asyncio
async def test_two_accepted_aliases_cannot_seal_one_entity(monkeypatch):
    from process.provider_directory_cms_serving_coverage import build_cms_coverage

    async def prepare(session):
        summary = publication_summary()
        summary["source_release"]["files"]["01-Organization.ndjson"].update(row_count=2, distinct_count=2)
        proof = summary["provider_directory_content_proof_admission_summary_v1"]
        proof["resource_counts"]["Organization"] = 2
        proof["resource_count"] = 9
        await session.execute(
            text(
                "INSERT INTO provider_directory_dataset_resource "
                "SELECT dataset_id, resource_type, 'accepted-alias', payload_hash, payload_json, "
                "acquired_resource_sha256 FROM provider_directory_dataset_resource "
                "WHERE resource_type='Organization' LIMIT 1"
            )
        )
        await session.execute(
            text(
                "INSERT INTO provider_directory_entity_source_binding VALUES "
                "('cms-npd', 'Organization', 'accepted-alias', :organization_id, NULL)"
            ),
            {"organization_id": ORG_ID},
        )
        await session.execute(
            text(
                "INSERT INTO provider_directory_entity_release_evidence "
                "(source_id, resource_type, resource_id, release_id, payload_sha256) VALUES "
                "('cms-npd', 'Organization', 'accepted-alias', repeat('a',64), repeat('b',64))"
            )
        )
        await session.execute(
            text(
                "UPDATE provider_directory_endpoint_dataset SET resource_count=9, "
                "publication_metadata_summary_json=CAST(:summary AS jsonb)"
            ),
            {"summary": json.dumps(summary)},
        )
        await reseal(session)

    async with cms_database(monkeypatch, prepare=prepare, seal=False) as sessions:
        async with sessions() as session:
            with pytest.raises(DirectoryReadError) as caught:
                await build_cms_coverage(session)
            assert caught.value.status == 503


@pytest.mark.asyncio
async def test_relationship_cursors_cover_explicit_references_without_duplicates(monkeypatch):
    """Encrypted seek cursors stay generation-bound and preserve unresolved targets."""
    async with cms_database(monkeypatch) as sessions:
        plan_id = str(source_resource_uuid("cms-npd", "InsurancePlan", "plan-example"))
        query = cms_query("plans", "relationships", plan_id)
        items = []
        for _ in range(4):
            page = await read(sessions, query)
            items.extend(page["items"])
            query = replace(query, generation_id=page["generation_id"], cursor=page["next_cursor"])
        assert query.cursor is None
        assert len(items) == len({item["relationship_key"] for item in items}) == 4
        assert sum(item["status"] == "unresolved" for item in items) == 2
        assert any(item["relationship_type"] == "plan-owned-by" and item["target_id"] == ORG_ID for item in items)
        assert any(item["relationship_type"] == "plan-network" and item["target_id"] == NETWORK_ID for item in items)


@pytest.mark.asyncio
async def test_relationship_ledger_preserves_ambiguity_nested_period_and_source_scope(monkeypatch):
    """A later binding or other source cannot turn a retained conflict into a resolved link."""

    async with cms_database(monkeypatch) as sessions:
        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE provider_directory_cms_npd_relationship SET resolution_status='ambiguous' "
                    "WHERE resource_type='InsurancePlan' AND reference_field='network' "
                    "AND target_reference='Organization/org-example'"
                )
            )
            await session.execute(
                text(
                    "INSERT INTO provider_directory_cms_npd_relationship VALUES "
                    "('synthetic-dataset','cms-npd',repeat('a',64),'InsurancePlan','plan-example',"
                    "repeat('b',64),repeat('b',64),'plan.network',1,1,'Organization/org-example',"
                    "'resolved','2025-03-01',NULL)"
                )
            )
            await session.execute(
                text(
                    "INSERT INTO provider_directory_entity_source_binding VALUES "
                    "('other-source','Organization','missing',:organization_id,NULL)"
                ),
                {"organization_id": ORG_ID},
            )
            await session.execute(
                text(
                    "INSERT INTO provider_directory_cms_npd_relationship VALUES "
                    "('synthetic-dataset','other-source',repeat('a',64),'InsurancePlan','plan-example',"
                    "repeat('b',64),repeat('b',64),'plan.network',2,1,'Organization/org-example','resolved',NULL,NULL),"
                    "('synthetic-dataset','cms-npd',repeat('c',64),'InsurancePlan','plan-example',"
                    "repeat('b',64),repeat('b',64),'plan.network',3,1,'Organization/org-example','resolved',NULL,NULL)"
                )
            )
        plan_id = str(source_resource_uuid("cms-npd", "InsurancePlan", "plan-example"))
        page = await read(sessions, cms_query("plans", "relationships", plan_id, 100))
        networks = [
            network_link for network_link in page["items"] if network_link["relationship_type"] == "plan-network"
        ]
        assert len(networks) == 3
        assert any(
            network_link["status"] == "conflict" and network_link["target_id"] is None for network_link in networks
        )
        assert any(
            network_link["status"] == "resolved"
            and network_link["target_id"] == NETWORK_ID
            and network_link["effective_start"] == "2025-03-01"
            for network_link in networks
        )
        assert any(
            network_link["status"] == "unresolved" and network_link["target_id"] is None for network_link in networks
        )
        assert "plan-example" not in json.dumps(page)


async def _add_plan(session, should_bind=True):
    """Add a second sealed synthetic plan, optionally withholding its identity binding."""
    summary = publication_summary()
    summary["source_release"]["files"]["05-InsurancePlan.ndjson"].update(row_count=2, distinct_count=2)
    proof = summary["provider_directory_content_proof_admission_summary_v1"]
    proof["resource_counts"]["InsurancePlan"] = 2
    proof["resource_count"] = 9
    await session.execute(
        text("""INSERT INTO provider_directory_dataset_resource
            SELECT dataset_id, resource_type, 'second-plan', payload_hash,
            '{"name":"Second Plan","status":"inactive"}'::json, acquired_resource_sha256
            FROM provider_directory_dataset_resource WHERE resource_type='InsurancePlan' LIMIT 1""")
    )
    await session.execute(
        text(
            "UPDATE provider_directory_endpoint_dataset SET resource_count=9, "
            "publication_metadata_summary_json=CAST(:summary AS jsonb)"
        ),
        {"summary": json.dumps(summary)},
    )
    await reseal(session)
    if should_bind:
        await seed_resource_identities(session, resource_type="InsurancePlan", resource_ids=["second-plan"])


@pytest.mark.asyncio
async def test_plan_uuid_keyset_and_generation_restart(monkeypatch):
    """Indexed UUID pagination does not leak source seek keys or survive a new cutover."""
    async with cms_database(monkeypatch, prepare=_add_plan) as sessions:
        first = await read(sessions, cms_query("plans"))
        continuation = replace(cms_query("plans"), generation_id=first["generation_id"], cursor=first["next_cursor"])
        second = await read(sessions, continuation)
        assert second["next_cursor"] is None
        identities = [first["items"][0]["id"], second["items"][0]["id"]]
        assert identities == sorted(set(identities)) and len(identities) == 2
        await change(sessions, "UPDATE provider_directory_endpoint_dataset SET published_at='2026-02-01'")
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, continuation)
        assert caught.value.status == 409
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query("plans", limit=100))
        assert caught.value.status == 503


@pytest.mark.asyncio
async def test_new_published_dataset_does_not_inherit_old_coverage(monkeypatch):
    async with cms_database(monkeypatch) as sessions:
        first = await read(sessions, cms_query())
        async with sessions() as session, session.begin():
            await session.execute(
                text("""INSERT INTO provider_directory_endpoint_dataset
                    (dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
                     status, is_current, resource_count, validated_at,
                     publication_metadata_summary_json, publication_metadata_sha256,
                     content_proof_admission_version, content_proof_admission_kind,
                     content_proof_admission_sha256, content_proof_resource_types)
                    SELECT 'synthetic-next', endpoint_id, acquisition_root_run_id,
                           dataset_hash, 'validated', false, resource_count, validated_at,
                           publication_metadata_summary_json, publication_metadata_sha256,
                           content_proof_admission_version, content_proof_admission_kind,
                           content_proof_admission_sha256, content_proof_resource_types
                    FROM provider_directory_endpoint_dataset WHERE dataset_id='synthetic-dataset'""")
            )
            await session.execute(
                text("""INSERT INTO provider_directory_dataset_resource
                    SELECT 'synthetic-next', resource_type, resource_id, payload_hash,
                           payload_json, acquired_resource_sha256
                    FROM provider_directory_dataset_resource WHERE dataset_id='synthetic-dataset'""")
            )
            await session.execute(
                text("""UPDATE provider_directory_endpoint_dataset
                    SET status='superseded', is_current=false, superseded_at='2026-02-02'
                    WHERE dataset_id='synthetic-dataset'""")
            )
            await session.execute(
                text("""UPDATE provider_directory_endpoint_dataset
                    SET status='published', is_current=true, published_at='2026-02-02'
                    WHERE dataset_id='synthetic-next'""")
            )
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, replace(cms_query(), generation_id=first["generation_id"]))
        assert caught.value.status == 409
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query())
        assert caught.value.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE provider_directory_endpoint_dataset SET is_current=false, status='validated'",
        "UPDATE provider_directory_endpoint_dataset SET content_proof_admission_kind='uhc_canonical'",
        "UPDATE provider_directory_endpoint_dataset SET content_proof_admission_version=NULL",
        "UPDATE provider_directory_endpoint_dataset SET publication_metadata_sha256=repeat('0',64)",
        "UPDATE provider_directory_endpoint_dataset SET content_proof_resource_types=ARRAY['Organization']",
        "UPDATE provider_directory_source SET endpoint_id=NULL",
    ],
)
async def test_unaccepted_or_incompletely_bound_source_fails_closed(monkeypatch, mutation):
    async with cms_database(monkeypatch) as sessions:
        await change(sessions, mutation)
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query())
        assert caught.value.status == 503


@pytest.mark.asyncio
async def test_even_resealed_incomplete_eight_file_vector_is_not_accepted(monkeypatch):
    async with cms_database(monkeypatch) as sessions:
        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE provider_directory_endpoint_dataset SET publication_metadata_summary_json="
                    "publication_metadata_summary_json #- '{source_release,files,08-OrganizationAffiliation.ndjson}'"
                )
            )
            await reseal(session)
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query())
        assert caught.value.status == 503


@pytest.mark.asyncio
async def test_missing_plan_binding_cannot_return_partial_page(monkeypatch):
    async def prepare(session):
        await _add_plan(session, should_bind=False)

    async with cms_database(monkeypatch, prepare=prepare, seal=False) as sessions:
        from process.provider_directory_cms_serving_coverage import build_cms_coverage

        async with sessions() as session:
            with pytest.raises(DirectoryReadError):
                await build_cms_coverage(session)
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query("plans"))
        assert caught.value.status == 503
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query("organizations"))
        assert caught.value.status == 503


@pytest.mark.asyncio
async def test_missing_network_witness_does_not_silently_omit_network(monkeypatch):
    async def prepare(session):
        await session.execute(text("DELETE FROM provider_directory_insurance_network_plan_evidence"))

    async with cms_database(monkeypatch, prepare=prepare, seal=False) as sessions:
        from process.provider_directory_cms_serving_coverage import build_cms_coverage

        async with sessions() as session:
            with pytest.raises(DirectoryReadError):
                await build_cms_coverage(session)
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query("networks"))
        assert caught.value.status == 503
        await change(
            sessions,
            "INSERT INTO provider_directory_insurance_network_plan_evidence VALUES "
            "('cms-npd', repeat('a',64), 'org-example', 'plan-example', repeat('b',64))",
        )
        async with sessions() as session:
            await build_cms_coverage(session)
        assert len((await read(sessions, cms_query("networks")))["items"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("network_type", ([{"text": "ntwk"}], [{"coding": [{"code": "ntwk"}]}]))
async def test_source_declared_network_without_plan_witness_is_served(monkeypatch, network_type):
    async def prepare(session):
        await session.execute(text("DELETE FROM provider_directory_insurance_network_plan_evidence"))
        await session.execute(
            text(
                "UPDATE provider_directory_dataset_resource SET payload_json="
                "jsonb_set(payload_json::jsonb, '{network_refs}', '[]'::jsonb) "
                "WHERE resource_type='InsurancePlan'"
            )
        )
        await session.execute(
            text(
                "UPDATE provider_directory_entity_release_evidence SET payload_json=CAST(:payload AS jsonb) "
                "WHERE resource_type='Organization' AND resource_id='org-example'"
            ),
            {"payload": json.dumps({"resourceType": "Organization", "id": "org-example", "type": network_type})},
        )

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        page = await read(sessions, cms_query("networks"))
        assert [item["id"] for item in page["items"]] == [NETWORK_ID]
        network = page["items"][0]
        assert {entry["resource_type"] for entry in network["evidence"]} == {"Organization"}
        assert (await read(sessions, cms_query("networks", "entity", NETWORK_ID)))["item"] == network
        assert (await read(sessions, cms_query("networks", "relationships", NETWORK_ID)))["items"] == []


@pytest.mark.asyncio
async def test_source_declared_network_without_binding_fails_coverage(monkeypatch):
    async def prepare(session):
        await session.execute(text("DELETE FROM provider_directory_insurance_network_plan_evidence"))
        await session.execute(text("DELETE FROM provider_directory_insurance_network_source_binding"))
        await session.execute(
            text(
                "UPDATE provider_directory_dataset_resource SET payload_json="
                "jsonb_set(payload_json::jsonb, '{network_refs}', '[]'::jsonb) "
                "WHERE resource_type='InsurancePlan'"
            )
        )
        await session.execute(
            text(
                "UPDATE provider_directory_entity_release_evidence SET payload_json="
                '\'{"type":[{"text":"ntwk"}]}\'::jsonb WHERE resource_type=\'Organization\''
            )
        )

    async with cms_database(monkeypatch, prepare=prepare, seal=False) as sessions:
        from process.provider_directory_cms_serving_coverage import build_cms_coverage

        async with sessions() as session:
            with pytest.raises(DirectoryReadError) as caught:
                await build_cms_coverage(session)
        assert caught.value.status == 503


@pytest.mark.asyncio
async def test_old_plan_only_coverage_cannot_approve_a_new_network_role(monkeypatch):
    from process.provider_directory_cms_serving_coverage import build_cms_coverage

    async with cms_database(monkeypatch, prepare=_prepare_new_network_role, seal=False) as sessions:
        # Model a receipt retained from before the new-writer constraint existed.
        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    "ALTER TABLE provider_directory_cms_serving_coverage "
                    "DROP CONSTRAINT cms_npd_coverage_new_receipt_v2_check"
                )
            )
            await session.execute(
                text(
                    "INSERT INTO provider_directory_cms_serving_coverage "
                    "(dataset_id, release_id, dataset_hash, published_at, created_at) "
                    "SELECT dataset_id, :release_id, dataset_hash, published_at, now() "
                    "FROM provider_directory_endpoint_dataset WHERE dataset_id='synthetic-dataset'"
                ),
                {"release_id": publication_summary()["source_release"]["vector_sha256"]},
            )
            await session.execute(
                text(
                    "ALTER TABLE provider_directory_cms_serving_coverage "
                    "ADD CONSTRAINT cms_npd_coverage_new_receipt_v2_check CHECK (proof_version=2) NOT VALID"
                )
            )
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query("networks"))
        assert caught.value.status == 503
        async with sessions() as session:
            with pytest.raises(DirectoryReadError) as caught:
                await build_cms_coverage(session)
        assert caught.value.status == 503
        await change(
            sessions,
            "INSERT INTO provider_directory_insurance_network_source_binding VALUES "
            "('cms-npd','Organization','org-example','" + NETWORK_ID + "')",
        )
        async with sessions() as session:
            await build_cms_coverage(session)
        assert [network_item["id"] for network_item in (await read(sessions, cms_query("networks")))["items"]] == [
            NETWORK_ID
        ]
        async with sessions() as session:
            versions = (
                (
                    await session.execute(
                        text("SELECT proof_version FROM provider_directory_cms_serving_coverage ORDER BY proof_version")
                    )
                )
                .scalars()
                .all()
            )
        assert versions == [1, 2]


@pytest.mark.asyncio
async def test_coverage_seal_serializes_same_release_network_writes(monkeypatch):
    from process import provider_directory_cms_serving_coverage as coverage

    async def prepare(session):
        await session.execute(
            text("""INSERT INTO provider_directory_entity_source_binding
                (source_id, resource_type, resource_id, organization_id)
                VALUES ('other-source', 'Organization', 'other-org', :organization_id)"""),
            {"organization_id": ORG_ID},
        )

    started = asyncio.Event()
    resume = asyncio.Event()
    original = coverage.require_cms_bindings

    async def pause_first_check(session, schema, generation, kind):
        if not started.is_set():
            started.set()
            await resume.wait()
        return await original(session, schema, generation, kind)

    monkeypatch.setattr(coverage, "require_cms_bindings", pause_first_check)
    async with cms_database(monkeypatch, prepare=prepare, seal=False) as sessions:

        async def seal():
            async with sessions() as session:
                await coverage.build_cms_coverage(session)

        builder = asyncio.create_task(seal())
        try:
            await asyncio.wait_for(started.wait(), 3)
            writer = asyncio.create_task(
                change(
                    sessions,
                    "INSERT INTO provider_directory_insurance_network_plan_evidence VALUES "
                    "('cms-npd', repeat('a',64), 'org-example', 'late-plan', repeat('b',64))",
                )
            )
            await asyncio.sleep(0.05)
            assert not writer.done()
            await asyncio.wait_for(
                change(
                    sessions,
                    "UPDATE provider_directory_entity_source_binding "
                    "SET organization_id='00000000-0000-0000-0000-000000000099' "
                    "WHERE source_id='other-source'",
                ),
                1,
            )
        finally:
            resume.set()
        await builder
        with pytest.raises(DBAPIError, match="covered_network_witness_immutable"):
            await asyncio.wait_for(writer, 3)


@pytest.mark.asyncio
async def test_coverage_builder_does_not_require_api_cursor_key(monkeypatch):
    from process import provider_directory_cms_serving_coverage as coverage

    async with cms_database(monkeypatch, seal=False) as sessions:
        monkeypatch.delenv(CURSOR_KEY_ENV)
        async with sessions() as session:
            await coverage.build_cms_coverage(session)

        async def reject_rescan(*_args):
            raise AssertionError("covered release was rescanned")

        monkeypatch.setattr(coverage, "require_cms_bindings", reject_rescan)
        async with sessions() as session:
            assert await coverage.build_cms_coverage(session) == "synthetic-dataset"
        async with sessions() as session:
            count = await session.scalar(text("SELECT count(*) FROM provider_directory_cms_serving_coverage"))
        assert count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "DELETE FROM provider_directory_entity_release_evidence WHERE resource_id='org-example'",
        "UPDATE provider_directory_entity_release_evidence SET payload_sha256=repeat('d',64) "
        "WHERE resource_id='org-example'",
    ],
)
async def test_changed_or_missing_release_evidence_cannot_get_a_receipt(monkeypatch, mutation):
    from process.provider_directory_cms_serving_coverage import build_cms_coverage

    async def prepare(session):
        await session.execute(text(mutation))

    async with cms_database(monkeypatch, prepare=prepare, seal=False) as sessions:
        async with sessions() as session:
            with pytest.raises(DirectoryReadError) as caught:
                await build_cms_coverage(session)
            assert caught.value.status == 503
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query())
        assert caught.value.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation,reason",
    [
        ("DELETE FROM provider_directory_entity_source_binding WHERE resource_id='org-example'", "evidence_immutable"),
        ("UPDATE provider_directory_entity_release_evidence SET payload_sha256=repeat('d',64)", "evidence_immutable"),
        ("DELETE FROM provider_directory_insurance_network_plan_evidence", "evidence_immutable"),
        ("TRUNCATE provider_directory_insurance_network_plan_evidence", "covered_truncate_forbidden"),
        ("TRUNCATE provider_directory_cms_serving_coverage", "covered_truncate_forbidden"),
        (
            "INSERT INTO provider_directory_insurance_network_plan_evidence "
            "SELECT source_id, release_id, network_resource_id, 'late-plan', plan_payload_sha256 "
            "FROM provider_directory_insurance_network_plan_evidence LIMIT 1",
            "network_witness_immutable",
        ),
        (
            "UPDATE provider_directory_dataset_resource SET acquired_resource_sha256=repeat('d',64) "
            "WHERE resource_type='Organization'",
            "resource_immutable",
        ),
    ],
)
async def test_published_coverage_inputs_cannot_change(monkeypatch, mutation, reason):
    async with cms_database(monkeypatch) as sessions:
        with pytest.raises(DBAPIError, match=reason):
            await change(sessions, mutation)
        assert len((await read(sessions, cms_query()))["items"]) == 1


@pytest.mark.asyncio
async def test_cms_coverage_does_not_discard_other_source_updates(monkeypatch):
    async def prepare(session):
        await session.execute(
            text("""INSERT INTO provider_directory_entity_source_binding
                (source_id, resource_type, resource_id, organization_id)
                VALUES ('other-source', 'Organization', 'other-org', :organization_id)"""),
            {"organization_id": ORG_ID},
        )

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        await change(
            sessions,
            "UPDATE provider_directory_entity_source_binding "
            "SET organization_id='00000000-0000-0000-0000-000000000099' "
            "WHERE source_id='other-source'",
        )
        async with sessions() as session:
            observed = await session.scalar(
                text(
                    "SELECT organization_id FROM provider_directory_entity_source_binding WHERE source_id='other-source'"
                )
            )
        assert str(observed) == "00000000-0000-0000-0000-000000000099"


@pytest.mark.asyncio
async def test_prebound_candidate_identity_is_not_an_accepted_entity(monkeypatch):
    async with cms_database(monkeypatch) as sessions:
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query("organizations", "entity", "00000000-0000-0000-0000-000000000099"))
        assert caught.value.status == 404


@pytest.mark.asyncio
async def test_identity_history_remains_immutable(monkeypatch):
    """Accepted fixture history retains its native immutable-update guard."""
    async with cms_database(monkeypatch) as sessions:
        with pytest.raises(DBAPIError, match="identity_immutable"):
            await change(
                sessions,
                "UPDATE provider_directory_resource_identity SET entity_id='00000000-0000-0000-0000-000000000099'",
            )


@pytest.mark.asyncio
async def test_identity_binder_requires_candidate_scope():
    from process.provider_directory_generic_candidate import GenericCandidateError

    with pytest.raises(GenericCandidateError, match="physical_session_required"):
        await bind_resource_identity_batch(
            object(), source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["fresh-plan"]
        )


def test_uuid_identity_is_exact_source_type_scoped_and_stable():
    identity = source_resource_uuid("cms-npd", "InsurancePlan", "plan-example")
    assert isinstance(identity, UUID) and identity.version == 5
    assert identity != source_resource_uuid("other-source", "InsurancePlan", "plan-example")
    assert identity != source_resource_uuid("cms-npd", "PractitionerRole", "plan-example")
    assert identity == source_resource_uuid("cms-npd", "InsurancePlan", "plan-example")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,source_id", [("payers", "unknown"), ("organizations", "unknown")])
async def test_unsupported_payer_or_source_never_opens_a_transaction(kind, source_id):
    with pytest.raises(DirectoryReadError) as caught:
        await read_cms_entities(None, replace(cms_query(kind), source_id=source_id))
    assert caught.value.status == 503


@pytest.mark.asyncio
async def test_network_relationship_uuid_cursor_covers_each_exact_plan(monkeypatch):
    async def prepare(session):
        await _add_plan(session)
        await session.execute(
            text("""UPDATE provider_directory_dataset_resource SET payload_json=
                jsonb_set(payload_json::jsonb, '{network_refs}', '["Organization/org-example"]')::json
                WHERE resource_id='second-plan'""")
        )
        await session.execute(
            text("""INSERT INTO provider_directory_insurance_network_plan_evidence
                SELECT source_id, release_id, network_resource_id, 'second-plan', plan_payload_sha256
                FROM provider_directory_insurance_network_plan_evidence LIMIT 1""")
        )

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        query = cms_query("networks", "relationships", NETWORK_ID)
        first = await read(sessions, query)
        second = await read(sessions, replace(query, generation_id=first["generation_id"], cursor=first["next_cursor"]))
        assert first["next_cursor"] is not None and second["next_cursor"] is None
        ids = [first["items"][0]["target_id"], second["items"][0]["target_id"]]
        assert ids == sorted(set(ids)) and len(ids) == 2
        assert all(item["evidence"][0]["resource_type"] == "InsurancePlan" for item in first["items"] + second["items"])


@pytest.mark.asyncio
async def test_projection_discards_invalid_status_and_control_characters(monkeypatch):
    async def prepare(session):
        await session.execute(
            text(
                "UPDATE provider_directory_dataset_resource SET payload_json=CAST(:payload AS json) "
                "WHERE resource_type='Organization'"
            ),
            {"payload": json.dumps({"name": "unsafe\nlabel", "active": "true"})},
        )

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        entity = (await read(sessions, cms_query()))["items"][0]
        assert entity["display_name"] is None and entity["status"] == "unknown"


def test_identity_migration_appends_without_removing_published_identity_history():
    from tests.provider_directory_cms_postgres_support import migration_module

    migration = migration_module("20260930010000")
    assert migration.down_revision == "20260929030000_provider_directory_mrf_payer_binding"
    with pytest.raises(RuntimeError, match="requires_explicit_plan"):
        migration.downgrade()


@pytest.mark.asyncio
async def test_cancelled_read_releases_its_snapshot(monkeypatch):
    import asyncio

    from api import provider_directory_cms_entities as serving

    async def cancel_read(*args, **kwargs):
        raise asyncio.CancelledError

    async with cms_database(monkeypatch) as sessions:
        monkeypatch.setattr(serving, "cms_entity_rows", cancel_read)
        async with sessions() as session:
            with pytest.raises(asyncio.CancelledError):
                await read_cms_entities(session, cms_query())
            assert not session.in_transaction()
            assert await session.scalar(text("SELECT 1")) == 1


def _coordinates(schema, dataset="synthetic-dataset"):
    return RegistryNetworkSourceCoordinates(
        "fhir", "cms-npd", schema, dataset, "synthetic-producer", "synthetic-edition"
    )


async def _prepare(session):
    await _add_plan(session)
    for statement in (
        "CREATE TABLE network_membership_source (network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)",
        "CREATE TABLE network_membership (LIKE network_membership_source)",
        "CREATE TABLE provider_location_binding (provider_system text,provider_id text,location_id uuid,location_key text,entity_type text,entity_id text)",
        "CREATE TABLE provider_directory_cms_npd_resource_witness (dataset_id text,source_id text,release_id text,resource_type text,resource_id text,normalized_payload_hash text,raw_payload_sha256 text,raw_payload_json jsonb)",
    ):
        await session.execute(text(statement))
    coordinates = _coordinates(os.environ["HLTHPRT_DB_SCHEMA"])
    descriptor_by_field = {
        **asdict(coordinates),
        "alias_scope": "synthetic-alias",
        "source_key_kind": "organization_resource_id",
    }
    await session.execute(
        text(
            "UPDATE provider_directory_endpoint_dataset SET publication_metadata_summary_json="
            "jsonb_set(publication_metadata_summary_json,'{network_bindings}',CAST(:value AS jsonb))"
        ),
        {"value": json.dumps(descriptor_by_field)},
    )
    await reseal(session)
    await session.execute(
        text(
            "CREATE TABLE registry_approved_record (approved_revision bigint,record_kind text,"
            "record_key text,record_revision bigint,custom_revision bigint,record_json jsonb)"
        )
    )
    for kind, key, document in (
        ("network", "1001", {"network_id": 1001, "archived": False}),
        (
            "network_binding",
            "synthetic-binding",
            {
                **asdict(coordinates),
                "network_id": 1001,
                "archived": False,
                "source_key": "org-example",
                "source_scope_json": {
                    "organization_id": "org-example",
                    "legacy_uuid": NETWORK_ID,
                    "alias_scope": "synthetic-alias",
                },
            },
        ),
    ):
        await session.execute(
            text("INSERT INTO registry_approved_record VALUES(7,:kind,:key,1,7,CAST(:document AS jsonb))"),
            {"kind": kind, "key": key, "document": json.dumps(document)},
        )


@pytest.fixture
def trusted_recipe(monkeypatch):
    """Mock only the existing manifest/map authorities; source admission and scope SQL remain real."""
    state_by_field = {"dataset": "synthetic-dataset"}

    async def manifest(driver, *, generation_id=None):
        if generation_id not in (None, 9):
            raise ValueError("unavailable exact canonical pin")
        return PinnedNetworkServingManifest(
            9, "00000000-0000-0000-0000-000000000001", os.environ["HLTHPRT_DB_SCHEMA"], 1, {}, 7, "c" * 64, 1
        )

    async def recipes(driver, manifest):
        schema = os.environ["HLTHPRT_DB_SCHEMA"]
        oid = await driver.fetchval("SELECT to_regclass($1)::oid", schema + ".provider_directory_dataset_resource")
        source = PinnedFHIRMembershipSource(
            schema,
            "cms-npd",
            "synthetic-endpoint",
            state_by_field["dataset"],
            "b" * 64,
            "a" * 64,
            oid,
            "synthetic-alias",
            "2026-01-01",
        )
        return (RegistrySourceMembershipRecipe(source, _coordinates(schema, state_by_field["dataset"])),)

    async def approved(driver, *, approved_revision):
        assert approved_revision == 7
        return ApprovedMembershipSource(7, "d" * 64)

    monkeypatch.setattr(scoped, "resolve_network_serving_manifest", manifest)
    monkeypatch.setattr(scoped, "resolve_retained_registry_source_recipes", recipes)
    monkeypatch.setattr(scoped, "pin_retained_approved_membership_source", approved)
    return state_by_field


def _query(kind="plans", shape="entities", entity_id=None, limit=1):
    return replace(
        cms_query(kind, shape, entity_id, limit),
        network_ids=(1001,),
        network_generation=9,
        access_scope_sha256="e" * 64,
    )


@pytest.mark.asyncio
async def test_scope_filters_before_lookahead_and_both_relationship_ends(monkeypatch, trusted_recipe):
    async with cms_database(monkeypatch, prepare=_prepare) as sessions:
        legacy = await read(sessions, cms_query("plans", limit=100))
        assert len(legacy["items"]) == 2
        response_headers_by_name = {}
        async with sessions() as session:
            page = await read_cms_entities(session, _query(), response_headers=response_headers_by_name)
        assert response_headers_by_name["X-Network-Generation"] == "9"
        assert len(page["items"]) == 1 and page["next_cursor"] is None
        assert page["generation_id"] != legacy["generation_id"]
        selected_id = page["items"][0]["id"]
        forbidden_id = next(item["id"] for item in legacy["items"] if item["id"] != selected_id)
        with pytest.raises(DirectoryReadError) as error:
            await read(sessions, _query(shape="entity", entity_id=forbidden_id))
        assert error.value.status == 404
        links = await read(sessions, _query(shape="relationships", entity_id=selected_id, limit=100))
        assert links["items"]
        assert {item["target_id"] for item in links["items"]} <= {ORG_ID, NETWORK_ID}
        assert all(item["status"] == "resolved" for item in links["items"])
        for entity_id in (forbidden_id,):
            with pytest.raises(DirectoryReadError) as error:
                await read(sessions, _query(shape="relationships", entity_id=entity_id))
            assert error.value.status == 404


@pytest.mark.asyncio
async def test_wrong_selected_mapping_canonical_and_source_pins_fail_closed(monkeypatch, trusted_recipe):
    async with cms_database(monkeypatch, prepare=_prepare) as sessions:
        for query in (
            replace(_query(), network_ids=(1002,)),
            replace(_query(), network_generation=10),
            replace(_query(), generation_id="gen_other"),
        ):
            with pytest.raises(DirectoryReadError) as error:
                await read(sessions, query)
            assert error.value.status in (409, 503)
        trusted_recipe["dataset"] = "another-source-generation"
        with pytest.raises(DirectoryReadError) as error:
            await read(sessions, _query())
        assert error.value.status == 503


@pytest.mark.asyncio
async def test_scoped_redirect_cannot_expose_an_unselected_target(monkeypatch, trusted_recipe):
    async with cms_database(monkeypatch, prepare=_prepare) as sessions:
        async with sessions() as session, session.begin():
            await session.execute(
                text("INSERT INTO provider_directory_entity_redirect VALUES ('cms-npd','Organization',:old,:target)"),
                {"old": UUID(ORG_ID), "target": UUID("00000000-0000-0000-0000-000000000099")},
            )
        with pytest.raises(DirectoryReadError) as error:
            await read(sessions, _query("organizations", "entity", ORG_ID))
        assert error.value.status == 503


@pytest.mark.parametrize("kind", ("sites", "practitioner-roles", "payers"))
@pytest.mark.asyncio
async def test_unproved_scoped_records_are_excluded(monkeypatch, trusted_recipe, kind):
    async with cms_database(monkeypatch, prepare=_prepare) as sessions:
        assert (await read(sessions, _query(kind)))["items"] == []


@pytest.mark.asyncio
async def test_shared_sql_predicate_filters_identity_alias_count_before_projection(monkeypatch, trusted_recipe):
    from api.provider_directory_cms_generation import accepted_cms_generation
    from api.provider_directory_cms_queries import cms_entity_rows

    async def prepare(session):
        await _prepare(session)
        await session.execute(
            text(
                "INSERT INTO provider_directory_dataset_resource SELECT dataset_id,resource_type,"
                "'unselected-alias',payload_hash,payload_json,acquired_resource_sha256 "
                "FROM provider_directory_dataset_resource WHERE resource_type='Organization'"
            )
        )
        await session.execute(
            text(
                "INSERT INTO provider_directory_entity_source_binding VALUES "
                "('cms-npd','Organization','unselected-alias',:identity,NULL)"
            ),
            {"identity": UUID(ORG_ID)},
        )
        await session.execute(
            text(
                "INSERT INTO provider_directory_entity_release_evidence SELECT source_id,resource_type,"
                "'unselected-alias',release_id,payload_sha256,payload_json "
                "FROM provider_directory_entity_release_evidence WHERE resource_type='Organization'"
            )
        )

    # This deliberately inconsistent alias is a SQL component fixture, never a serving acceptance proof.
    async with cms_database(monkeypatch, prepare=prepare, seal=False) as sessions:
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            generation = await accepted_cms_generation(session, None)
            schema = '"' + os.environ["HLTHPRT_DB_SCHEMA"] + '"'
            scope = await scoped.pin_cms_network_scope(session, schema, _query("organizations"), generation)
            selected_generation_by_field = {**generation, **scope.parameters_by_name, "network_scope": scope}
            entity_rows = await cms_entity_rows(session, schema, _query("organizations"), selected_generation_by_field)
            assert len(entity_rows) == 1 and entity_rows[0]["identity_count"] == 1
            with pytest.raises(DirectoryReadError):
                await cms_entity_rows(session, schema, cms_query("organizations"), generation)


@pytest.mark.asyncio
async def test_relationship_continuation_keeps_the_same_source_and_canonical_scope(monkeypatch, trusted_recipe):
    async with cms_database(monkeypatch, prepare=_prepare) as sessions:
        plans = await read(sessions, _query())
        query = _query(shape="relationships", entity_id=plans["items"][0]["id"])
        first = await read(sessions, query)
        assert len(first["items"]) == 1 and first["next_cursor"] is not None
        continuation = replace(query, generation_id=first["generation_id"], cursor=first["next_cursor"])
        second = await read(sessions, continuation)
        assert len(second["items"]) == 1 and second["next_cursor"] is None
        assert second["items"][0]["relationship_key"] != first["items"][0]["relationship_key"]
        with pytest.raises(DirectoryReadError) as error:
            await read(sessions, replace(continuation, access_scope_sha256="f" * 64))
        assert error.value.status == 409
        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE registry_approved_record SET record_json="
                    "jsonb_set(record_json,'{edition_id}','\"another-edition\"') "
                    "WHERE record_kind='network_binding'"
                )
            )
        with pytest.raises(DirectoryReadError) as error:
            await read(sessions, continuation)
        assert error.value.status == 503


async def _add_unselected_office(session):
    await session.execute(
        text("""INSERT INTO provider_directory_dataset_resource
        SELECT dataset_id,resource_type,'unselected-site',payload_hash,
          jsonb_set(payload_json::jsonb,'{resource_id}','"unselected-site"'),acquired_resource_sha256
        FROM provider_directory_dataset_resource WHERE resource_type='Location'""")
    )
    await session.execute(
        text("""INSERT INTO provider_directory_entity_source_binding VALUES
        ('cms-npd','Location','unselected-site',NULL,'00000000-0000-0000-0000-000000000019')""")
    )
    await session.execute(
        text("""INSERT INTO provider_directory_entity_release_evidence
        SELECT source_id,resource_type,'unselected-site',release_id,payload_sha256,payload_json
        FROM provider_directory_entity_release_evidence WHERE resource_type='Location'""")
    )
    await session.execute(
        text("""INSERT INTO provider_directory_dataset_resource
        SELECT dataset_id,resource_type,'unselected-role',payload_hash,
          jsonb_set(payload_json::jsonb,'{resource_id}','"unselected-role"'),acquired_resource_sha256
        FROM provider_directory_dataset_resource WHERE resource_type='PractitionerRole'""")
    )
    await seed_resource_identities(session, resource_type="PractitionerRole", resource_ids=["unselected-role"])
    await session.execute(
        text("""UPDATE provider_directory_dataset_resource SET payload_json=
        jsonb_set(payload_json::jsonb,'{location_refs}',CAST(:refs AS jsonb)) WHERE resource_type='PractitionerRole'"""),
        {"refs": json.dumps(["Location/site-example", "Location/unselected-site"])},
    )
    metadata_by_field = await session.scalar(
        text("SELECT publication_metadata_summary_json FROM provider_directory_endpoint_dataset")
    )
    for filename, kind in (("02-Location.ndjson", "Location"), ("07-PractitionerRole.ndjson", "PractitionerRole")):
        metadata_by_field["source_release"]["files"][filename].update(row_count=2, distinct_count=2)
        metadata_by_field["provider_directory_content_proof_admission_summary_v1"]["resource_counts"][kind] = 2
    metadata_by_field["provider_directory_content_proof_admission_summary_v1"]["resource_count"] = 11
    await session.execute(
        text(
            "UPDATE provider_directory_endpoint_dataset SET resource_count=11, publication_metadata_summary_json=CAST(:metadata AS jsonb)"
        ),
        {"metadata": json.dumps(metadata_by_field)},
    )
    await reseal(session)


async def _prepare_membership(session):
    await _prepare(session)
    await session.execute(
        text("""UPDATE provider_directory_dataset_resource SET payload_json=
        payload_json::jsonb || jsonb_build_object('practitioner_ref','Practitioner/practitioner-example','active',true)
        WHERE resource_type='PractitionerRole'""")
    )
    await session.execute(
        text("""UPDATE provider_directory_dataset_resource SET payload_json=
        payload_json::jsonb || jsonb_build_object('npi',1234567893) WHERE resource_type='Practitioner'""")
    )
    await _add_unselected_office(session)
    await session.execute(
        text("""INSERT INTO provider_directory_cms_npd_relationship VALUES
        ('synthetic-dataset','cms-npd',repeat('a',64),'PractitionerRole','role-example',repeat('b',64),repeat('b',64),
        'practitioner',0,1,'Practitioner/practitioner-example','resolved',NULL,NULL)""")
    )
    await session.execute(
        text("""INSERT INTO provider_directory_cms_npd_resource_witness
        SELECT dataset_id,'cms-npd',repeat('a',64),resource_type,resource_id,payload_hash,
          acquired_resource_sha256,jsonb_build_object('resourceType',resource_type,'id',resource_id,
            'identifier',CASE WHEN resource_type='Practitioner' THEN
            '[{"system":"http://hl7.org/fhir/sid/us-npi","value":"1234567893"}]'::jsonb ELSE '[]'::jsonb END)
        FROM provider_directory_dataset_resource""")
    )
    await session.execute(
        text("""INSERT INTO network_membership_source VALUES
        (1001,'npi','1234567893',:site,encode(sha256(convert_to(jsonb_build_array(
          'synthetic-dataset','cms-npd',repeat('a',64),'PractitionerRole','role-example',repeat('b',64),
          1001,'1234567893',CAST(:site AS uuid))::text,'UTF8')),'hex'))"""),
        {"site": UUID(SITE_ID)},
    )
    await session.execute(text("INSERT INTO network_membership SELECT * FROM network_membership_source"))
    await session.execute(
        text(
            "INSERT INTO provider_location_binding VALUES ('npi','1234567893',:site,repeat('d',64),'npi','1234567893')"
        ),
        {"site": UUID(SITE_ID)},
    )


@pytest.mark.asyncio
async def test_exact_role_site_pair_and_relationships_are_selected(monkeypatch, trusted_recipe):
    async with cms_database(monkeypatch, prepare=_prepare_membership) as sessions:
        role_id = str(source_resource_uuid("cms-npd", "PractitionerRole", "role-example"))
        for kind in ("sites", "practitioner-roles"):
            assert (await read(sessions, cms_query(kind)))["next_cursor"] is not None
        for kind, entity_id in (("sites", SITE_ID), ("practitioner-roles", role_id)):
            page = await read(sessions, _query(kind))
            assert [item["id"] for item in page["items"]] == [entity_id]
            assert page["next_cursor"] is None
            links = await read(sessions, _query(kind, "relationships", entity_id, 100))
            assert links["items"]
            assert {item["target_id"] for item in links["items"]} <= {
                ORG_ID,
                NETWORK_ID,
                SITE_ID,
                "1234567893",
                str(source_resource_uuid("cms-npd", "InsurancePlan", "plan-example")),
            }


@pytest.mark.parametrize(
    "mutation",
    (
        "UPDATE network_membership SET location_id='00000000-0000-0000-0000-000000000099'",
        "UPDATE provider_location_binding SET provider_id='wrong-provider'",
        "UPDATE network_membership_source SET evidence_id=repeat('0',64)",
        "UPDATE provider_directory_cms_npd_resource_witness SET release_id='wrong-release'",
    ),
)
@pytest.mark.asyncio
async def test_membership_requires_source_evidence_and_same_effective_office(monkeypatch, trusted_recipe, mutation):
    async with cms_database(monkeypatch, prepare=_prepare_membership) as sessions:
        await change(sessions, mutation)
        for kind in ("sites", "practitioner-roles"):
            assert (await read(sessions, _query(kind)))["items"] == []


@pytest.mark.asyncio
async def test_scoped_payer_requires_selected_plan_issuer_before_seek(monkeypatch, trusted_recipe):
    from tests.test_provider_directory_cms_payers import _add_second_payer, _review_payer

    async def prepare(session):
        await _prepare(session)
        metadata_by_field = await session.scalar(
            text("SELECT publication_metadata_summary_json FROM provider_directory_endpoint_dataset")
        )
        await _add_second_payer(session)
        metadata_by_field["source_release"]["files"]["01-Organization.ndjson"].update(row_count=2, distinct_count=2)
        metadata_by_field["provider_directory_content_proof_admission_summary_v1"]["resource_counts"][
            "Organization"
        ] = 2
        metadata_by_field["provider_directory_content_proof_admission_summary_v1"]["resource_count"] = 10
        await session.execute(
            text(
                "UPDATE provider_directory_endpoint_dataset SET resource_count=10, publication_metadata_summary_json=CAST(:metadata AS jsonb)"
            ),
            {"metadata": json.dumps(metadata_by_field)},
        )
        await session.execute(
            text("""UPDATE provider_directory_dataset_resource SET payload_json=
            jsonb_set(payload_json::jsonb,'{owned_by_ref}','"Organization/second-org"') WHERE resource_id='second-plan'""")
        )
        await session.execute(
            text("""UPDATE provider_directory_dataset_resource SET payload_json=
            jsonb_set(payload_json::jsonb,'{part_of_ref}','"Organization/second-org"') WHERE resource_id='org-example'""")
        )
        await reseal(session)

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        await _review_payer(sessions)
        assert (await read(sessions, _query("organizations", limit=100)))["items"][0]["id"] == ORG_ID
        assert (await read(sessions, cms_query("payers")))["next_cursor"] is not None
        page = await read(sessions, _query("payers"))
        assert [payer_entry["id"] for payer_entry in page["items"]] == ["existing_payer-17"]
        assert page["next_cursor"] is None
        links = await read(sessions, _query("payers", "relationships", "existing_payer-17"))
        assert [payer_link["target_id"] for payer_link in links["items"]] == [ORG_ID]
        with pytest.raises(DirectoryReadError) as error:
            await read(sessions, _query("payers", "entity", "A_existing_payer"))
        assert error.value.status == 404


@pytest.mark.asyncio
async def test_site_redirect_and_hidden_role_cannot_escape_scope(monkeypatch, trusted_recipe):
    async with cms_database(monkeypatch, prepare=_prepare_membership) as sessions:
        for kind, entity_id in (
            ("sites", "00000000-0000-0000-0000-000000000019"),
            ("practitioner-roles", str(source_resource_uuid("cms-npd", "PractitionerRole", "unselected-role"))),
        ):
            for shape in ("entity", "relationships"):
                with pytest.raises(DirectoryReadError) as error:
                    await read(sessions, _query(kind, shape, entity_id))
                assert error.value.status == 404
        await change(
            sessions,
            "INSERT INTO provider_directory_entity_redirect VALUES ('cms-npd','Location','"
            + SITE_ID
            + "','00000000-0000-0000-0000-000000000019')",
        )
        with pytest.raises(DirectoryReadError) as error:
            await read(sessions, _query("sites", "entity", SITE_ID))
        assert error.value.status == 503


@pytest.mark.asyncio
async def test_exact_non_npi_source_identity_can_prove_a_role_site_pair(monkeypatch, trusted_recipe):
    from process.network_cms_provider_identity import CMSProviderIdentity

    async def prepare(session):
        await _prepare_membership(session)
        provider_id = CMSProviderIdentity("cms-npd", "Practitioner", "practitioner-example").provider_id
        await session.execute(
            text(
                "UPDATE provider_directory_dataset_resource SET payload_json=payload_json::jsonb-'npi' WHERE resource_type='Practitioner'"
            )
        )
        await session.execute(
            text(
                "UPDATE provider_directory_cms_npd_resource_witness SET raw_payload_json=raw_payload_json-'identifier' WHERE resource_type='Practitioner'"
            )
        )
        for table in ("network_membership_source", "network_membership", "provider_location_binding"):
            await session.execute(text("DELETE FROM " + table))
        await session.execute(
            text("""INSERT INTO network_membership_source VALUES
            (1001,'provider_directory',:provider_id,:site,encode(sha256(convert_to(jsonb_build_array(
              'synthetic-dataset','cms-npd',repeat('a',64),'PractitionerRole','role-example',repeat('b',64),
              1001,'provider_directory',CAST(:provider_id AS text),'Practitioner','practitioner-example',repeat('b',64),CAST(:site AS uuid))::text,'UTF8')),'hex'))"""),
            {"provider_id": provider_id, "site": UUID(SITE_ID)},
        )
        await session.execute(text("INSERT INTO network_membership SELECT * FROM network_membership_source"))
        await session.execute(
            text(
                "INSERT INTO provider_location_binding VALUES ('provider_directory',:provider_id,:site,repeat('d',64),'provider_directory',:provider_id)"
            ),
            {"provider_id": provider_id, "site": UUID(SITE_ID)},
        )

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        assert [site_entry["id"] for site_entry in (await read(sessions, _query("sites")))["items"]] == [SITE_ID]
        assert len((await read(sessions, _query("practitioner-roles")))["items"]) == 1


@pytest.mark.asyncio
async def test_selected_plan_issuer_expands_exact_source_organization(monkeypatch, trusted_recipe):
    from tests.test_provider_directory_cms_payers import _add_second_payer

    async def prepare(session):
        await _prepare(session)
        metadata_by_field = await session.scalar(
            text("SELECT publication_metadata_summary_json FROM provider_directory_endpoint_dataset")
        )
        await _add_second_payer(session)
        metadata_by_field["source_release"]["files"]["01-Organization.ndjson"].update(row_count=2, distinct_count=2)
        metadata_by_field["provider_directory_content_proof_admission_summary_v1"]["resource_counts"][
            "Organization"
        ] = 2
        metadata_by_field["provider_directory_content_proof_admission_summary_v1"]["resource_count"] = 10
        await session.execute(
            text(
                "UPDATE provider_directory_endpoint_dataset SET resource_count=10, publication_metadata_summary_json=CAST(:metadata AS jsonb)"
            ),
            {"metadata": json.dumps(metadata_by_field)},
        )
        await session.execute(
            text("""UPDATE provider_directory_dataset_resource SET payload_json=
            jsonb_set(payload_json::jsonb,'{owned_by_ref}','"Organization/second-org"') WHERE resource_id='plan-example'""")
        )
        await reseal(session)

    async with cms_database(monkeypatch, prepare=prepare) as sessions:
        assert [payer_entry["id"] for payer_entry in (await read(sessions, _query("payers")))["items"]] == [
            "A_existing_payer"
        ]
        organizations = await read(sessions, _query("organizations", limit=100))
        assert {organization_entry["id"] for organization_entry in organizations["items"]} == {
            ORG_ID,
            "00000000-0000-0000-0000-000000000011",
        }


@pytest.mark.asyncio
async def test_missing_pinned_membership_heap_is_explicitly_unavailable(monkeypatch, trusted_recipe):
    async with cms_database(monkeypatch, prepare=_prepare_membership) as sessions:
        await change(sessions, "DROP TABLE network_membership_source")
        with pytest.raises(DirectoryReadError) as error:
            await read(sessions, _query("sites"))
        assert error.value.status == 503


@pytest.mark.asyncio
async def test_scoped_control_rows_change_generation_without_count_change(monkeypatch, trusted_recipe):
    async with cms_database(monkeypatch, prepare=_prepare) as sessions:
        async with sessions() as session, session.begin():
            await session.execute(
                text("INSERT INTO provider_directory_entity_redirect VALUES ('cms-npd','Organization',:old,:target)"),
                {"old": UUID(ORG_ID), "target": UUID(SITE_ID)},
            )
        first = await read(sessions, _query())
        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE provider_directory_entity_redirect SET canonical_entity_id=:target WHERE old_entity_id=:old"
                ),
                {"old": UUID(ORG_ID), "target": UUID(NETWORK_ID)},
            )
        second = await read(sessions, _query())
        assert first["generation_id"] != second["generation_id"]
        with pytest.raises(DirectoryReadError) as stale:
            await read(sessions, replace(_query(), generation_id=first["generation_id"]))
        assert stale.value.status == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", ["rows", "bytes"])
async def test_scoped_control_digest_refuses_native_bounds(monkeypatch, trusted_recipe, bound):
    async with cms_database(monkeypatch, prepare=_prepare) as sessions:
        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    "ALTER TABLE provider_directory_entity_redirect_decision ADD COLUMN old_entity_id uuid, ADD COLUMN review_actor text"
                )
            )
            rows, size = (5001, 1) if bound == "rows" else (1, 1048576)
            await session.execute(
                text(
                    "INSERT INTO provider_directory_entity_redirect_decision SELECT md5(unit::text)::uuid,'cms-npd',CAST(:old AS uuid),repeat('x',:size) FROM generate_series(1,:rows) unit"
                ),
                {"old": ORG_ID, "size": size, "rows": rows},
            )
        with pytest.raises(DirectoryReadError) as refused:
            await read(sessions, _query())
        assert refused.value.status == 503


async def _prepare_new_network_role(session):
    await session.execute(text("DELETE FROM provider_directory_insurance_network_plan_evidence"))
    await session.execute(text("DELETE FROM provider_directory_insurance_network_source_binding"))
    await session.execute(
        text(
            "UPDATE provider_directory_dataset_resource SET payload_json="
            "jsonb_set(payload_json::jsonb, '{network_refs}', '[]'::jsonb) "
            "WHERE resource_type='InsurancePlan'"
        )
    )
    await session.execute(
        text(
            "UPDATE provider_directory_entity_release_evidence SET payload_json="
            '\'{"type":[{"text":"ntwk"}]}\'::jsonb WHERE resource_type=\'Organization\''
        )
    )
