# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native source-bound entity reads with real seals and immutable identity storage."""

import asyncio
import json
from dataclasses import replace
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api.provider_directory_cms_entities import read_cms_entities
from api.provider_directory_entities_contract import CURSOR_KEY_ENV, DirectoryRead, DirectoryReadError
from process.provider_directory_resource_identity import bind_resource_identity_batch, source_resource_uuid
from tests.provider_directory_cms_postgres_support import (
    NETWORK_ID,
    ORG_ID,
    SITE_ID,
    cms_database,
    publication_summary,
    reseal,
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
                text("INSERT INTO provider_directory_entity_redirect_decision VALUES "
                     "('00000000-0000-0000-0000-000000000077', 'cms-npd')")
            )
            await session.execute(
                text("INSERT INTO provider_directory_entity_redirect VALUES "
                     "('cms-npd', 'Organization', :old_id, :canonical_id)"),
                {"old_id": old_id, "canonical_id": ORG_ID},
            )
        stale = replace(cms_query("organizations", "entity", old_id), generation_id=before["generation_id"])
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, stale)
        assert caught.value.status == 409
        detail = await read(sessions, cms_query("organizations", "entity", old_id))
        relations = await read(sessions, cms_query("organizations", "relationships", old_id))
        assert detail == relations == {
            "generation_id": detail["generation_id"],
            "redirect": {
                "source_id": "cms-npd",
                "kind": "organizations",
                "requested_id": old_id,
                "canonical_id": ORG_ID,
            },
        }
        assert detail["generation_id"] != before["generation_id"]
        invalid_cursor = replace(
            cms_query("organizations", "relationships", old_id),
            generation_id=detail["generation_id"], cursor="invalid",
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
                text("INSERT INTO provider_directory_entity_redirect_decision VALUES "
                     "('00000000-0000-0000-0000-000000000078', 'cms-npd')")
            )
            await session.execute(
                text("INSERT INTO provider_directory_entity_redirect VALUES "
                     "('cms-npd', 'Organization', :old_id, :canonical_id)"),
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
        await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["second-plan"]
        )


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

    async def prepare(session):
        await session.execute(text("DELETE FROM provider_directory_insurance_network_plan_evidence"))
        await session.execute(text("DELETE FROM provider_directory_insurance_network_source_binding"))
        await session.execute(text(
            "UPDATE provider_directory_dataset_resource SET payload_json="
            "jsonb_set(payload_json::jsonb, '{network_refs}', '[]'::jsonb) "
            "WHERE resource_type='InsurancePlan'"
        ))
        await session.execute(text(
            "UPDATE provider_directory_entity_release_evidence SET payload_json="
            "'{\"type\":[{\"text\":\"ntwk\"}]}'::jsonb WHERE resource_type='Organization'"
        ))

    async with cms_database(monkeypatch, prepare=prepare, seal=False) as sessions:
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
        await change(sessions,
            "INSERT INTO provider_directory_insurance_network_source_binding VALUES "
            "('cms-npd','Organization','org-example','" + NETWORK_ID + "')")
        async with sessions() as session:
            await build_cms_coverage(session)
        assert [network_item["id"] for network_item in (await read(sessions, cms_query("networks")))["items"]] == [NETWORK_ID]
        async with sessions() as session:
            versions = (await session.execute(text(
                "SELECT proof_version FROM provider_directory_cms_serving_coverage ORDER BY proof_version"
            ))).scalars().all()
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
async def test_identity_binder_replays_and_rolls_back_without_reassignment(monkeypatch):
    """A committed binding survives replay; cancelled transactions leave no new identity."""
    async with cms_database(monkeypatch) as sessions:
        async with sessions() as session, session.begin():
            ids = await bind_resource_identity_batch(
                session,
                source_id="cms-npd",
                resource_type="InsurancePlan",
                resource_ids=["plan-example", "plan-example"],
            )
            assert ids == [source_resource_uuid("cms-npd", "InsurancePlan", "plan-example")] * 2
        async with sessions() as session:
            await bind_resource_identity_batch(
                session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["rolled-back"]
            )
            await session.rollback()
            count = await session.scalar(
                text("SELECT count(*) FROM provider_directory_resource_identity WHERE resource_id='rolled-back'")
            )
            assert count == 0
        with pytest.raises(DBAPIError, match="identity_immutable"):
            await change(
                sessions,
                "UPDATE provider_directory_resource_identity SET entity_id='00000000-0000-0000-0000-000000000099'",
            )


@pytest.mark.asyncio
async def test_identity_binder_fresh_batches_return_scoped_ids_without_rereading(monkeypatch):
    """Fresh insert results suffice, including duplicate input and separate identity scopes."""
    identities = []
    async with cms_database(monkeypatch) as sessions:
        for source_id, resource_type in (
            ("cms-npd", "InsurancePlan"),
            ("other-source", "InsurancePlan"),
            ("cms-npd", "PractitionerRole"),
        ):
            async with sessions() as session, session.begin():
                executed_select_flags = []
                original_execute = session.execute

                async def observe(statement, *args, **kwargs):
                    executed_select_flags.append(statement.is_select)
                    return await original_execute(statement, *args, **kwargs)

                monkeypatch.setattr(session, "execute", observe)
                ids = await bind_resource_identity_batch(
                    session,
                    source_id=source_id,
                    resource_type=resource_type,
                    resource_ids=["fresh-resource", "fresh-resource"],
                )
                expected = source_resource_uuid(source_id, resource_type, "fresh-resource")
                assert ids == [expected, expected]
                assert executed_select_flags == [False]
                identities.append(ids[0])
        assert len(set(identities)) == 3


@pytest.mark.asyncio
async def test_identity_binder_mixed_batch_rereads_only_replayed_keys(monkeypatch):
    async with cms_database(monkeypatch) as sessions:
        async with sessions() as session, session.begin():
            reread_keys = []
            original_execute = session.execute

            async def observe(statement, *args, **kwargs):
                if statement.is_select:
                    reread_keys.append(statement.compile().params["resource_id_1"])
                return await original_execute(statement, *args, **kwargs)

            monkeypatch.setattr(session, "execute", observe)
            ids = await bind_resource_identity_batch(
                session,
                source_id="cms-npd",
                resource_type="InsurancePlan",
                resource_ids=["fresh-plan", "plan-example", "fresh-plan"],
            )
            assert ids == [
                source_resource_uuid("cms-npd", "InsurancePlan", resource_id)
                for resource_id in ("fresh-plan", "plan-example", "fresh-plan")
            ]
            assert reread_keys == [["plan-example"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_field", ["source", "type", "resource", "entity", "duplicate"])
async def test_identity_binder_rejects_invalid_returned_mapping(invalid_field):
    """Validate the actual returned scope and UUID rather than trusting insert inputs."""
    expected = source_resource_uuid("cms-npd", "InsurancePlan", "fresh-plan")
    returned_fields = ["cms-npd", "InsurancePlan", "fresh-plan", expected]
    if invalid_field == "source":
        returned_fields[0] = "other-source"
    elif invalid_field == "type":
        returned_fields[1] = "PractitionerRole"
    elif invalid_field == "resource":
        returned_fields[2] = "unexpected-plan"
    elif invalid_field == "entity":
        returned_fields[3] = UUID("00000000-0000-0000-0000-000000000099")
    rows = [tuple(returned_fields)] * (2 if invalid_field == "duplicate" else 1)

    class InsertResult:
        def all(self):
            return rows

    class Session:
        async def execute(self, statement):
            assert statement.is_insert
            return InsertResult()

    with pytest.raises(ValueError, match="identity_conflict"):
        await bind_resource_identity_batch(
            Session(), source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["fresh-plan"]
        )


async def _contend_identity_insert(sessions, started, contender_by_field):
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        contender_by_field["pid"] = await session.scalar(text("SELECT pg_catalog.pg_backend_pid()"))
        started.set()
        return await bind_resource_identity_batch(
            session,
            source_id="cms-npd",
            resource_type="InsurancePlan",
            resource_ids=["fresh-during-race", "concurrent-plan", "fresh-during-race"],
        )


async def _wait_for_identity_block(winner, winner_pid, contender_by_field, contender_task):
    while not await winner.scalar(
        text("SELECT :holder = ANY(pg_catalog.pg_blocking_pids(:contender))"),
        {"holder": winner_pid, "contender": contender_by_field["pid"]},
    ):
        assert not contender_task.done()
        await asyncio.sleep(0.01)


async def _assert_identity_contender(contender_task, winner_outcome):
    if winner_outcome == "conflicting_commit":
        with pytest.raises(ValueError, match="identity_conflict"):
            await asyncio.wait_for(contender_task, 3)
    else:
        assert await asyncio.wait_for(contender_task, 3) == [
            source_resource_uuid("cms-npd", "InsurancePlan", resource_id)
            for resource_id in ("fresh-during-race", "concurrent-plan", "fresh-during-race")
        ]


async def _run_identity_contender(sessions, winner, winner_pid, winner_outcome):
    started = asyncio.Event()
    contender_by_field = {"pid": None}
    contender_task = None
    try:
        contender_task = asyncio.create_task(_contend_identity_insert(sessions, started, contender_by_field))
        await asyncio.wait_for(started.wait(), 3)
        await asyncio.wait_for(
            _wait_for_identity_block(winner, winner_pid, contender_by_field, contender_task), 3
        )
        if winner_outcome == "rollback":
            await winner.rollback()
        else:
            await winner.commit()
        await _assert_identity_contender(contender_task, winner_outcome)
    finally:
        await winner.rollback()
        if contender_task is not None:
            if not contender_task.done():
                contender_task.cancel()
            await asyncio.wait_for(asyncio.gather(contender_task, return_exceptions=True), 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("winner_outcome", ["compatible_commit", "conflicting_commit", "rollback"])
async def test_identity_binder_reads_a_concurrent_insert_after_its_statement(monkeypatch, winner_outcome):
    """The contender must observe the winner after its INSERT waits on the exact owned key."""
    expected = source_resource_uuid("cms-npd", "InsurancePlan", "concurrent-plan")
    winner_entity = UUID("00000000-0000-0000-0000-000000000099")
    if winner_outcome == "compatible_commit":
        winner_entity = expected
    async with cms_database(monkeypatch) as sessions:
        async with sessions() as winner:
            await winner.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            winner_pid = await winner.scalar(text("SELECT pg_catalog.pg_backend_pid()"))
            await winner.execute(
                text(
                    "INSERT INTO provider_directory_resource_identity VALUES "
                    "('cms-npd','InsurancePlan','concurrent-plan',:entity_id,now())"
                ),
                {"entity_id": winner_entity},
            )
            await _run_identity_contender(sessions, winner, winner_pid, winner_outcome)
        if winner_outcome == "conflicting_commit":
            async with sessions() as session:
                assert await session.scalar(
                    text(
                        "SELECT entity_id FROM provider_directory_resource_identity "
                        "WHERE source_id='cms-npd' AND resource_type='InsurancePlan' "
                        "AND resource_id='fresh-during-race'"
                    )
                ) is None


@pytest.mark.asyncio
async def test_resource_identity_binder_accepts_one_thousand_ids(monkeypatch):
    resource_ids = [f"role-{index}" for index in range(1_000)]
    async with cms_database(monkeypatch) as sessions:
        async with sessions() as session, session.begin():
            first = await bind_resource_identity_batch(
                session, source_id="cms-npd", resource_type="PractitionerRole", resource_ids=resource_ids
            )
        async with sessions() as session, session.begin():
            assert (
                await bind_resource_identity_batch(
                    session, source_id="cms-npd", resource_type="PractitionerRole", resource_ids=resource_ids
                )
                == first
            )
    with pytest.raises(ValueError, match="batch_invalid"):
        await bind_resource_identity_batch(
            None, source_id="cms-npd", resource_type="PractitionerRole", resource_ids=resource_ids + ["extra"]
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
async def test_binder_rejects_existing_conflicting_identity(monkeypatch):
    async with cms_database(monkeypatch) as sessions:
        await change(
            sessions,
            "INSERT INTO provider_directory_resource_identity VALUES "
            "('cms-npd','InsurancePlan','conflicting-plan','00000000-0000-0000-0000-000000000099',now())",
        )
        async with sessions() as session:
            with pytest.raises(ValueError, match="identity_conflict"):
                await bind_resource_identity_batch(
                    session,
                    source_id="cms-npd",
                    resource_type="InsurancePlan",
                    resource_ids=["conflicting-plan", "fresh-with-conflict"],
                )
            await session.rollback()
            assert await session.scalar(
                text(
                    "SELECT entity_id FROM provider_directory_resource_identity "
                    "WHERE source_id='cms-npd' AND resource_type='InsurancePlan' "
                    "AND resource_id='fresh-with-conflict'"
                )
            ) is None


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
