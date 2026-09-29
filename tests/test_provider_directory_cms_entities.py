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
                "INSERT INTO provider_directory_entity_release_evidence VALUES "
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
        assert items[0]["target_id"] == ORG_ID
        assert items[2]["target_id"] == NETWORK_ID


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
                    session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["conflicting-plan"]
                )
            await session.rollback()


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
