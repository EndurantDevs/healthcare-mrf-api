# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only confirmed existing payer identities cross the public entity boundary."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import text

from api.provider_directory_entities_contract import DirectoryReadError, parse_directory_read
from tests.provider_directory_cms_postgres_support import ORG_ID, cms_database, publication_summary, reseal
from tests.test_provider_directory_cms_entities import cms_query, read


async def _review_payer(sessions, payer_id="existing_payer-17"):
    decision_id = uuid4()
    async with sessions() as session, session.begin():
        await session.execute(text("INSERT INTO mrf_payer VALUES (:payer_id)"), {"payer_id": payer_id})
        await session.execute(
            text("""INSERT INTO provider_directory_mrf_payer_review_decision VALUES
            (:decision_id, 'bind', 'cms-npd', 'Organization', 'org-example', :payer_id, repeat('b',64), NULL)"""),
            {"decision_id": decision_id, "payer_id": payer_id},
        )
        await session.execute(
            text("""INSERT INTO provider_directory_mrf_payer_binding VALUES
            ('cms-npd', 'Organization', 'org-example', :payer_id, :decision_id)"""),
            {"payer_id": payer_id, "decision_id": decision_id},
        )
    return decision_id


@pytest.mark.asyncio
async def test_only_reviewed_existing_payer_has_detail_and_explicit_source_link(monkeypatch):
    async with cms_database(monkeypatch) as sessions:
        assert (await read(sessions, cms_query("payers")))["items"] == []
        await _review_payer(sessions)
        page = await read(sessions, cms_query("payers"))
        assert [entity["id"] for entity in page["items"]] == ["existing_payer-17"]
        detail = await read(sessions, cms_query("payers", "entity", "existing_payer-17"))
        assert detail["item"] == page["items"][0]
        relationships = await read(sessions, cms_query("payers", "relationships", "existing_payer-17"))
        assert len(relationships["items"]) == 1
        relationship = relationships["items"][0]
        assert relationship["target_id"] == ORG_ID
        assert relationship["relationship_type"] == "payer-source-organization"
        assert relationship["status"] == "resolved"


@pytest.mark.asyncio
async def test_review_closure_invalidates_generation_and_removes_payer(monkeypatch):
    async with cms_database(monkeypatch) as sessions:
        decision_id = await _review_payer(sessions)
        prior = await read(sessions, cms_query("payers"))
        async with sessions() as session, session.begin():
            await session.execute(
                text("""INSERT INTO provider_directory_mrf_payer_review_decision VALUES
                (:closure_id, 'close', 'cms-npd', 'Organization', 'org-example',
                 'existing_payer-17', repeat('b',64), :decision_id)"""),
                {"closure_id": uuid4(), "decision_id": decision_id},
            )
            await session.execute(text("DELETE FROM provider_directory_mrf_payer_binding"))
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, replace(cms_query("payers"), generation_id=prior["generation_id"]))
        assert caught.value.status == 409
        assert (await read(sessions, cms_query("payers")))["items"] == []


@pytest.mark.asyncio
async def test_changed_source_facts_require_matching_review(monkeypatch):
    async with cms_database(monkeypatch) as sessions:
        await _review_payer(sessions)
        async with sessions() as session, session.begin():
            await session.execute(
                text("UPDATE provider_directory_mrf_payer_review_decision SET source_payload_sha256=repeat('d',64)")
            )
        assert (await read(sessions, cms_query("payers")))["items"] == []
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query("payers", "entity", "existing_payer-17"))
        assert caught.value.status == 404


def test_payer_id_contract_accepts_existing_opaque_identity_only_for_payers():
    query = parse_directory_read("payers", "existing_payer-17", "entity", "source_id=cms-npd")
    assert query.entity_id == "existing_payer-17"
    with pytest.raises(DirectoryReadError):
        parse_directory_read("organizations", "existing_payer-17", "entity", "source_id=cms-npd")
    with pytest.raises(DirectoryReadError):
        parse_directory_read("payers", "invalid/payer", "entity", "source_id=cms-npd")


async def _add_second_payer(session):
    """Add a second accepted organization and independently reviewed existing payer."""
    await session.execute(
        text("""INSERT INTO provider_directory_dataset_resource
            SELECT dataset_id, resource_type, 'second-org', payload_hash, payload_json, acquired_resource_sha256
            FROM provider_directory_dataset_resource WHERE resource_type='Organization'""")
    )
    await session.execute(
        text("""INSERT INTO provider_directory_entity_source_binding VALUES
            ('cms-npd','Organization','second-org','00000000-0000-0000-0000-000000000011',NULL)""")
    )
    await session.execute(
        text("""INSERT INTO provider_directory_entity_release_evidence
            SELECT source_id, resource_type, 'second-org', release_id, payload_sha256
            FROM provider_directory_entity_release_evidence WHERE resource_type='Organization'""")
    )
    await session.execute(text("INSERT INTO mrf_payer VALUES ('A_existing_payer')"))
    decision_id = uuid4()
    await session.execute(
        text("""INSERT INTO provider_directory_mrf_payer_review_decision VALUES
            (:decision_id,'bind','cms-npd','Organization','second-org','A_existing_payer',repeat('b',64),NULL)"""),
        {"decision_id": decision_id},
    )
    await session.execute(
        text("""INSERT INTO provider_directory_mrf_payer_binding VALUES
            ('cms-npd','Organization','second-org','A_existing_payer',:decision_id)"""),
        {"decision_id": decision_id},
    )
    summary = publication_summary()
    summary["source_release"]["files"]["01-Organization.ndjson"].update(row_count=2, distinct_count=2)
    proof = summary["provider_directory_content_proof_admission_summary_v1"]
    proof["resource_counts"]["Organization"] = 2
    proof["resource_count"] = 9
    await session.execute(
        text(
            "UPDATE provider_directory_endpoint_dataset SET resource_count=9, "
            "publication_metadata_summary_json=CAST(:summary AS jsonb)"
        ),
        {"summary": json.dumps(summary)},
    )
    await reseal(session)


@pytest.mark.asyncio
async def test_payer_cursor_preserves_opaque_ids_and_binary_order(monkeypatch):
    async with cms_database(monkeypatch, prepare=_add_second_payer) as sessions:
        await _review_payer(sessions)
        query = cms_query("payers")
        first = await read(sessions, query)
        second = await read(sessions, replace(query, generation_id=first["generation_id"], cursor=first["next_cursor"]))
        assert first["items"][0]["id"] == "A_existing_payer"
        assert second["items"][0]["id"] == "existing_payer-17"
        assert second["next_cursor"] is None
