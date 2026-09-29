# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native database proofs for accepted medical-group reads and failure cleanup."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from api import provider_directory_medical_groups as serving
from api.endpoint import provider_directory_entities as endpoint
from api.provider_directory_entities_contract import DirectoryReadError, parse_directory_read
from tests.provider_directory_entities_postgres_support import (
    GROUP_A,
    GROUP_B,
    SITE_A,
    SITE_B,
    SITE_D,
    directory_database,
)
from tests.test_provider_directory_entities import directory_query


async def _read(sessions, query):
    async with sessions() as session:
        result = await serving.read_medical_groups(session, query)
        assert not session.in_transaction()
        return result


async def _change(sessions, statement):
    async with sessions() as session, session.begin():
        await session.execute(text(statement))


@pytest.mark.asyncio
async def test_accepted_groups_page_by_stable_uuid_without_candidate_or_source_leaks(monkeypatch):
    async with directory_database(monkeypatch) as sessions:
        query = directory_query("source_id=cms-doctors&limit=1")
        first = await _read(sessions, query)
        assert [item["id"] for item in first["items"]] == [GROUP_A]
        assert first["items"][0]["status"] == "conflict"
        assert first["items"][0]["display_name"] is None
        second_query = replace(query, generation_id=first["generation_id"], cursor=first["next_cursor"])
        second = await _read(sessions, second_query)
        assert [item["id"] for item in second["items"]] == [GROUP_B]
        assert second["items"][0]["display_name"] == "Second Group"
        assert second["next_cursor"] is None
        assert second == await _read(sessions, second_query)
        evidence = second["items"][0]["evidence"][0]
        assert evidence["record_key"].startswith("src_") and len(evidence["record_key"]) == 68
        assert evidence["observed_at"].endswith("+00:00")
        encoded = json.dumps([first, second])
        assert "synthetic-pac" not in encoded
        assert "synthetic-release" not in encoded
        assert "synthetic-address" not in encoded
        assert "unpublished" not in encoded


@pytest.mark.asyncio
async def test_detail_and_relationships_resolve_exact_source_site_bindings(monkeypatch):
    async with directory_database(monkeypatch) as sessions:
        detail_query = directory_query("source_id=cms-doctors", shape="entity", entity_id=GROUP_A)
        detail = await _read(sessions, detail_query)
        assert detail["item"]["id"] == GROUP_A
        query = directory_query(shape="relationships", entity_id=GROUP_A)
        first = await _read(sessions, query)
        second = await _read(
            sessions, replace(query, generation_id=first["generation_id"], cursor=first["next_cursor"])
        )
        assert second["next_cursor"] is None
        relationship_items = first["items"] + second["items"]
        assert len(relationship_items) == 3
        assert len({relationship["relationship_key"] for relationship in relationship_items}) == 3
        assert first["items"] == sorted(first["items"], key=lambda relationship: relationship["relationship_key"])
        assert {relationship["target_id"] for relationship in relationship_items} == {SITE_A, SITE_B, SITE_D}
        assert "synthetic-address" not in json.dumps(relationship_items)
        for relationship in relationship_items:
            assert relationship["target_kind"] == "sites" and relationship["target_id"] is not None
            assert relationship["status"] == "resolved" and relationship["relationship_type"] == "group-site"
            assert relationship["effective_start"] is None and relationship["effective_end"] is None
            assert relationship["provider_npi"] == "1234567893"
            assert relationship["provider_profile_path"] == "/api/v1/providers/1234567893/profile"
            site = await _read(sessions, serving_query("sites", relationship["target_id"]))
            assert site["item"]["id"] == relationship["target_id"]
            assert site["item"]["kind"] == "sites"
            assert site["item"]["source_id"] == "cms-doctors"
            assert site["item"]["evidence"][0]["resource_type"] == "CMSDoctorsSite"
        async with sessions() as session:
            request = SimpleNamespace(query_string="source_id=cms-doctors", ctx=SimpleNamespace(sa_session=session))
            routed = await endpoint.entity_detail(request, "sites", SITE_A)
            assert routed.status == 200
            assert json.loads(routed.body)["item"]["id"] == SITE_A
        site_links = await _read(
            sessions, parse_directory_read("sites", SITE_A, "relationships", "source_id=cms-doctors")
        )
        assert len(site_links["items"]) == 1
        assert site_links["items"][0]["target_id"] == GROUP_A
        assert site_links["items"][0]["relationship_type"] == "site-group"
        assert site_links["items"][0]["provider_npi"] == "1234567893"


def serving_query(kind, entity_id):
    return parse_directory_read(kind, entity_id, "entity", "source_id=cms-doctors")


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["entity", "relationships"])
async def test_missing_group_is_not_found_even_if_candidate_binding_exists(monkeypatch, shape):
    async with directory_database(monkeypatch) as sessions:
        query = directory_query("source_id=cms-doctors", shape=shape, entity_id="00000000-0000-0000-0000-000000000003")
        with pytest.raises(DirectoryReadError) as caught:
            await _read(sessions, query)
        assert caught.value.status == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "DELETE FROM reference_family_result_generation",
        "UPDATE reference_family_result_generation SET origin_lineage_id = NULL",
        "UPDATE reference_family_result_generation SET origin_generation = 0",
        "UPDATE reference_family_result_generation SET published_at = NULL",
        "UPDATE reference_family_result_generation SET relation_oids = relation_oids[1:2]",
        "UPDATE reference_family_result_generation SET relation_oids[3] = 1",
        "DELETE FROM provider_directory_cms_doctors_group_binding WHERE org_pac_id = 'synthetic-pac-beta'",
        "DELETE FROM provider_directory_cms_doctors_site_binding WHERE adrs_id = 'synthetic-address-beta'",
    ],
)
async def test_unaccepted_or_incompletely_bound_source_never_returns_partial_page(monkeypatch, mutation):
    async with directory_database(monkeypatch) as sessions:
        await _change(sessions, mutation)
        with pytest.raises(DirectoryReadError) as caught:
            await _read(sessions, directory_query())
        assert caught.value.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("generation_change", ["local_generation = 2, origin_generation = 2", "local_generation = 2"])
async def test_publication_or_restore_invalidates_pinned_generation(monkeypatch, generation_change):
    async with directory_database(monkeypatch) as sessions:
        query = directory_query("source_id=cms-doctors&limit=1")
        first = await _read(sessions, query)
        await _change(sessions, "UPDATE reference_family_result_generation SET " + generation_change)
        with pytest.raises(DirectoryReadError) as caught:
            await _read(sessions, replace(query, generation_id=first["generation_id"], cursor=first["next_cursor"]))
        assert caught.value.status == 409
        restarted = await _read(sessions, query)
        assert restarted["generation_id"] != first["generation_id"]
        assert restarted["items"] == first["items"]


@pytest.mark.asyncio
async def test_missing_dependency_returns_sanitized_unavailable(monkeypatch):
    async with directory_database(monkeypatch) as sessions:
        await _change(sessions, "DROP TABLE cms_doctor_group_site")
        async with sessions() as session:
            request = SimpleNamespace(query_string="source_id=cms-doctors", ctx=SimpleNamespace(sa_session=session))
            result = await endpoint.list_entities(request, "medical-groups")
            assert result.status == 503
            assert b"cms_doctor_group_site" not in result.body
            assert not session.in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"source_id": "unaccepted-source"}, {"kind": "organizations"}])
async def test_unaccepted_sources_and_other_entity_kinds_fail_closed(monkeypatch, changes):
    async with directory_database(monkeypatch) as sessions:
        with pytest.raises(DirectoryReadError) as caught:
            await _read(sessions, replace(directory_query(), **changes))
        assert caught.value.status == 503


@pytest.mark.asyncio
async def test_cancellation_releases_snapshot_and_relation_locks(monkeypatch):
    async with directory_database(monkeypatch) as sessions:
        reached_page = asyncio.Event()

        async def pause_page(session, *_args):
            assert (await session.execute(text("SHOW transaction_read_only"))).scalar_one() == "on"
            reached_page.set()
            await asyncio.Future()

        monkeypatch.setattr(serving, "_read_page", pause_page)
        async with sessions() as session:
            task = asyncio.create_task(serving.read_medical_groups(session, directory_query()))
            await asyncio.wait_for(reached_page.wait(), timeout=5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not session.in_transaction()
            await _change(sessions, "LOCK TABLE cms_doctor_group_site IN ACCESS EXCLUSIVE MODE NOWAIT")


@pytest.mark.asyncio
async def test_cutover_lock_contention_fails_closed_and_releases_transaction(monkeypatch):
    async with directory_database(monkeypatch) as sessions:
        async with sessions() as writer, writer.begin():
            await writer.execute(text("LOCK TABLE cms_doctor_group_site IN ACCESS EXCLUSIVE MODE"))
            async with sessions() as reader:
                request = SimpleNamespace(query_string="source_id=cms-doctors", ctx=SimpleNamespace(sa_session=reader))
                result = await asyncio.wait_for(endpoint.list_entities(request, "medical-groups"), timeout=3)
                assert result.status == 503
                assert not reader.in_transaction()
        assert len((await _read(sessions, directory_query()))["items"]) == 2


@pytest.mark.asyncio
async def test_relation_replacement_without_matching_authority_is_unavailable(monkeypatch):
    async with directory_database(monkeypatch) as sessions:
        await _change(sessions, "ALTER TABLE cms_doctor_group_site RENAME TO former_group_site")
        await _change(sessions, "CREATE TABLE cms_doctor_group_site (LIKE former_group_site INCLUDING ALL)")
        with pytest.raises(DirectoryReadError) as caught:
            await _read(sessions, directory_query())
        assert caught.value.status == 503


@pytest.mark.asyncio
async def test_unicode_trimmed_name_satisfies_response_contract(monkeypatch):
    async with directory_database(monkeypatch) as sessions:
        await _change(
            sessions,
            "UPDATE cms_doctor_group_site SET facility_name = U&'\\00A0Second Group\\00A0' "
            "WHERE org_pac_id = 'synthetic-pac-beta'",
        )
        detail = await _read(sessions, directory_query("source_id=cms-doctors", shape="entity", entity_id=GROUP_B))
        assert detail["item"]["display_name"] == "Second Group"
