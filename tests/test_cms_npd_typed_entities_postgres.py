# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public entity reads over synthetic typed CMS NPD releases in real PostgreSQL."""

import asyncio
import secrets
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api import cms_npd_typed_entities as serving
from api.cms_npd_typed_entities import read_cms_entities, read_cms_entities_snapshot
from api.cms_npd_typed_queries import current_cms_release
from api.provider_directory_entities_contract import DirectoryRead, DirectoryReadError
from tests.cms_npd_typed_postgres_support import (
    NET_DECLARED,
    NET_NESTED,
    NET_ORG,
    NPI,
    ORG_A,
    ORG_B,
    ORG_C,
    PLAN_1,
    PLAN_2,
    PLAN_3,
    ROLE_1,
    ROLE_2,
    ROLE_3,
    ROLE_4,
    SITE_1,
    SITE_2,
    load_release,
    network_entity,
    organization_entity,
    typed_cms_database,
)

ORG_A_ENTITY = organization_entity(ORG_A)
NETWORK = network_entity(NET_ORG)
NESTED_NETWORK = network_entity(NET_NESTED)
DECLARED_NETWORK = network_entity(NET_DECLARED)


def cms_query(kind="organizations", shape="entities", entity_id=None, limit=100):
    return DirectoryRead(kind, "cms-npd", shape, entity_id, None, limit)


async def read(sessions, query):
    async with sessions() as session:
        payload = await read_cms_entities(session, query)
        assert not session.in_transaction()
        return payload


async def read_all(sessions, query):
    """Follow every cursor; return all items and the number of pages."""
    items, pages, page = [], 0, await read(sessions, query)
    while True:
        items, pages = items + page["items"], pages + 1
        if page["next_cursor"] is None:
            return items, pages
        page = await read(sessions, replace(query, generation_id=page["generation_id"], cursor=page["next_cursor"]))


async def change(sessions, statement, parameters=None):
    async with sessions() as session, session.begin():
        await session.execute(text(statement), parameters or {})


def _summary(entity):
    return entity["display_name"], entity["status"], entity["effective_start"], entity["effective_end"]


EXPECTED_ENTITIES = {
    "organizations": {
        ORG_A_ENTITY: ("Example Health Organization", "active", None, None),
        organization_entity(ORG_B): ("Example Parent Organization", "active", None, None),
        organization_entity(ORG_C): (None, "inactive", None, None),
        organization_entity(NET_ORG): ("Example Network", "active", None, None),
        organization_entity(NET_NESTED): ("Example Nested Network", "active", None, None),
        organization_entity(NET_DECLARED): ("Example Declared Network", "active", None, None),
    },
    "sites": {
        str(SITE_1): ("Example Clinic", "active", None, None),
        str(SITE_2): ("Example Annex", "unknown", None, None),
    },
    "plans": {
        str(PLAN_1): ("Example Plan", "active", "2026-01-01", "2026-12-31"),
        str(PLAN_2): ("Example Draft Plan", "unknown", None, None),
        str(PLAN_3): ("Example Nested Plan", "active", "2026-01-01", "2026-12-31"),
    },
    "practitioner-roles": {
        str(ROLE_1): (None, "active", "2026-02-01", None),
        str(ROLE_2): (None, "inactive", None, None),
        str(ROLE_3): (None, "active", "2026-04-01", "2026-05-31"),
        str(ROLE_4): (None, "active", None, None),
    },
    "networks": {
        NETWORK: ("Example Network", "active", None, None),
        NESTED_NETWORK: ("Example Nested Network", "active", None, None),
        DECLARED_NETWORK: ("Example Declared Network", "active", None, None),
    },
}


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", sorted(EXPECTED_ENTITIES))
async def test_every_kind_lists_pages_and_details(monkeypatch, kind):
    async with typed_cms_database(monkeypatch) as sessions:
        page = await read(sessions, cms_query(kind))
        expected = EXPECTED_ENTITIES[kind]
        assert [entity["id"] for entity in page["items"]] == sorted(expected)
        assert {entity["id"]: _summary(entity) for entity in page["items"]} == expected
        assert page["next_cursor"] is None and page["generation_id"].startswith("gen_")
        assert all(entity["kind"] == kind and entity["source_id"] == "cms-npd" for entity in page["items"])
        items, pages = await read_all(sessions, cms_query(kind, limit=1))
        assert items == page["items"] and pages == len(expected)
        entity_id = page["items"][0]["id"]
        detail = await read(sessions, cms_query(kind, "entity", entity_id))
        assert detail["item"] == page["items"][0]
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query(kind, "entity", str(uuid4())))
        assert caught.value.status == 404


@pytest.mark.asyncio
async def test_network_evidence_names_its_first_plan(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        network = (await read(sessions, cms_query("networks", "entity", NETWORK)))["item"]
        assert [evidence["resource_type"] for evidence in network["evidence"]] == ["Organization", "InsurancePlan"]
        plan = (await read(sessions, cms_query("plans", "entity", str(PLAN_1))))["item"]
        assert network["evidence"][1]["record_key"] == plan["evidence"][0]["record_key"]


EXPECTED_RELATIONSHIPS = {
    ("organizations", ORG_A_ENTITY): [("organization-part-of", "organizations", organization_entity(ORG_B))],
    ("organizations", organization_entity(ORG_B)): [("organization-part-of", "organizations", None)],
    ("sites", str(SITE_1)): [("site-managing-organization", "organizations", ORG_A_ENTITY)],
    ("sites", str(SITE_2)): [
        ("site-managing-organization", "organizations", None),
        ("site-part-of", "sites", str(SITE_1)),
    ],
    ("plans", str(PLAN_1)): [
        ("plan-owned-by", "organizations", ORG_A_ENTITY),
        ("plan-network", "networks", NETWORK),
        ("plan-network", "networks", None),
    ],
    ("plans", str(PLAN_2)): [("plan-network", "networks", NETWORK)],
    ("plans", str(PLAN_3)): [
        ("plan-administered-by", "organizations", ORG_A_ENTITY),
        ("plan-coverage-area", "sites", str(SITE_1)),
        ("plan-coverage-area", "sites", None),
        ("plan-network", "networks", NESTED_NETWORK),
        ("plan-coverage-area", "sites", str(SITE_2)),
        ("plan-network", "networks", None),
    ],
    ("practitioner-roles", str(ROLE_1)): [
        ("role-practitioner", "providers", str(NPI)),
        ("role-organization", "organizations", ORG_A_ENTITY),
        ("role-site", "sites", str(SITE_1)),
        ("role-site", "sites", None),
        ("role-network", "networks", NETWORK),
    ],
    ("practitioner-roles", str(ROLE_2)): [("role-practitioner", "providers", None)],
    ("practitioner-roles", str(ROLE_3)): [
        ("role-practitioner", "providers", None),
        ("role-network", "networks", NETWORK),
        ("role-network", "networks", None),
        ("role-plan", "plans", str(PLAN_1)),
        ("role-plan", "plans", None),
    ],
    ("practitioner-roles", str(ROLE_4)): [("role-practitioner", "providers", None)],
    ("networks", NETWORK): [("network-plan", "plans", str(PLAN_1)), ("network-plan", "plans", str(PLAN_2))],
    ("networks", NESTED_NETWORK): [("network-plan", "plans", str(PLAN_3))],
    ("networks", DECLARED_NETWORK): [],
    ("organizations", organization_entity(ORG_C)): [],
}


# Nested plan references carry their parent element's period; every other link carries its parent's.
NESTED_PERIODS = {
    NESTED_NETWORK: ("2026-03-01", "2026-06-30"),
    str(SITE_2): ("2026-03-01", "2026-06-30"),
    None: ("2026-01-01", "2026-12-31"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,entity_id", sorted(EXPECTED_RELATIONSHIPS))
async def test_relationships_resolve_targets_in_the_release(monkeypatch, kind, entity_id):
    async with typed_cms_database(monkeypatch) as sessions:
        page = await read(sessions, cms_query(kind, "relationships", entity_id))
        parent = (await read(sessions, cms_query(kind, "entity", entity_id)))["item"]
        found_links = [(link["relationship_type"], link["target_kind"], link["target_id"]) for link in page["items"]]
        assert sorted(found_links, key=str) == sorted(EXPECTED_RELATIONSHIPS[kind, entity_id], key=str)
        for link in page["items"]:
            assert link["status"] == ("resolved" if link["target_id"] is not None else "unresolved")
            expected_period = (
                (None, None) if kind == "networks" else (parent["effective_start"], parent["effective_end"])
            )
            if entity_id == str(PLAN_3) and link["relationship_type"] != "plan-administered-by":
                expected_period = NESTED_PERIODS.get(link["target_id"], expected_period)
            assert (link["effective_start"], link["effective_end"]) == expected_period
            if link["target_kind"] == "providers" and link["target_id"] is not None:
                assert link["provider_npi"] == str(NPI)
                assert link["provider_profile_path"] == f"/api/v1/providers/{NPI}/profile"
            else:
                assert "provider_npi" not in link


@pytest.mark.asyncio
async def test_relationship_int_cursor_pages_without_duplicates(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        query = cms_query("practitioner-roles", "relationships", str(ROLE_1))
        everything = (await read(sessions, query))["items"]
        items, pages = await read_all(sessions, replace(query, limit=2))
        assert len(everything) == 5 and pages == 3
        assert sorted(item["relationship_key"] for item in items) == sorted(
            item["relationship_key"] for item in everything
        )


@pytest.mark.asyncio
async def test_plan_relationships_page_in_source_field_order(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        items, pages = await read_all(sessions, cms_query("plans", "relationships", str(PLAN_3), limit=1))
        assert pages == 6
        assert [(item["relationship_type"], item["target_id"]) for item in items] == [
            ("plan-administered-by", ORG_A_ENTITY),
            ("plan-coverage-area", str(SITE_1)),
            ("plan-coverage-area", None),
            ("plan-network", NESTED_NETWORK),
            ("plan-coverage-area", str(SITE_2)),
            ("plan-network", None),
        ]


@pytest.mark.asyncio
async def test_network_relationship_uuid_cursor(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        items, pages = await read_all(sessions, cms_query("networks", "relationships", NETWORK, limit=1))
        assert [item["target_id"] for item in items] == [str(PLAN_1), str(PLAN_2)] and pages == 2


@pytest.mark.asyncio
async def test_no_current_release_or_missing_release_table_is_unavailable(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        await change(sessions, "UPDATE cms_npd_release SET status='previous'")
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, cms_query())
        assert caught.value.status == 503
        await change(sessions, "ALTER TABLE cms_npd_release RENAME TO cms_npd_release_moved")
        try:
            with pytest.raises(SQLAlchemyError):
                await read(sessions, cms_query())
        finally:
            await change(sessions, "ALTER TABLE cms_npd_release_moved RENAME TO cms_npd_release")


@pytest.mark.asyncio
async def test_unsafe_release_schema_name_is_never_interpolated(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        await change(sessions, "ALTER TABLE cms_npd_release DROP CONSTRAINT cms_npd_release_schema_name_check")
        original = await _current_schema(sessions)
        await change(sessions, """UPDATE cms_npd_release SET schema_name='cms_npd_0000000000000000"; SELECT 1; --'""")
        try:
            with pytest.raises(DirectoryReadError) as caught:
                await read(sessions, cms_query())
            assert caught.value.status == 503
        finally:
            await change(sessions, "UPDATE cms_npd_release SET schema_name=:schema", {"schema": original})


async def _current_schema(sessions):
    async with sessions() as session:
        return await session.scalar(text("SELECT schema_name FROM cms_npd_release WHERE status='current'"))


@pytest.mark.asyncio
@pytest.mark.parametrize("schema_name", ["public", "cms_npd_ABCDEF0123456789", "cms_npd_0123456789abcdef\n", None])
async def test_release_schema_pattern_rejects_other_names(schema_name):
    row_by_column = {"release_id": "c" * 64, "schema_name": schema_name, "published_at": datetime.now(timezone.utc)}
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [row_by_column]))

    async def execute(_statement):
        return result

    with pytest.raises(DirectoryReadError) as caught:
        await current_cms_release(SimpleNamespace(execute=execute), b"k" * 32)
    assert caught.value.status == 503


@pytest.mark.asyncio
async def test_pointer_switch_invalidates_generation_and_cursor(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        query = cms_query(limit=1)
        first = await read(sessions, query)
        async with sessions() as session, session.begin():
            await session.execute(text("UPDATE cms_npd_release SET status='previous'"))
            await load_release(session, secrets.token_hex(32))
        with pytest.raises(DirectoryReadError) as caught:
            await read(sessions, replace(query, generation_id=first["generation_id"], cursor=first["next_cursor"]))
        assert caught.value.status == 409
        fresh = await read(sessions, query)
        assert fresh["generation_id"] != first["generation_id"]
        assert fresh["items"][0]["evidence"][0]["release_id"] != first["items"][0]["evidence"][0]["release_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", [read_cms_entities, read_cms_entities_snapshot])
async def test_network_scoped_reads_fail_closed_before_any_query(reader):
    query = replace(cms_query(), network_ids=(7,))

    async def refuse(*_args, **_kwargs):
        raise AssertionError("no statement may run")

    with pytest.raises(DirectoryReadError) as caught:
        await reader(SimpleNamespace(execute=refuse, in_transaction=lambda: True), query)
    assert caught.value.status == 503


@pytest.mark.asyncio
async def test_snapshot_reader_uses_the_callers_transaction(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        async with sessions() as session, session.begin():
            page = await read_cms_entities_snapshot(session, cms_query("sites"))
        assert [entity["id"] for entity in page["items"]] == sorted(EXPECTED_ENTITIES["sites"])


@pytest.mark.asyncio
async def test_cancelled_read_releases_its_snapshot(monkeypatch):
    async def cancel_read(*_args, **_kwargs):
        raise asyncio.CancelledError

    async with typed_cms_database(monkeypatch) as sessions:
        monkeypatch.setattr(serving, "cms_entity_rows", cancel_read)
        async with sessions() as session:
            with pytest.raises(asyncio.CancelledError):
                await read_cms_entities(session, cms_query())
            assert not session.in_transaction()
            assert await session.scalar(text("SELECT 1")) == 1
