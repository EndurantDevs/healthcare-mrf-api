# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed CMS NPD entity reads against a scripted session: SQL, binds, cursors and fail-closed branches."""

import base64
import json
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from api import cms_npd_typed_entities as entities
from api.cms_npd_typed_queries import current_cms_release
from api.endpoint import provider_directory_entities as endpoint
from api.provider_directory_entities_contract import CURSOR_KEY_ENV, DirectoryReadError, parse_directory_read

KEY = bytes(range(32))
SCHEMA = "cms_npd_" + "a" * 16
PUBLISHED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
RELEASE_ROW = {"release_id": "r" * 64, "schema_name": SCHEMA, "published_at": PUBLISHED}
ORG_1 = "00000000-0000-4000-8000-000000000001"
ORG_2 = "00000000-0000-4000-8000-000000000002"
ORG_3 = "00000000-0000-4000-8000-000000000003"
SET_STATEMENTS = [
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
    "SET LOCAL statement_timeout = '2s'",
    "SET LOCAL lock_timeout = '250ms'",
    "SET LOCAL work_mem = '4MB'",
]


@pytest.fixture(autouse=True)
def cursor_key(monkeypatch):
    monkeypatch.setenv(CURSOR_KEY_ENV, base64.urlsafe_b64encode(KEY).decode().rstrip("="))
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def scalar_one(self):
        return self.rows


class FakeSession:
    """Answers each statement from the first route whose SQL fragment it contains; records every call."""

    def __init__(self, routes, *, in_transaction=False, fail_with=None):
        self.routes, self.open, self.fail_with = list(routes), in_transaction, fail_with
        self.calls, self.events = [], []

    def in_transaction(self):
        return self.open

    def begin(self):
        session = self

        class _Begin:
            async def __aenter__(self):
                session.events.append("BEGIN")

            async def __aexit__(self, error_type, *_):
                session.events.append("COMMIT" if error_type is None else "ROLLBACK")

        return _Begin()

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if self.fail_with is not None:
            raise self.fail_with
        return _Result(next((rows for fragment, rows in self.routes if fragment in sql), []))

    def data_calls(self):
        return [(sql, parameters) for sql, parameters in self.calls if not sql.startswith("SET ")]


def entity_row(entity_id, **overrides):
    return {
        "entity_id": entity_id,
        "resource_type": "Organization",
        "resource_id": f"org-{entity_id[-1]}",
        "name": "Example Organization",
        "active": "true",
        "status": None,
        "period_start": None,
        "period_end": None,
        "plan_evidence": None,
        **overrides,
    }


def cms_query(kind, query="source_id=cms-npd&limit=2", shape="entities", entity_id=None):
    return parse_directory_read(kind, entity_id, shape, query)


def next_page(query, page):
    selector = (
        f"source_id=cms-npd&limit={query.limit}&generation_id={page['generation_id']}&cursor={page['next_cursor']}"
    )
    return cms_query(query.kind, selector, query.shape, query.entity_id)


async def test_list_reads_one_snapshot_and_resumes_from_its_cursor():
    organization_rows = [entity_row(ORG_1), entity_row(ORG_2), entity_row(ORG_3)]
    session = FakeSession([("cms_npd_release", [RELEASE_ROW]), ("organization t", organization_rows)])
    query = cms_query("organizations")
    page = await entities.read_cms_entities(session, query)
    assert session.events == ["BEGIN", "COMMIT"]
    assert [sql for sql, _ in session.calls[:4]] == SET_STATEMENTS
    release_sql, (entity_sql, binds) = session.data_calls()[0][0], session.data_calls()[1]
    assert 'FROM "mrf".cms_npd_release' in release_sql and "status='current' LIMIT 2" in release_sql
    assert f'FROM "{SCHEMA}".organization t' in entity_sql and "ORDER BY t.entity_id LIMIT :page_size" in entity_sql
    assert binds == {"entity_id": None, "position": None, "page_size": 3}
    assert [item["id"] for item in page["items"]] == [ORG_1, ORG_2]
    assert page["generation_id"].startswith("gen_") and page["next_cursor"]
    first = page["items"][0]
    assert {field: first[field] for field in ("kind", "source_id", "display_name", "status")} == {
        "kind": "organizations",
        "source_id": "cms-npd",
        "display_name": "Example Organization",
        "status": "active",
    }
    assert first["evidence"][0]["observed_at"] == PUBLISHED.isoformat()
    assert first["evidence"][0]["record_key"].startswith("src_") and "org-1" not in json.dumps(first)

    second = FakeSession([("cms_npd_release", [RELEASE_ROW]), ("organization t", organization_rows[2:])])
    last = await entities.read_cms_entities(second, next_page(query, page))
    assert second.data_calls()[1][1]["position"] == ORG_2
    assert [item["id"] for item in last["items"]] == [ORG_3] and last["next_cursor"] is None


@pytest.mark.parametrize(
    ("overrides", "status"),
    [
        ({"status": "active", "active": None}, "active"),
        ({"status": "weird", "active": "false"}, "inactive"),
        ({"status": "conflict", "active": "true"}, "conflict"),
        ({"status": None, "active": None}, "unknown"),
    ],
)
def test_entity_status_prefers_the_active_flag_except_for_conflicts(overrides, status):
    generation_by_field = {"release_id": "r", "observed_at": None}
    assert entities._entity(KEY, generation_by_field, "sites", entity_row(ORG_1, **overrides))["status"] == status


def test_entity_evidence_names_the_plan_and_treats_naive_times_as_utc():
    generation_by_field = {"release_id": "r", "observed_at": datetime(2026, 1, 2)}
    network = entity_row(ORG_1, resource_type="Organization", plan_evidence="plan-1", period_start="2026-02-01")
    item = entities._entity(KEY, generation_by_field, "networks", network)
    assert [entry["resource_type"] for entry in item["evidence"]] == ["Organization", "InsurancePlan"]
    assert item["evidence"][0]["observed_at"] == "2026-01-02T00:00:00+00:00"
    assert (item["effective_start"], item["effective_end"]) == ("2026-02-01", None)


@pytest.mark.parametrize(("name", "expected"), [(5, None), ("  ", None), ("x" * 2049, None), ("Bad\x07", None)])
def test_unsafe_display_names_are_withheld(name, expected):
    assert entities._safe_name(name) is expected
    assert entities._safe_name("  Example  ") == "Example"


@pytest.mark.parametrize("raw", [None, "2026-13-01", "2026-01-02T00:00", "20260102"])
def test_effective_dates_must_be_plain_iso_dates(raw):
    assert entities._effective_date(raw) is None
    assert entities._effective_date("2026-01-02") == "2026-01-02"


@pytest.mark.parametrize(
    "query",
    [
        replace(cms_query("organizations"), excluded_network_ids=(1,)),
        cms_query("organizations", "source_id=cms-doctors"),
        cms_query("medical-groups", "source_id=cms-npd"),
        cms_query("payers"),
    ],
)
@pytest.mark.parametrize("reader", [entities.read_cms_entities, entities.read_cms_entities_snapshot])
async def test_unsupported_scopes_fail_closed_before_any_statement(query, reader):
    session = FakeSession([], in_transaction=reader is entities.read_cms_entities_snapshot)
    with pytest.raises(DirectoryReadError) as caught:
        await reader(session, query)
    assert caught.value.status == 503 and session.calls == []


@pytest.mark.parametrize("reader", [entities.read_cms_entities, entities.read_cms_entities_snapshot])
async def test_transaction_ownership_is_enforced(reader):
    session = FakeSession([], in_transaction=reader is entities.read_cms_entities)
    with pytest.raises(DirectoryReadError) as caught:
        await reader(session, cms_query("organizations"))
    assert caught.value.status == 503 and session.calls == []


async def test_statement_timeout_becomes_unavailable():
    session = FakeSession([], fail_with=TimeoutError())
    with pytest.raises(DirectoryReadError) as caught:
        await entities.read_cms_entities(session, cms_query("organizations"))
    assert caught.value.status == 503 and session.events == ["BEGIN", "ROLLBACK"]


async def test_snapshot_reader_uses_the_callers_transaction():
    session = FakeSession(
        [("cms_npd_release", [RELEASE_ROW]), ("location t", [entity_row(ORG_1)])], in_transaction=True
    )
    page = await entities.read_cms_entities_snapshot(session, cms_query("sites"))
    assert session.events == [] and not any(sql.startswith("SET ") for sql, _ in session.calls)
    assert f'FROM "{SCHEMA}".location t' in session.calls[1][0]
    assert [item["id"] for item in page["items"]] == [ORG_1] and page["next_cursor"] is None


async def test_stale_generation_is_a_conflict():
    session = FakeSession([("cms_npd_release", [RELEASE_ROW])])
    with pytest.raises(DirectoryReadError) as caught:
        await entities.read_cms_entities(session, cms_query("plans", "source_id=cms-npd&generation_id=gen_old"))
    assert caught.value.status == 409 and len(session.data_calls()) == 1


@pytest.mark.parametrize("shape", ["entity", "relationships"])
async def test_missing_exact_entity_is_not_found(shape):
    session = FakeSession([("cms_npd_release", [RELEASE_ROW])])
    with pytest.raises(DirectoryReadError) as caught:
        await entities.read_cms_entities(session, cms_query("plans", "source_id=cms-npd", shape, ORG_1))
    assert caught.value.status == 404
    assert session.data_calls()[1][1] == {"entity_id": ORG_1, "position": None, "page_size": 26}


async def test_detail_returns_one_item():
    plan = entity_row(ORG_1, resource_type="InsurancePlan", status="draft", active=None)
    session = FakeSession([("cms_npd_release", [RELEASE_ROW]), ("insurance_plan t", [plan])])
    detail = await entities.read_cms_entities(session, cms_query("plans", "source_id=cms-npd", "entity", ORG_1))
    assert set(detail) == {"generation_id", "item"}
    assert detail["item"]["id"] == ORG_1 and detail["item"]["status"] == "unknown"


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [RELEASE_ROW, RELEASE_ROW],
        [{**RELEASE_ROW, "schema_name": 'cms_npd_x"; DROP'}],
        [{**RELEASE_ROW, "schema_name": None}],
    ],
)
async def test_release_pointer_must_be_unique_and_well_formed(rows):
    with pytest.raises(DirectoryReadError) as caught:
        await current_cms_release(FakeSession([("cms_npd_release", rows)]), KEY)
    assert caught.value.status == 503


async def test_release_without_key_has_no_generation():
    release = await current_cms_release(FakeSession([("cms_npd_release", [RELEASE_ROW])]), None)
    assert release == {"release_id": "r" * 64, "schema": f'"{SCHEMA}"', "observed_at": PUBLISHED, "generation_id": None}


def edge(position, relationship_type, target_kind, target_id, **extra):
    return {
        "position": position,
        "relationship_type": relationship_type,
        "target_kind": target_kind,
        "target_id": target_id,
        **extra,
    }


async def test_role_relationships_resolve_providers_and_page_by_position():
    role = entity_row(ORG_1, resource_type="PractitionerRole", resource_id=ORG_1, period_start="2026-02-01")
    edges = [
        edge(1, "role-practitioner", "providers", "1234567893", period_start="2026-03-01", period_end=None),
        edge(2, "role-site", "sites", None),
        edge(3, "role-network", "networks", ORG_2),
    ]
    routes = [("cms_npd_release", [RELEASE_ROW]), ("row_number() OVER", edges), ("practitioner_role t", [role])]
    query = cms_query("practitioner-roles", "source_id=cms-npd&limit=2", "relationships", ORG_1)
    session = FakeSession(routes)
    page = await entities.read_cms_entities(session, query)
    edge_sql, binds = session.data_calls()[2]
    assert f'"{SCHEMA}".practitioner_role r' in edge_sql and "'role-practitioner'" in edge_sql
    assert "p.npi_explicit AND p.old_npis IS NULL" in edge_sql and "'role-plan'" in edge_sql
    assert binds == {"resource_id": ORG_1, "position": 0, "page_size": 3}
    by_type = {relation["relationship_type"]: relation for relation in page["items"]}
    provider, site = by_type["role-practitioner"], by_type["role-site"]
    assert provider["status"] == "resolved" and provider["provider_npi"] == "1234567893"
    assert provider["provider_profile_path"] == "/api/v1/providers/1234567893/profile"
    assert site["status"] == "unresolved" and "provider_npi" not in site
    assert provider["effective_start"] == "2026-03-01" and site["effective_start"] == "2026-02-01"
    assert provider["evidence"][0]["resource_type"] == "PractitionerRole"
    relationship_keys = [relation["relationship_key"] for relation in page["items"]]
    assert relationship_keys == sorted(relationship_keys) and len(relationship_keys) == 2

    second = FakeSession(routes)
    await entities.read_cms_entities(second, next_page(query, page))
    assert second.data_calls()[2][1]["position"] == 2


async def test_network_relationships_list_plans_with_plan_evidence():
    network = entity_row(ORG_1, resource_id="net-example", plan_evidence=ORG_2)
    plans = [edge(plan, "network-plan", "plans", plan, evidence_id=plan) for plan in (ORG_2, ORG_3)]
    routes = [("cms_npd_release", [RELEASE_ROW]), ("network-plan", plans), ("network n JOIN", [network])]
    query = cms_query("networks", "source_id=cms-npd&limit=1", "relationships", ORG_1)
    session = FakeSession(routes)
    page = await entities.read_cms_entities(session, query)
    plan_sql, binds = session.data_calls()[2]
    assert "p.network_ids @> ARRAY[CAST(:resource_id AS text)]" in plan_sql
    assert binds == {"resource_id": "net-example", "position": None, "page_size": 2}
    (plan_link,) = page["items"]
    assert plan_link["target_id"] == ORG_2 and plan_link["evidence"][0]["resource_type"] == "InsurancePlan"

    second = FakeSession(routes)
    await entities.read_cms_entities(second, next_page(query, page))
    assert second.data_calls()[2][1]["position"] == ORG_2


async def test_endpoint_sends_unscoped_cms_reads_to_the_typed_release(monkeypatch):
    calls = []

    async def typed_read(session, query):
        calls.append((session, query.kind, query.source_id))
        return {"generation_id": "gen_x", "items": [], "next_cursor": None}

    async def legacy_read(*_args, **_kwargs):
        raise AssertionError("legacy reader must not serve unscoped CMS reads")

    monkeypatch.setattr(endpoint, "read_typed_cms_entities", typed_read)
    monkeypatch.setattr(endpoint, "read_cms_entities", legacy_read)
    request = SimpleNamespace(query_string="source_id=cms-npd", ctx=SimpleNamespace(sa_session="session"))
    result = await endpoint.list_entities(request, "organizations")
    assert result.status == 200 and json.loads(result.body)["generation_id"] == "gen_x"
    assert calls == [("session", "organizations", "cms-npd")]


async def test_endpoint_keeps_unscoped_payer_reads_on_the_existing_reader(monkeypatch):
    calls = []

    async def existing_read(session, query, **kwargs):
        calls.append((session, query.kind, query.source_id, kwargs))
        return {"generation_id": "gen_payers", "items": [], "next_cursor": None}

    async def typed_read(*_args, **_kwargs):
        raise AssertionError("the typed reader does not serve payers")

    monkeypatch.setattr(endpoint, "read_typed_cms_entities", typed_read)
    monkeypatch.setattr(endpoint, "read_cms_entities", existing_read)
    request = SimpleNamespace(query_string="source_id=cms-npd", ctx=SimpleNamespace(sa_session="session"))
    result = await endpoint.list_entities(request, "payers")
    assert result.status == 200 and json.loads(result.body)["generation_id"] == "gen_payers"
    assert calls == [("session", "payers", "cms-npd", {})]
