# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""ZIP setup guards original-session results; Source authority leaves are doubles."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api import network_read_execution as execution
from api import plan_pricing_code_build as build
from api import plan_pricing_code_read as reads
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.network_read_identity import _BOUND_NAMES_SQL
from tests.test_plan_pricing_code_projection import _selection
from tests.test_plan_pricing_code_zip import _params, _scope, _zip_identity


class _Result:
    def __init__(self, session, value, *, metadata=False):
        self.session, self.value, self.closed = session, value, False
        self.before_guard, self.field_reads = session.guards, 0
        self.before_context, self.metadata = session.acquisitions, metadata
        self.scalars = lambda: SimpleNamespace(all=self._checked)
        self.mappings = self._checked

    def _checked(self):
        assert (
            self.session.guards > self.before_guard or self.metadata and self.session.acquisitions > self.before_context
        )
        self.field_reads += 1
        return self.value

    def scalar(self):
        return self._checked()

    scalar_one = scalar

    def all(self):
        return self._checked()

    def close(self):
        self.closed = True


class _Driver:
    def __init__(self, session):
        self.session, self.pid, self.active = session, 42, True
        self.modes = "repeatable read", "on"

    def get_server_pid(self):
        return self.pid

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, sql, *parameters):
        if "current_setting('transaction_isolation')" in sql:
            if self.session.on_modes is not None:
                self.session.on_modes(self.session)
            return self.modes
        assert sql == _BOUND_NAMES_SQL
        names, oids, pid = parameters
        self.session.guards += 1
        return {
            "backend_matches": self.pid == pid,
            "relation_count": len(names),
            "valid": all(self.session.current.get(name) == oid for name, oid in zip(names, oids)),
        }

    async def fetch(self, sql, schemas, names):
        assert "FROM ROWS FROM (pg_catalog.unnest(" in sql
        catalog_rows = []
        for ordinal, (schema, name) in enumerate(zip(schemas, names), 100):
            oid = None if name in self.session.absent else ordinal
            catalog_rows.append(
                {
                    "schema_name": schema,
                    "table_name": name,
                    "oid": oid,
                    "relkind": None if oid is None else self.session.kinds.get(name, "r"),
                    "inherited": name in self.session.inherited,
                    "readable": True,
                }
            )
            if oid is not None:
                self.session.current[f'"{schema}"."{name}"'] = oid
        if self.session.on_catalog is not None:
            self.session.on_catalog(self.session)
        return catalog_rows


class _Session:
    def __init__(self):
        self.info, self.current = {"unrelated": "preserved"}, {}
        self.transaction, self.driver, self.guards = object(), _Driver(self), 0
        self.acquisitions = 0
        self.absent, self.inherited, self.kinds = set(), set(), {}
        self.results, self.events, self.aggregate_rows = [], [], []
        self.local_source, self.attachment, self.on_query = False, True, None
        self.on_modes, self.on_catalog = None, None

    def in_transaction(self):
        return self.driver.active

    def get_transaction(self):
        return self.transaction

    async def connection(self):
        self.acquisitions += 1
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=self.driver))
        )

    async def scalar(self, statement, parameters=None):
        assert str(statement) == "SELECT to_regclass(:relation) IS NOT NULL"
        return self.attachment

    def _query_value(self, sql):
        if sql == "SELECT to_regclass(:relation) IS NOT NULL":
            return self.attachment, "attachment"
        if "coalesce(pg_catalog.bool_or(" in sql:
            return self.local_source, "local-refusal"
        if "'geo_assurance_ready'" in sql:
            value = json.dumps(
                {
                    **{name: [101, 201] for name in ("npi", "taxonomy", "vocabulary", "address", "zip")},
                    "address_evidence": [102, 202, 101, 201],
                    "geo_assurance_ready": True,
                }
            )
            return value, "provider"
        if "WITH center AS MATERIALIZED" in sql:
            return ["48105", "48104"], "geo"
        if "stored_byte_count, logical_digest, payload_sha256, payload" in sql:
            assert "FROM ONLY" in sql
            return self.aggregate_rows, "aggregate"
        if sql == "SELECT zip_api_code_rows":
            return [{"code_key": 1, "rate_count": 3}], "code"
        assert sql in {"SELECT zip_api_binding", "SELECT zip_api_identity"}
        return "guarded", sql.removeprefix("SELECT zip_api_")

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        assert "LOCK TABLE" not in sql and "WHERE false" not in sql and "FOR SHARE" not in sql
        value, event = self._query_value(sql)
        self.events.append(event)
        result = _Result(self, value, metadata=event == "attachment")
        self.results.append(result)
        if self.on_query is not None:
            self.on_query(event, self)
        return result


def _source_leaves(monkeypatch, session, *, failure=None):
    tables = SimpleNamespace()

    async def binding(reader, parameters, *, api_read):
        assert api_read is True
        assert type(reader) is build._CodeAPISetup and reader._session is session
        reader.info["private-source-cache"] = "temporary"
        (await reader.execute(text("SELECT zip_api_binding"))).scalar_one()
        if failure:
            raise ValueError("original Source refusal")
        return tables, ("published-shared-generation", 1, "coverage", parameters["plan_id"], parameters["market_type"])

    async def identity(reader, source_key, snapshot_id, system, code, *, tables, api_read):
        assert api_read is True
        (await reader.execute(text("SELECT zip_api_identity"))).scalar_one()
        return tables, None, {"snapshot_id": snapshot_id, "source_key": source_key, "code_system": system, "code": code}

    async def code_rows(reader, _tables, _arguments):
        return (await reader.execute(text("SELECT zip_api_code_rows"))).all()

    async def membership(actual_session, _identity, *, api_read):
        assert actual_session is session and api_read is True
        session.events.append("membership")
        return (object(),)

    member_by_key = {
        "code_coverage": [{"code_key": 1, "occurrence_count": 3}],
        "projection_id": "a" * 64,
        "content_digest": "b" * 64,
        "active_attachment": {"name": "pm_" + "a" * 52, "relation_oid": 1001},
        "storage": {"schema_oid": 2},
    }
    monkeypatch.setattr(build, "binding_source", binding)
    monkeypatch.setattr(build, "source_identity", identity)
    monkeypatch.setattr(build, "load_sealed_code_rows", code_rows)
    monkeypatch.setattr(build, "locate_membership", AsyncMock(side_effect=membership))
    monkeypatch.setattr(build, "membership_receipt", lambda _found: member_by_key)


@pytest.mark.parametrize("modes", [("repeatable read", "on"), ("serializable", "on")])
async def test_api_input_identity_keeps_all_original_decoders_behind_real_session_guards(monkeypatch, modes):
    session = _Session()
    session.driver.modes = modes
    _source_leaves(monkeypatch, session)
    scope_by_field = {name: value for name, value in _scope().items() if name != "geo_cells"}
    identity, inputs = await build.resolve_inputs(
        session, _selection(), ("CPT", "27447"), scope=scope_by_field, api_read=True
    )
    assert identity["scope"] == _scope() and len(identity["sources"]) == len(inputs) == 1
    assert identity["sources"][0]["source_identity"]["source_key"] == "synthetic-source"
    assert session.events == ["local-refusal", "binding", "identity", "membership", "code", "geo", "provider"]
    assert all(result.field_reads for result in session.results)
    assert "private-source-cache" not in session.info and session.info["unrelated"] == "preserved"
    assert execution._installed_read_inventory(session) is not None


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off")])
async def test_api_input_mode_flags_cannot_replace_original_native_snapshot(monkeypatch, modes):
    session = _Session()
    session.driver.modes = modes
    _source_leaves(monkeypatch, session)
    with pytest.raises(PlanPricingProjectionUnavailable, match="read-only"):
        await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=True)
    assert session.events == [] and session.info == {"unrelated": "preserved"}


async def test_local_source_reader_is_refused_before_existing_source_helpers_or_locks(monkeypatch):
    session = _Session()
    session.local_source = True
    _source_leaves(monkeypatch, session)
    with pytest.raises(PlanPricingProjectionUnavailable, match="selected source"):
        await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=True)
    assert session.events == ["local-refusal"] and session.info == {"unrelated": "preserved"}
    build.locate_membership.assert_not_awaited()


async def test_original_source_refusal_keeps_authority_and_info_private(monkeypatch):
    session = _Session()
    _source_leaves(monkeypatch, session, failure=True)
    with pytest.raises(ValueError, match="original Source refusal"):
        await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=True)
    assert session.events == ["local-refusal", "binding"] and session.info == {"unrelated": "preserved"}
    build.locate_membership.assert_not_awaited()


@pytest.mark.parametrize("mutation", ["name", "driver", "transaction", "optional-present"])
async def test_result_fields_stay_opaque_when_native_read_context_changes(monkeypatch, mutation):
    session = _Session()
    session.absent = {"ptg2_v4_finalizer_map_root"}
    _source_leaves(monkeypatch, session)

    def mutate(event, actual):
        if event != "binding":
            return
        if mutation == "name":
            actual.current[f'"{build.SCHEMA}"."npi"'] += 1
        elif mutation == "driver":
            actual.driver = _Driver(actual)
        elif mutation == "transaction":
            actual.transaction = object()
        else:
            actual.current[f'"{build.SCHEMA}"."ptg2_v4_finalizer_map_root"'] = 999

    session.on_query = mutate
    with pytest.raises(ServiceUnavailable):
        await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=True)
    assert session.results[-1].closed and session.results[-1].field_reads == 0
    assert session.info == {"unrelated": "preserved"}


async def test_api_setup_has_no_uninstalled_inventory_fallback():
    session = _Session()
    setup = build._CodeAPISetup(session)
    with pytest.raises(PlanPricingProjectionUnavailable, match="inventory"):
        await setup.scalar(text("SELECT zip_api_binding"))
    assert session.events == []


@pytest.mark.parametrize("scope", [None, _scope()])
async def test_api_capability_route_activates_both_metadata_seams_without_provider_locks(monkeypatch, scope):
    session = _Session()
    identity = _zip_identity()
    inputs = AsyncMock(return_value=(identity, []))
    locate, locks = AsyncMock(return_value=(SimpleNamespace(schema_name="synthetic"),)), AsyncMock()
    monkeypatch.setattr(build, "resolve_inputs", inputs)
    monkeypatch.setattr(reads, "locate", locate)
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    capability = await reads._resolve_capability(session, _selection(), ("CPT", "27447"), scope=scope, api_read=True)
    assert capability.identity == identity
    assert inputs.await_args.kwargs == {"api_read": True, **({"scope": scope} if scope is not None else {})}
    locate.assert_awaited_once_with(session, identity, api_read=True)
    locks.assert_not_awaited()


async def test_attached_zip_capability_failures_do_not_take_a_national_fallback(monkeypatch):
    session = _Session()
    monkeypatch.setattr(build, "resolve_inputs", AsyncMock(return_value=(_zip_identity(), [])))
    failed = AsyncMock(side_effect=PlanPricingProjectionUnavailable("native capability refusal"))
    monkeypatch.setattr(reads, "locate", failed)
    with pytest.raises(PlanPricingProjectionUnavailable, match="native capability refusal"):
        await reads.resolve_code_pricing(
            session, _selection(), {**_params(expected=False), "include_providers": "false"}, api_read=True
        )
    assert failed.await_count == 1


async def test_public_zip_route_activates_qualified_plain_inputs(monkeypatch):
    session = _Session()
    _source_leaves(monkeypatch, session)
    locate = AsyncMock(return_value=(SimpleNamespace(schema_name="synthetic"),))
    locks = AsyncMock()
    monkeypatch.setattr(reads, "locate", locate)
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    capability = await reads.resolve_code_pricing(
        session, _selection(), {**_params(expected=False), "include_providers": "false"}
    )
    assert capability.api_read is True and capability.identity["scope"] == _scope()
    locate.assert_awaited_once_with(session, capability.identity, api_read=True)
    locks.assert_not_awaited()


@pytest.mark.parametrize("route", ["partitioned", "inherited", "local", "read_committed"])
async def test_public_zip_route_preserves_existing_unqualified_reader(monkeypatch, route):
    session = _Session()
    if route == "partitioned":
        session.kinds["ptg2_v3_code"] = "p"
    elif route == "inherited":
        session.inherited.add("ptg2_v3_code")
    elif route == "local":
        session.local_source = True
    else:
        session.driver.modes = "read committed", "on"
    inputs = AsyncMock(return_value=(_zip_identity(), []))
    locate, locks = AsyncMock(return_value=(SimpleNamespace(schema_name="synthetic"),)), AsyncMock()
    monkeypatch.setattr(build, "resolve_inputs", inputs)
    monkeypatch.setattr(reads, "locate", locate)
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    capability = await reads.resolve_code_pricing(
        session, _selection(), {**_params(expected=False), "include_providers": "false"}
    )
    assert capability.api_read is False
    inputs.assert_awaited_once_with(
        session,
        _selection(),
        ("CPT", "27447"),
        scope={key: value for key, value in _scope().items() if key != "geo_cells"},
    )
    locate.assert_awaited_once_with(session, _zip_identity())
    locks.assert_awaited_once_with(session)
    assert session.info == {"unrelated": "preserved"}


async def test_api_route_drift_does_not_become_a_legacy_fallback(monkeypatch):
    session = _Session()
    session.on_query = lambda event, actual: (
        setattr(actual, "transaction", object()) if event == "local-refusal" else None
    )
    locks = AsyncMock()
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    with pytest.raises(ServiceUnavailable):
        await reads.resolve_code_pricing(
            session, _selection(), {**_params(expected=False), "include_providers": "false"}
        )
    locks.assert_not_awaited()
    assert session.results[-1].closed and session.results[-1].field_reads == 0


@pytest.mark.parametrize("stage", ["modes", "catalog"])
async def test_ineligible_metadata_cannot_hide_original_transaction_drift(monkeypatch, stage):
    session = _Session()
    if stage == "modes":
        session.driver.modes = "read committed", "on"
    else:
        session.kinds["ptg2_v3_code"] = "p"
    setattr(session, "on_" + stage, lambda actual: setattr(actual, "transaction", object()))
    locks = AsyncMock()
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    with pytest.raises(PlanPricingProjectionUnavailable, match="transaction changed"):
        await reads.resolve_code_pricing(
            session, _selection(), {**_params(expected=False), "include_providers": "false"}
        )
    locks.assert_not_awaited()
    assert session.events == ["attachment"] and session.info == {"unrelated": "preserved"}


@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("mutation", ["transaction", "driver"])
@pytest.mark.parametrize("api_read", [None, True])
async def test_attachment_discovery_drift_cannot_fallback_or_establish_a_new_baseline(
    monkeypatch, present, mutation, api_read
):
    session = _Session()
    session.attachment = present

    def mutate(event, actual):
        assert event == "attachment"
        setattr(actual, mutation, object() if mutation == "transaction" else _Driver(actual))

    session.on_query = mutate
    locks, inputs = AsyncMock(), AsyncMock()
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    monkeypatch.setattr(build, "resolve_inputs", inputs)
    with pytest.raises(PlanPricingProjectionUnavailable, match="transaction changed"):
        await reads.resolve_code_pricing(
            session, _selection(), {**_params(expected=False), "include_providers": "false"}, api_read=api_read
        )
    locks.assert_not_awaited()
    inputs.assert_not_awaited()
    assert session.events == ["attachment"]
    assert session.results[0].closed and session.results[0].field_reads == 0


async def test_setup_helpers_keep_the_original_transaction_and_physical_connection():
    session = _Session()
    setup = build._CodeAPISetup(session)
    assert setup.original_read_session is session
    assert setup.in_transaction() is True and setup.get_transaction() is session.transaction
    borrowed = await setup.connection()
    assert (await borrowed.get_raw_connection()).driver_connection is session.driver
    assert session.acquisitions == 1 and session.events == []
    session.driver.active = False
    assert setup.in_transaction() is False


async def test_session_without_native_transaction_interface_declines_api_without_a_borrow():
    session = SimpleNamespace(info={"unrelated": "preserved"}, connection=AsyncMock())
    assert await build.can_read_code_api(session, _selection()) is False
    session.connection.assert_not_awaited()
    assert session.info == {"unrelated": "preserved"}


@pytest.mark.parametrize("damage", ["missing", "duplicate", "order"])
async def test_fixed_input_census_cannot_be_partial_duplicated_or_reordered(monkeypatch, damage):
    session = _Session()
    original = session.driver.fetch

    async def fetch(*arguments):
        rows = await original(*arguments)
        if damage == "missing":
            return rows[:-1]
        if damage == "duplicate":
            return [*rows, rows[-1]]
        return list(reversed(rows))

    monkeypatch.setattr(session.driver, "fetch", fetch)
    with pytest.raises(PlanPricingProjectionUnavailable, match="inventory differs"):
        await build.can_read_code_api(session, _selection())
    assert session.events == [] and session.info == {"unrelated": "preserved"}


@pytest.mark.parametrize("api_read", [None, 0, 1, "true"])
async def test_nonboolean_api_mode_is_refused_before_reading_any_source(api_read):
    session = _Session()
    with pytest.raises(ValueError, match="API mode is invalid"):
        await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=api_read)
    assert session.events == [] and session.acquisitions == 0


@pytest.mark.parametrize("source_kind", ["v", "m", "f"])
async def test_unsupported_source_relation_never_enters_the_api_source_helpers(source_kind):
    session = _Session()
    session.kinds["ptg2_v3_code"] = source_kind
    assert await build.can_read_code_api(session, _selection()) is False
    assert session.events == [] and session.info == {"unrelated": "preserved"}


async def test_legacy_selection_restores_the_existing_inventory_instead_of_replacing_it():
    session = _Session()
    await build._api_input_inventory(session, session.driver)
    previous = session.info[execution._SESSION_INVENTORY_KEY]
    session.local_source = True
    assert await build.can_read_code_api(session, _selection()) is False
    assert session.info[execution._SESSION_INVENTORY_KEY] is previous
    assert session.info["unrelated"] == "preserved"
    assert session.events == ["local-refusal"]


async def test_api_page_guards_geo_and_real_aggregate_decoder(monkeypatch):
    from api import plan_pricing_projection_read as projection
    from api.plan_pricing_projection_contract import code_projection_scope
    from tests import test_plan_pricing_projection_v3_read as packs

    session = _Session()
    async with build._api_input_reader(session, _selection()) as setup:
        assert setup.original_read_session is session
    capability = reads.CodePricingCapability(
        _zip_identity(), (SimpleNamespace(schema_name="synthetic"),), api_read=True
    )
    monkeypatch.setattr(packs, "PROJECTION_ID", capability.projection_id)
    session.aggregate_rows = [packs._pack_row(packs._record("48104"), packs._record("48105"))]
    request = projection._ProjectionRequest(
        "rate_aggregates", capability.projection_id, capability.identity["contract"], "CPT", "27447"
    )
    pagination = SimpleNamespace(limit=2, offset=0, page=1)
    with code_projection_scope(capability):
        response = await projection._projection_page_response(
            session, request, _params(expected=False), pagination, None, _selection()
        )
    assert response["pagination"]["total"] == 2
    assert session.events[-2:] == ["geo", "aggregate"]
    assert all(result.field_reads for result in session.results)
