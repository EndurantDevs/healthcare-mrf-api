# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""ZIP setup guards original-session results; Source authority leaves are doubles."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy import text

from api import plan_pricing_code_build as build
from api import plan_pricing_code_read as reads
from api import ptg_membership_storage as membership
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.network_compact_layout import CompactAddressRelation
from tests.test_plan_pricing_code_projection import _selection
from tests.test_plan_pricing_code_zip import _params, _scope, _zip_identity


class _Result:
    def __init__(self, session, value, *, metadata=False):
        self.session, self.value, self.closed = session, value, False
        self.field_reads = 0
        self.scalars = lambda: SimpleNamespace(all=self._checked)
        self.mappings = self._checked

    def _checked(self):
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
        self.session, self.active = session, True
        self.modes = "repeatable read", "on"

    async def fetchrow(self, sql, *parameters):
        assert "current_setting('transaction_isolation')" in sql
        if self.session.on_modes is not None:
            self.session.on_modes(self.session)
        return self.modes

    async def fetch(self, sql, schemas, names):
        assert "FROM ROWS FROM (pg_catalog.unnest(" in sql
        assert "AS available" in sql and "oid" not in sql
        rows = [
            {"schema_name": schema, "table_name": name, "available": name not in self.session.absent}
            for schema, name in zip(schemas, names)
        ]
        if self.session.on_catalog is not None:
            self.session.on_catalog(self.session)
        return rows


class _Session:
    def __init__(self):
        self.info = {"unrelated": "preserved"}
        self.transaction, self.driver = object(), _Driver(self)
        self.acquisitions = 0
        self.absent = set()
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
            if isinstance(failure, BaseException):
                raise failure
            raise ValueError("original Source refusal")
        return tables, ("published-shared-generation", 1, "coverage", parameters["plan_id"], parameters["market_type"])

    async def identity(reader, source_key, snapshot_id, system, code, *, tables, api_read):
        assert api_read is True
        (await reader.execute(text("SELECT zip_api_identity"))).scalar_one()
        return tables, None, {"snapshot_id": snapshot_id, "source_key": source_key, "code_system": system, "code": code}

    async def code_rows(reader, _tables, _arguments):
        return (await reader.execute(text("SELECT zip_api_code_rows"))).all()

    async def membership_lookup(actual_session, identity, *, api_read):
        assert actual_session is session and api_read is True
        session.events.append("membership")
        projection = membership.digest(identity)
        schema = membership.native.reference_family_stage_schema(UUID(int=1))
        header_by_field = {
            "identity": identity,
            "content_digest": "b" * 64,
            "counts": {"code_coverage": [{"code_key": 1, "occurrence_count": 3}]},
        }
        return (
            CompactAddressRelation(schema, "ptg_membership_manifest"),
            header_by_field,
            None,
            CompactAddressRelation("hp_snapshot_retention", membership._attachment_for_digest(projection)),
            {"owner_type": membership.CONTRACT, "owner_id": projection, "snapshot_id": identity["snapshot_id"]},
        )

    monkeypatch.setattr(build, "binding_source", binding)
    monkeypatch.setattr(build, "source_identity", identity)
    monkeypatch.setattr(build, "load_sealed_code_rows", code_rows)
    monkeypatch.setattr(build, "locate_membership", AsyncMock(side_effect=membership_lookup))
    monkeypatch.setattr(build, "membership_receipt", membership.receipt)


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
    assert session.info == {"unrelated": "preserved"}


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


async def test_changed_transaction_refuses_the_complete_input_identity(monkeypatch):
    session = _Session()
    _source_leaves(monkeypatch, session)
    session.on_query = lambda event, actual: setattr(actual, "transaction", object()) if event == "binding" else None
    with pytest.raises(PlanPricingProjectionUnavailable, match="transaction changed"):
        await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=True)
    assert session.info == {"unrelated": "preserved"}


async def test_api_setup_refuses_unconfigured_named_source_relations():
    session = _Session()
    setup = build._CodeAPISetup(session)
    with pytest.raises(PlanPricingProjectionUnavailable, match="routing is unavailable"):
        setup.source_relation(build.SCHEMA, "npi")
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


@pytest.mark.parametrize("route", ["local", "read_committed"])
async def test_public_zip_route_refuses_unqualified_inputs_without_legacy_fallback(monkeypatch, route):
    session = _Session()
    session.local_source = route == "local"
    if route == "read_committed":
        session.driver.modes = "read committed", "on"
    _source_leaves(monkeypatch, session)
    locate, locks = AsyncMock(), AsyncMock()
    monkeypatch.setattr(reads, "locate", locate)
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    with pytest.raises(PlanPricingProjectionUnavailable):
        await reads.resolve_code_pricing(
            session, _selection(), {**_params(expected=False), "include_providers": "false"}
        )
    locate.assert_not_awaited()
    locks.assert_not_awaited()
    assert session.info == {"unrelated": "preserved"}


async def test_api_route_drift_does_not_become_a_legacy_fallback(monkeypatch):
    session = _Session()
    _source_leaves(monkeypatch, session)
    session.on_query = lambda event, actual: (
        setattr(actual, "transaction", object()) if event == "local-refusal" else None
    )
    locks = AsyncMock()
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    with pytest.raises(PlanPricingProjectionUnavailable, match="transaction changed"):
        await reads.resolve_code_pricing(
            session, _selection(), {**_params(expected=False), "include_providers": "false"}
        )
    locks.assert_not_awaited()
    assert session.info == {"unrelated": "preserved"}


@pytest.mark.parametrize("stage", ["modes", "catalog"])
async def test_ineligible_metadata_cannot_hide_original_transaction_drift(monkeypatch, stage):
    session = _Session()
    if stage == "modes":
        session.driver.modes = "read committed", "on"
    else:
        session.absent.add("npi")
    setattr(session, "on_" + stage, lambda actual: setattr(actual, "transaction", object()))
    with pytest.raises(PlanPricingProjectionUnavailable, match="transaction changed"):
        await build.can_read_code_api(session, _selection())
    assert session.events == [] and session.info == {"unrelated": "preserved"}


@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("api_read", [None, True])
async def test_attachment_discovery_drift_cannot_fallback_or_establish_a_new_baseline(monkeypatch, present, api_read):
    session = _Session()
    session.attachment = present
    session.on_query = lambda event, actual: setattr(actual, "transaction", object())
    locks, inputs = AsyncMock(), AsyncMock()
    monkeypatch.setattr(reads, "_lock_provider_relations", locks)
    monkeypatch.setattr(build, "resolve_inputs", inputs)
    with pytest.raises(PlanPricingProjectionUnavailable, match="transaction changed"):
        await reads.resolve_code_pricing(
            session, _selection(), {**_params(expected=False), "include_providers": "false"}, api_read=api_read
        )
    locks.assert_not_awaited()
    inputs.assert_not_awaited()
    assert session.events == ["attachment"] and session.results[0].closed and session.results[0].field_reads == 0


async def test_setup_helpers_borrow_the_configured_session_transaction():
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
async def test_fixed_named_input_inventory_cannot_be_partial_duplicated_or_reordered(monkeypatch, damage):
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
        await build._api_input_inventory(session, session.driver)
    assert session.events == [] and session.info == {"unrelated": "preserved"}


@pytest.mark.parametrize("api_read", [None, 0, 1, "true"])
async def test_nonboolean_api_mode_is_refused_before_reading_any_source(api_read):
    session = _Session()
    with pytest.raises(ValueError, match="API mode is invalid"):
        await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=api_read)
    assert session.events == [] and session.acquisitions == 0


async def test_ineligible_selection_preserves_captured_names_and_private_session_info():
    session = _Session()
    names = await build._api_input_inventory(session, session.driver)
    original_names = set(names)
    session.local_source = True
    assert await build.can_read_code_api(session, _selection()) is False
    assert names == original_names and session.info == {"unrelated": "preserved"}
    assert session.events == ["local-refusal"]


async def test_api_page_preserves_zip_scope_and_the_real_aggregate_decoder(monkeypatch):
    from api import plan_pricing_code_storage as storage
    from api import plan_pricing_projection_read as projection
    from api.plan_pricing_projection_contract import code_projection_scope
    from tests import test_plan_pricing_projection_v3_read as packs

    session = _Session()
    _source_leaves(monkeypatch, session)
    identity, _inputs = await build.resolve_inputs(
        session,
        _selection(),
        ("CPT", "27447"),
        scope={key: scope_value for key, scope_value in _scope().items() if key != "geo_cells"},
        api_read=True,
    )
    key = storage.identity_digest(identity)
    schema = membership.native.reference_family_stage_schema(UUID(int=2))
    found = (
        CompactAddressRelation(schema, "plan_pricing_code_manifest"),
        {"identity": identity, "content_digest": "b" * 64, "counts": {}},
        key,
        CompactAddressRelation("hp_snapshot_retention", storage.attachment_name(identity)),
    )
    capability = reads.CodePricingCapability(identity, found, api_read=True)
    assert storage.receipt(found)["projection_id"] == capability.projection_id
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
    assert all(query_result.field_reads for query_result in session.results)


async def test_optional_named_source_inputs_remain_scoped_to_the_captured_inventory():
    from api.ptg2_tables import PTG2_SCHEMA

    session = _Session()
    session.absent.add("ptg2_v4_finalizer_map_root")
    names = await build._api_input_inventory(session, session.driver)
    setup = build._CodeAPISetup(session, names)
    session.absent.clear()
    with pytest.raises(PlanPricingProjectionUnavailable, match="routing is unavailable"):
        setup.source_relation(PTG2_SCHEMA, "ptg2_v4_finalizer_map_root")
    assert setup.source_relation(PTG2_SCHEMA, "ptg2_snapshot") == f'"{PTG2_SCHEMA}"."ptg2_snapshot"'


@pytest.mark.parametrize("failure", [ValueError("Source refused"), asyncio.CancelledError()])
async def test_original_failure_and_cancellation_survive_transaction_drift(monkeypatch, failure):
    session = _Session()
    _source_leaves(monkeypatch, session, failure=failure)
    session.on_query = lambda event, actual: setattr(actual, "transaction", object()) if event == "binding" else None
    with pytest.raises(type(failure)) as caught:
        await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=True)
    assert caught.value is failure
    assert "transaction changed" in caught.value.__notes__[-1]
    assert session.info == {"unrelated": "preserved"}


@pytest.mark.parametrize("available", [None, 1, "true"])
async def test_named_inventory_refuses_malformed_availability_without_selecting_legacy(monkeypatch, available):
    session = _Session()
    original = session.driver.fetch

    async def fetch(*arguments):
        rows = await original(*arguments)
        rows[0]["available"] = available
        return rows

    monkeypatch.setattr(session.driver, "fetch", fetch)
    with pytest.raises(PlanPricingProjectionUnavailable, match="inventory differs"):
        await build.can_read_code_api(session, _selection())
    assert session.events == []
