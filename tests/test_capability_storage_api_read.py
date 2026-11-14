# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named capability reads preserve scope, immutable data and their SQL snapshot."""

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from api import plan_pricing_code_storage as code
from api import ptg_membership_storage as membership
from api.ptg2_serving import PTG2_SCHEMA
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_code_projection import _identity
from tests.test_ptg_membership_projection import PARAMS


def _code_identity():
    identity = _identity()
    schema = membership.native.reference_family_stage_schema(UUID(int=3))
    identity["sources"][0]["membership"] = {
        "projection_id": "c" * 64,
        "content_digest": "a" * 64,
        "coverage_digest": "d" * 64,
        "active_attachment": {
            "schema_name": "hp_snapshot_retention",
            "name": membership._attachment_for_digest("c" * 64),
        },
        "storage": {
            "dataset_id": str(UUID(int=3)),
            "schema_name": schema,
            "relations": [[schema, name] for name in sorted(membership.SPEC.table_names)],
        },
    }
    return identity


class _Result:
    def __init__(self, session, kind, value):
        self.session, self.kind, self.value, self.closed = session, kind, value, False

    def _decode(self):
        self.session.events.append(("decode", self.kind))
        return self.value

    def mappings(self):
        return self

    def first(self):
        return self._decode()

    def one(self):
        return self._decode()

    def all(self):
        return self._decode()

    def scalar_one(self):
        return self._decode()

    def __iter__(self):
        return iter(self._decode())

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, storage):
        self.storage, self.active, self.info = storage, True, {}
        self.transaction, self.modes = object(), ("repeatable read", "on")
        self.events, self.queries, self.results = [], [], []
        self.identity = copy.deepcopy(PARAMS if storage is membership else _code_identity())
        self.schema = storage.native.reference_family_stage_schema(UUID(int=1 if storage is membership else 2))
        self.dependencies = [{"schema_name": self.schema, "table_name": name} for name in storage.SPEC.table_names]
        if storage is code:
            self.dependencies.extend(
                {
                    "schema_name": "hp_snapshot_retention",
                    "table_name": source["membership"]["active_attachment"]["name"],
                }
                for source in self.identity["sources"]
            )
        self.header = {
            "identity": copy.deepcopy(self.identity),
            "content_digest": "a" * 64,
            "counts": {"code_coverage": []},
        }
        self.coverage, self.attachment, self.pin_present = [], True, True
        self.fault, self.drift, self.selector_present = None, None, True
        self.commit, self.rollback = AsyncMock(), AsyncMock()
        self.pin_keys = []

    def in_transaction(self):
        return self.active

    def get_transaction(self):
        return self.transaction if self.active else None

    def _response(self, sql, parameters):
        if "current_setting('transaction_isolation')" in sql:
            return "modes", self.modes
        if "SELECT DISTINCT source.nspname" in sql:
            return "dependencies", self.dependencies
        if "SELECT pg_catalog.to_regclass" in sql:
            return "attachment", (
                self.attachment if type(self.attachment) is bool else parameters["relation"] in self.attachment
            )
        if "SELECT owner_id FROM ONLY" in sql:
            return "editions", [{"owner_id": key} for key in self.pin_keys]
        if "SELECT identity,content_digest,counts" in sql:
            return "header", self.header
        if "SELECT identity FROM ONLY" in sql:
            return "selector", {"identity": self.header["identity"]} if self.selector_present else None
        if "SELECT code_key,occurrence_count" in sql:
            return "coverage", self.coverage
        if "SELECT EXISTS(SELECT 1 FROM ONLY" in sql:
            expected = (
                membership.digest(self.header["identity"])
                if self.storage is membership
                else code.identity_digest(self.header["identity"])
            )
            assert parameters["owner_id"] == expected
            assert parameters["owner_type"] == (
                membership.CONTRACT if self.storage is membership else self.identity["contract"]
            )
            snapshots = (
                {self.identity["snapshot_id"]}
                if self.storage is membership
                else {source_by_field["source_identity"]["snapshot_id"] for source_by_field in self.identity["sources"]}
            )
            assert parameters["snapshot_id"] in snapshots
            assert membership.native._quoted(PTG2_SCHEMA) + ".ptg2_snapshot_pin" in sql
            return "source-pin", self.pin_present
        raise AssertionError(sql)

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        assert all(
            value not in sql
            for value in ("LOCK TABLE", "pg_advisory", "pg_roles", "pg_database", "relowner", "pg_has_role")
        )
        kind, value = self._response(sql, parameters)
        self.queries.append((sql, parameters))
        self.events.append(("execute", kind))
        if self.fault is not None and self.fault[0] == kind:
            raise self.fault[1]
        result = _Result(self, kind, value)
        self.results.append(result)
        if self.drift == kind:
            self.transaction = object()
        return result


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("modes", [("repeatable read", "on"), ("serializable", "on")])
async def test_api_read_returns_complete_named_inventory_header_and_existing_retention(storage, modes):
    session = _Session(storage)
    session.modes = modes
    original = session.transaction
    found = await storage.locate(session, session.identity, api_read=True)
    assert found[0].schema_name == session.schema
    assert found[1] == session.header
    receipt = storage.receipt(found)
    assert receipt["storage"]["schema_name"] == session.schema
    assert receipt["storage"]["relations"] == [[session.schema, name] for name in storage.SPEC.table_names]
    assert set(receipt["active_attachment"]) == {"schema_name", "name"}
    assert receipt["content_digest"] == session.header["content_digest"]
    assert session.transaction is original and session.active and session.info == {}
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off")])
async def test_api_requires_readonly_repeatable_snapshot_before_attachment_lookup(storage, modes):
    session = _Session(storage)
    session.modes = modes
    with pytest.raises(PTG2ManifestArtifactError, match="snapshot"):
        await storage.locate(session, session.identity, api_read=True)
    assert session.events == [("execute", "modes"), ("decode", "modes")]


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("phase", ["dependencies", "header", "source-pin"])
async def test_changed_transaction_closes_buffer_before_decode_or_capability(storage, phase):
    session = _Session(storage)
    session.drift = phase
    with pytest.raises(PTG2ManifestArtifactError, match="transaction changed"):
        await storage.locate(session, session.identity, api_read=True)
    assert ("decode", phase) not in session.events and session.results[-1].closed
    assert session.info == {}


@pytest.mark.parametrize("rows", [[], [{"code_key": 1, "occurrence_count": 2, "set_count": 1, "set_digest": "b" * 64}]])
async def test_membership_coverage_compares_complete_rows_including_empty_results(rows):
    session = _Session(membership)
    session.coverage = rows
    if rows:
        with pytest.raises(PTG2ManifestArtifactError, match="code coverage"):
            await membership.locate(session, session.identity, api_read=True)
    else:
        assert await membership.locate(session, session.identity, api_read=True) is not None


@pytest.mark.parametrize("storage", [membership, code])
async def test_missing_source_retention_pin_never_returns_capability(storage):
    session = _Session(storage)
    session.pin_present = False
    with pytest.raises(PTG2ManifestArtifactError, match="retention pin"):
        await storage.locate(session, session.identity, api_read=True)
    assert not session.info


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("fault", ["missing", "extra", "split", "namespace"])
async def test_named_inventory_refuses_incomplete_or_changed_family(storage, fault):
    session = _Session(storage)
    if fault == "missing":
        session.dependencies.pop(0)
    elif fault == "extra":
        session.dependencies.append({"schema_name": session.schema, "table_name": "unexpected"})
    elif fault == "split":
        session.dependencies[0] = {"schema_name": "other", "table_name": session.dependencies[0]["table_name"]}
    else:
        _change_dependency_namespace(session)
    with pytest.raises(PTG2ManifestArtifactError, match="inventory|namespace"):
        await storage.locate(session, session.identity, api_read=True)
    assert ("execute", "header") not in session.events


def _change_dependency_namespace(session):
    for dependency_by_field in session.dependencies:
        if dependency_by_field["schema_name"] == session.schema:
            dependency_by_field["schema_name"] = "unsupported_archive"


@pytest.mark.parametrize("storage", [membership, code])
async def test_absent_attachment_preserves_unindexed_routing_without_ending_snapshot(storage):
    session = _Session(storage)
    session.attachment = False
    assert await storage.locate(session, session.identity, api_read=True) is None
    assert ("execute", "dependencies") not in session.events and session.active


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("phase", ["attachment", "header", "source-pin"])
@pytest.mark.parametrize("failure", [asyncio.CancelledError(), asyncpg.InsufficientPrivilegeError("read denied")])
async def test_cancel_and_database_denial_propagate_without_return_or_transaction_end(storage, phase, failure):
    session = _Session(storage)
    original = session.transaction
    session.fault = phase, failure
    with pytest.raises(type(failure)) as caught:
        await storage.locate(session, session.identity, api_read=True)
    assert caught.value is failure and session.transaction is original and session.active
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("invalid_flag", [None, 1, "true"])
async def test_api_opt_in_requires_a_server_boolean(storage, invalid_flag):
    session = _Session(storage)
    with pytest.raises(PTG2ManifestArtifactError, match="read mode"):
        await storage.locate(session, session.identity, api_read=invalid_flag)
    assert session.events == []


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("fault", ["identity", "content"])
async def test_logical_scope_and_content_digest_refuse_before_retention_pin(storage, fault):
    session = _Session(storage)
    if fault == "identity":
        session.header["identity"]["code"] = "99214"
    else:
        session.header["content_digest"] = "not-a-digest"
    with pytest.raises(PTG2ManifestArtifactError, match="identity"):
        await storage.locate(session, session.identity, api_read=True)
    assert ("execute", "source-pin") not in session.events


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("inactive", [False, True])
async def test_api_storage_scope_requires_the_original_active_transaction(storage, inactive):
    session = _Session(storage)
    original = session.transaction
    with pytest.raises(PTG2ManifestArtifactError, match="transaction changed"):
        async with membership._api_storage_read(session, original):
            if inactive:
                session.active = False
            else:
                session.transaction = object()


async def test_saved_code_receipt_projects_known_metadata_and_recovers_its_exact_pin():
    session = _Session(code)
    source = session.header["identity"]["sources"][0]
    storage = source["membership"]["storage"]
    source["membership"]["storage"] = {
        "dataset_id": storage["dataset_id"],
        "schema_oid": 900,
        "owner_oid": 901,
        "relation_oids": [[name, index + 100] for index, (_schema, name) in enumerate(storage["relations"])],
    }
    source["membership"]["active_attachment"].pop("schema_name")
    source["membership"]["active_attachment"]["relation_oid"] = 902
    source["read_identity_digest"] = "f" * 64
    expected = code.identity_digest(session.header["identity"])
    found = await code.locate(session, session.identity, api_read=True)
    assert code.receipt(found)["projection_id"] == expected
    from api.plan_pricing_code_read import CodePricingCapability

    assert CodePricingCapability(session.identity, found, api_read=True).projection_id == expected
    assert "relation_oid" not in code.receipt(found)["identity"]["sources"][0]["membership"]["active_attachment"]


@pytest.mark.parametrize("fault", ["unknown", "missing", "wrong_name"])
async def test_saved_code_receipt_refuses_unknown_or_changed_wire_before_edition_recovery(fault):
    session = _Session(code)
    receipt = session.header["identity"]["sources"][0]["membership"]
    if fault == "unknown":
        receipt["storage"]["unknown"] = "field"
    elif fault == "missing":
        receipt.pop("content_digest")
    else:
        receipt["active_attachment"]["name"] = "pm_" + "b" * 52
    with pytest.raises(PTG2ManifestArtifactError):
        await code.locate(session, session.identity, api_read=True)
    assert ("execute", "source-pin") not in session.events


async def test_saved_membership_source_metadata_uses_existing_named_alias_and_pin():
    session = _Session(membership)
    session.identity.update(
        contract=membership.CONTRACT,
        physical_binding=None,
        graph_root={"map_digest": "d" * 64},
        finalizer={"state": "legacy_relational"},
        source_storage={"schema_name": "named_source", "relations": [["named_source", "payload"]]},
    )
    session.header["identity"] = copy.deepcopy(session.identity)
    session.header["identity"]["source_storage"] = {
        "database_oid": 100,
        "schema_oid": 101,
        "relation_oids": [["payload", 102]],
        "attestation_digest": "e" * 64,
    }
    original = membership.digest(session.header["identity"])
    alias = membership._attachment_for_digest(original)
    session.pin_keys = [original]
    session.attachment = {membership.qualified_relation_sql("hp_snapshot_retention", alias)}
    found = await membership.locate(session, session.identity, api_read=True)
    result = membership.receipt(found)
    assert result["projection_id"] == original
    assert result["source_storage"] == session.identity["source_storage"]
    assert result["active_attachment"]["name"] == alias
    assert all("database_oid" not in sql and "owner.oid" not in sql for sql, _parameters in session.queries)


async def test_membership_reader_propagates_api_mode_and_reuses_only_its_request_pin(monkeypatch):
    from api import ptg_membership_reader as reader

    session = object()
    tables = SimpleNamespace(source_key=PARAMS["source_key"], snapshot_id=PARAMS["snapshot_id"])
    request = SimpleNamespace(code_system=PARAMS["code_system"], code=PARAMS["code"])
    state = SimpleNamespace(membership_index_checked=False, membership_index=None)
    resolve, locate = AsyncMock(return_value=(tables, object(), PARAMS)), AsyncMock(return_value=object())
    monkeypatch.setattr(reader, "source_identity", resolve)
    monkeypatch.setattr(reader, "locate", locate)
    first = await reader._attached_index(session, tables, request, state)
    assert await reader._attached_index(session, tables, request, state) is first
    resolve.assert_awaited_once_with(
        session,
        PARAMS["source_key"],
        PARAMS["snapshot_id"],
        PARAMS["code_system"],
        PARAMS["code"],
        tables=tables,
        api_read=True,
    )
    locate.assert_awaited_once_with(session, PARAMS, api_read=True)


@pytest.mark.parametrize("api_read", [None, False, True])
async def test_code_request_propagates_named_mode_and_preserves_explicit_builder_mode(monkeypatch, api_read):
    from api import plan_pricing_code_read as read
    from tests.test_plan_pricing_code_projection import _selection

    selected, session = _selection(), object()
    resolve = AsyncMock(return_value=None)
    monkeypatch.setattr(read, "_resolve_capability", resolve)
    assert (
        await read.resolve_code_pricing(session, selected, {"code_system": "CPT", "code": "27447"}, api_read=api_read)
        is None
    )
    resolve.assert_awaited_once_with(
        session, selected, ("CPT", "27447"), **({"api_read": True} if api_read is not False else {})
    )


@pytest.mark.parametrize("missing", [False, True])
async def test_source_api_inventory_reads_qualified_names_without_physical_metadata(missing):
    from api import ptg_membership_source as source

    async def execute(statement, parameters):
        sql = str(statement)
        assert "pg_catalog.to_regclass" in sql and "oid" not in sql and "attestation" not in sql
        assert parameters["schema"] == "named_source"
        names = parameters["names"] if not missing else []
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [{"relname": name} for name in names]))

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute), scalar=AsyncMock())
    operation = source._source_storage(session, "named_source", "snapshot", api_read=True)
    if missing:
        with pytest.raises(PTG2ManifestArtifactError, match="inventory is incomplete"):
            await operation
    else:
        inventory = await operation
        assert inventory == {
            "schema_name": "named_source",
            "relations": [["named_source", name] for name in session.execute.await_args.args[1]["names"]],
        }
    session.scalar.assert_not_awaited()
