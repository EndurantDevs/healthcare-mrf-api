# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained read-access successor contract, independent of a live database."""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from process import network_retained_read_access as retained
from process import network_serving_read as serving
from process.network_address_projection import PinnedAddressSource

pytestmark = pytest.mark.asyncio

SCHEMA = "network_candidate_" + "a" * 32
PROOF_SCHEMA = "network_retained_access_" + "a" * 32
MANIFEST = "b" * 64
BEFORE = [["schema", 7, 0, 5, 9, "USAGE", False]]


def _receipt():
    return retained._RetainedReceipt("owner", 5, 7, ("reader",), (("entity_address_unified", 11),), None)


def _after():
    return retained._extended_acl(_receipt(), BEFORE, 42)


def _proof(**changes):
    proof_by_field = dict(
        contract=retained._CONTRACT,
        schema_name=SCHEMA,
        manifest_sha256=MANIFEST,
        reader="executor",
        reader_oid=42,
        roles_sha256="c" * 64,
        before=BEFORE,
        after=_after(),
    )
    return {**proof_by_field, **changes}


def _entry(schema=SCHEMA):
    candidate_by_field = {
        "schema_name": schema,
        "validation_json": {"writer_closure": {"owner_role": "owner", "loader_roles": [], "reader_roles": ["reader"]}},
    }
    catalog_by_field = {"schema_oid": 7, "relation_oids": {"entity_address_unified": 11}, "privileges_closed": False}
    return {
        "candidate_json": json.dumps(candidate_by_field),
        "catalog_json": json.dumps(catalog_by_field),
        "manifest_json": json.dumps({"generation_id": 3, "manifest_sha256": MANIFEST}),
    }


class _Connection:
    def __init__(self, *, proof=None, owner=5, closed=True, missing_namespace=False, generation=3):
        self.statements = []
        self.document = None if proof is None else json.dumps(proof)
        self.row = None if proof is None else {"nspowner": owner, "document": self.document, "closed": closed}
        self.missing_namespace, self.generation = missing_namespace, generation

    async def fetchval(self, sql, *arguments):
        if "to_regnamespace" in sql:
            return self.missing_namespace
        if "FROM pg_roles" in sql:
            return 5
        if "generation_id" in sql:
            return self.generation
        if "quote_literal" in sql:
            return "'literal'"
        raise AssertionError(sql)

    async def fetchrow(self, sql, *arguments):
        assert arguments == (PROOF_SCHEMA,)
        return self.row

    async def execute(self, sql):
        self.statements.append(sql)

    @asynccontextmanager
    async def transaction(self):
        yield


@pytest.fixture
def native(monkeypatch):
    calls_by_kind = {"acl": [], "grants": []}
    state_by_field = {"acl": BEFORE, "reader": (42, "c" * 64)}

    async def acl(connection, receipt):
        calls_by_kind["acl"].append(state_by_field["acl"])
        return state_by_field["acl"]

    async def grant(connection, receipt, reader, *, revoke=False):
        calls_by_kind["grants"].append((reader, revoke))
        state_by_field["acl"] = BEFORE if revoke else _after()

    async def reader(connection, receipt, name):
        return state_by_field["reader"]

    monkeypatch.setattr(retained, "_acl", acl)
    monkeypatch.setattr(retained, "_grant", grant)
    monkeypatch.setattr(retained, "_reader", reader)
    monkeypatch.setattr(retained, "_operator", AsyncMock(return_value='"admin"'))
    return calls_by_kind, state_by_field


@pytest.mark.parametrize("schema", [None, "network_bootstrap_" + "a" * 32, SCHEMA + "x"])
async def test_proof_namespace_accepts_only_candidate_schemas(schema):
    with pytest.raises(retained.RetainedReadAccessError):
        retained.proof_namespace(schema)
    assert retained.proof_namespace(SCHEMA) == PROOF_SCHEMA


async def test_extra_readers_need_a_matching_proof_acl_and_role_component(native):
    calls, state = native
    state["acl"] = _after()
    assert await retained.retained_extra_readers(_Connection(proof=_proof()), _entry()) == ("executor",)
    assert await retained.retained_extra_readers(_Connection(proof=_proof()), _entry("public")) == ()
    assert await retained.retained_extra_readers(_Connection(missing_namespace=True), _entry()) == ()
    state["reader"] = (42, "d" * 64)
    assert await retained.retained_extra_readers(_Connection(proof=_proof()), _entry()) == ()
    state["reader"], state["acl"] = (42, "c" * 64), BEFORE
    assert await retained.retained_extra_readers(_Connection(proof=_proof()), _entry()) == ()


@pytest.mark.parametrize(
    "connection",
    [
        _Connection(proof=_proof(), owner=6),
        _Connection(proof=_proof(), closed=False),
        _Connection(proof=_proof(contract="other")),
        _Connection(proof=_proof(manifest_sha256="e" * 64)),
        _Connection(proof=_proof(after=BEFORE)),
    ],
)
async def test_extra_readers_fail_closed_on_any_proof_difference(native, connection):
    native[1]["acl"] = _after()
    assert await retained.retained_extra_readers(connection, _entry()) == ()


async def test_invalid_proof_document_is_refused():
    connection = _Connection(proof=_proof())
    connection.row = {"nspowner": 5, "document": "{not json", "closed": True}
    with pytest.raises(retained.RetainedReadAccessError, match="invalid"):
        await retained._proof(connection, _receipt(), SCHEMA, MANIFEST)
    connection.row = None
    assert await retained._proof(connection, _receipt(), SCHEMA, MANIFEST) is None


def _pin():
    return PinnedAddressSource(SCHEMA, "entity_address_unified", MANIFEST)


@pytest.fixture
def pinned_reads(monkeypatch):
    read = AsyncMock(return_value=_entry())
    resolve = AsyncMock()
    monkeypatch.setattr(serving, "_read_manifest", read)
    monkeypatch.setattr(serving, "resolve_retained_network_address_source", resolve)
    return read, resolve


async def test_install_grants_records_the_proof_and_re_verifies(native, pinned_reads):
    calls, state = native
    connection = _Connection()
    proof = await retained.install_retained_read_access(
        connection, _pin(), reader_role="executor", control_schema="registry"
    )
    assert proof == _proof() and calls["grants"] == [("executor", False)]
    assert any(
        statement.startswith("CREATE SCHEMA") and PROOF_SCHEMA in statement for statement in connection.statements
    )
    assert pinned_reads[1].await_count == 2


async def test_install_re_arms_only_the_same_reader_from_the_original_acl(native, pinned_reads):
    calls, state = native
    connection = _Connection(proof=_proof(roles_sha256="0" * 64))
    proof = await retained.install_retained_read_access(
        connection, _pin(), reader_role="executor", control_schema="registry"
    )
    assert proof["roles_sha256"] == "c" * 64
    assert not any(statement.startswith("CREATE SCHEMA") for statement in connection.statements)
    with pytest.raises(retained.RetainedReadAccessError):
        await retained.install_retained_read_access(
            _Connection(proof=_proof()), _pin(), reader_role="other", control_schema="registry"
        )
    state["acl"] = _after()
    with pytest.raises(retained.RetainedReadAccessError):
        await retained.install_retained_read_access(
            _Connection(proof=_proof()), _pin(), reader_role="executor", control_schema="registry"
        )


async def test_revoke_restores_only_the_original_acl(native, pinned_reads):
    calls, state = native
    state["acl"] = _after()
    await retained.revoke_retained_read_access(
        _Connection(proof=_proof()), _pin(), reader_role="executor", control_schema="registry"
    )
    assert calls["grants"] == [("executor", True)] and state["acl"] == BEFORE
    await retained.revoke_retained_read_access(
        _Connection(proof=_proof()), _pin(), reader_role="executor", control_schema="registry"
    )
    assert calls["grants"] == [("executor", True)]
    for connection, reader in ((_Connection(), "executor"), (_Connection(proof=_proof()), "other")):
        with pytest.raises(retained.RetainedReadAccessError):
            await retained.revoke_retained_read_access(
                connection, _pin(), reader_role=reader, control_schema="registry"
            )
    state["acl"] = [["schema", 7, 0, 5, 99, "USAGE", False]]
    with pytest.raises(retained.RetainedReadAccessError):
        await retained.revoke_retained_read_access(
            _Connection(proof=_proof()), _pin(), reader_role="executor", control_schema="registry"
        )


async def test_unknown_retained_generation_is_refused(native, pinned_reads):
    with pytest.raises(retained.RetainedReadAccessError):
        await retained.revoke_retained_read_access(
            _Connection(proof=_proof(), generation=None), _pin(), reader_role="executor", control_schema="registry"
        )


@pytest.mark.parametrize(
    ("proof", "closed", "readers", "queries"),
    [(True, True, ("x",), 1), (False, False, ("x",), 1), (True, False, (), 1), (True, False, ("x",), 2)],
)
async def test_manifest_read_re_reads_only_for_a_proven_extra_reader(monkeypatch, proof, closed, readers, queries):
    first_by_column = {**_entry(), "retained_proof": proof}
    first_by_column["catalog_json"] = json.dumps(
        {**json.loads(first_by_column["catalog_json"]), "privileges_closed": closed}
    )
    read = AsyncMock(side_effect=[first_by_column, _entry()])
    monkeypatch.setattr(serving, "_read_manifest_with", read)
    monkeypatch.setattr(retained, "retained_extra_readers", AsyncMock(return_value=readers))
    await serving._read_manifest(object(), "registry", None)
    assert read.await_count == queries
    if queries == 2:
        assert read.await_args.args[2:] == (3, False, readers)
