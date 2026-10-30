# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact compact memberships, immutable reuse and bounded preparation."""

import asyncio
import hashlib
import os
import struct
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import network_membership_slices as slices
from process.network_address_projection import NetworkAddressProjectionError, PinnedAddressSource
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_slices import (
    AffectedMembershipBuckets,
    NetworkMembershipSliceError,
    PinnedMembershipSlices,
    prepare_network_membership_slices,
)


def _pure_slice_inputs():
    candidate = UUID(int=2)
    target = MembershipCopyTarget(
        str(UUID(int=10)), str(UUID(int=11)), str(UUID(int=12)), str(candidate), "network_candidate_" + candidate.hex
    )
    previous_id = UUID(int=1)
    previous = PinnedMembershipSlices(str(previous_id), "network_candidate_" + previous_id.hex, 101)
    affected = AffectedMembershipBuckets(target.schema_name, "affected_buckets", 102)
    source = PinnedAddressSource("retained_source", "addresses", "address-generation")
    return target, previous, affected, source


@pytest.mark.parametrize(
    "changes",
    [
        {"candidate_id": "invalid"},
        {"candidate_id": UUID(int=1)},
        {"candidate_id": str(UUID(int=0))},
        {"schema_name": "other_candidate"},
        {"ref_map_oid": True},
        {"ref_map_oid": 0},
        {"ref_map_oid": 2**32},
        {"slice_contract_version": True},
        {"slice_contract_version": 2},
    ],
)
def test_pure_slice_pin_requires_exact_candidate_relation_and_contract(changes):
    _, previous, _, _ = _pure_slice_inputs()
    with pytest.raises(NetworkMembershipSliceError, match="Pinned slice identity or contract"):
        replace(previous, **changes)


@pytest.mark.parametrize("oid", [True, 0, -1, 2**32])
def test_pure_affected_pin_requires_a_native_oid(oid):
    with pytest.raises(NetworkMembershipSliceError, match="Affected bucket relation identity"):
        AffectedMembershipBuckets("candidate", "affected", oid)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["previous", "affected"])
@pytest.mark.parametrize("fault", ["before_lock", "after_lock", "invalid_rows"])
async def test_pure_slice_pins_recheck_identity_under_lock_before_accepting_rows(kind, fault):
    target, previous, affected, _ = _pure_slice_inputs()
    oid = previous.ref_map_oid if kind == "previous" else affected.relation_oid
    results = {
        "before_lock": [oid + 1],
        "after_lock": [oid, oid + 1],
        "invalid_rows": [oid, oid, True],
    }[fault]
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=results), execute=AsyncMock())
    with pytest.raises(NetworkMembershipSliceError, match="changed|invalid"):
        if kind == "previous":
            await slices._pin_previous(connection, previous, target)
        else:
            await slices._pin_affected(connection, affected, target, previous)
    assert connection.fetchval.await_count == len(results)
    assert connection.execute.await_count == (0 if fault == "before_lock" else 1)
    if fault != "before_lock":
        expected_mode = "ACCESS SHARE" if kind == "previous" else "SHARE"
        assert f"IN {expected_mode} MODE" in connection.execute.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["self", "foreign_affected", "no_previous", "untyped_previous", "untyped_affected"])
async def test_pure_slice_reuse_requires_distinct_history_and_owned_change_set(fault):
    target, previous, affected, _ = _pure_slice_inputs()
    connection = SimpleNamespace(fetchval=AsyncMock(), execute=AsyncMock())
    with pytest.raises(NetworkMembershipSliceError, match="different immutable candidate|owned relation"):
        if fault in {"self", "untyped_previous"}:
            prior = PinnedMembershipSlices(target.candidate_id, target.schema_name, 101) if fault == "self" else {}
            await slices._pin_previous(connection, prior, target)
        else:
            selected = replace(affected, schema_name=previous.schema_name) if fault == "foreign_affected" else affected
            await slices._pin_affected(
                connection,
                {} if fault == "untyped_affected" else selected,
                target,
                None if fault == "no_previous" else previous,
            )
    connection.fetchval.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["target", "source", "same_schema"])
async def test_pure_slice_preparation_refuses_untrusted_or_self_address_source(fault):
    target, _, _, source = _pure_slice_inputs()
    connection = SimpleNamespace(is_in_transaction=lambda: True, transaction=AsyncMock())
    if fault == "same_schema":
        source = replace(source, schema_name=target.schema_name)
    with pytest.raises(NetworkMembershipSliceError, match="Trusted candidate|outside the slice candidate"):
        await prepare_network_membership_slices(
            connection, {} if fault == "target" else target, {} if fault == "source" else source
        )
    connection.transaction.assert_not_called()


@pytest.mark.asyncio
async def test_pure_slice_addresses_require_indexed_exact_binding_before_materializing():
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False), execute=AsyncMock())
    with pytest.raises(NetworkMembershipSliceError, match="native location primary key"):
        await slices._matched_facts(connection, '"candidate"', '"source".addresses', '"work"', {}, None)
    connection.execute.assert_not_awaited()
    with pytest.raises(NetworkMembershipSliceError, match="Compact office source differs"):
        target, _, _, source = _pure_slice_inputs()
        await slices._office_relation(connection, target, source, {})


def _bucket(key):
    return hashlib.sha256(key.encode()).digest()[0] & 63


def _digest(keys):
    digest = hashlib.sha256(b"network-office-slice-v1\0")
    for key in sorted(keys, key=lambda value: value.encode()):
        encoded = key.encode()
        digest.update(struct.pack("!i", len(encoded)) + encoded)
    return digest.hexdigest()


class _CopyObserver:
    def __init__(self, connection):
        self.connection = connection
        self.cursor_requests = []
        self.ref_page_rows = []
        self.edge_copy_queries = []

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def cursor(self, query, **kwargs):
        self.cursor_requests.append((query, kwargs))
        return self.connection.cursor(query, **kwargs)

    async def copy_records_to_table(self, table_name, **kwargs):
        self.ref_page_rows.append(len(kwargs["records"]))
        return await self.connection.copy_records_to_table(table_name, **kwargs)

    async def copy_from_query(self, query, *arguments, **kwargs):
        self.edge_copy_queries.append(query)
        return await self.connection.copy_from_query(query, *arguments, **kwargs)


@pytest.fixture
async def slice_db():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    source_schema = "network_slice_test_" + uuid4().hex
    schemas = [source_schema]
    db = SimpleNamespace(
        connection=connection,
        source_schema=source_schema,
        schemas=schemas,
        source=PinnedAddressSource(source_schema, "retained_addresses", "address-edition-a"),
        keys=["office-a", "office-b", "office-c", "office-d"],
        location_ids=[uuid4() for _ in range(4)],
    )
    try:
        await connection.execute(f'CREATE SCHEMA "{source_schema}"')
        await connection.execute(f"""CREATE TABLE "{source_schema}".network_membership_candidate(
            candidate_id uuid PRIMARY KEY,dataset_id uuid,schema_id uuid,producer_id uuid,schema_name text,
            state text,source_generations jsonb,expected_rows bigint,accepted_rows bigint)
        """)
        await connection.execute(f"""CREATE TABLE "{source_schema}".retained_addresses(
            location_key varchar(64) PRIMARY KEY,entity_type text NOT NULL,entity_id text NOT NULL,postal_address text)
        """)
        await connection.copy_records_to_table(
            "retained_addresses",
            schema_name=source_schema,
            records=[
                (key, "npi", provider, "synthetic address")
                for key, provider in zip(db.keys, ["provider-a", "provider-a", "provider-b", "provider-c"], strict=True)
            ],
        )
        yield db
    finally:
        try:
            for schema in reversed(schemas):
                await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        finally:
            await connection.close()


async def _candidate(db, pairs):
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        *[str(uuid4()) for _ in range(3)], str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    db.schemas.append(copy_target.schema_name)
    await db.connection.execute(f'CREATE SCHEMA "{copy_target.schema_name}"')
    await db.connection.execute(f"""CREATE TABLE "{copy_target.schema_name}".network_membership(
        network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,
        location_id uuid NOT NULL,evidence_id text NOT NULL)
    """)
    await db.connection.execute(f"""CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
        provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
        location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
        UNIQUE(provider_system,provider_id,location_id))
    """)
    providers = ["provider-a", "provider-a", "provider-b", "provider-c"]
    await db.connection.copy_records_to_table(
        "provider_location_binding",
        schema_name=copy_target.schema_name,
        records=[
            ("npi", provider, location_id, key, "npi", provider)
            for key, location_id, provider in zip(db.keys, db.location_ids, providers, strict=True)
        ],
    )
    await db.connection.copy_records_to_table(
        "network_membership",
        schema_name=copy_target.schema_name,
        records=[
            (network_id, "npi", providers[office], db.location_ids[office], f"evidence-{index}")
            for index, (network_id, office) in enumerate(pairs)
        ],
    )
    await db.connection.execute(
        f"INSERT INTO \"{db.source_schema}\".network_membership_candidate VALUES($1,$2,$3,$4,$5,'sealed',"
        "jsonb_build_object('unified_address',$6::text),$7,$7)",
        candidate_id,
        *[UUID(getattr(copy_target, name)) for name in ("dataset_id", "schema_id", "producer_id")],
        copy_target.schema_name,
        db.source.generation_id,
        len(pairs),
    )
    return copy_target


async def _prepare(db, target, **kwargs):
    return await prepare_network_membership_slices(
        db.connection, target, db.source, control_schema=db.source_schema, **kwargs
    )


async def _refs(db, target):
    rows = await db.connection.fetch(
        f'SELECT * FROM "{target.schema_name}".network_slice_ref ORDER BY network_id,office_bucket'
    )
    return {(entry["network_id"], entry["office_bucket"]): dict(entry) for entry in rows}


def _pin(target, receipt):
    return PinnedMembershipSlices(target.candidate_id, target.schema_name, receipt.ref_map_oid)


@pytest.mark.asyncio
async def test_exact_sites_digest_indexes_and_no_address_clone(slice_db):
    db = slice_db
    copy_target = await _candidate(db, [(42, 0), (7, 0), (42, 0), (88, 2)])
    before = await db.connection.fetch(f'SELECT * FROM "{db.source_schema}".retained_addresses ORDER BY location_key')
    address_oid = await db.connection.fetchval("SELECT to_regclass($1)::oid", db.source_schema + ".retained_addresses")
    async with db.connection.transaction():
        receipt = await _prepare(db, copy_target)
    refs = await _refs(db, copy_target)
    assert (receipt.membership_rows, receipt.distinct_memberships, receipt.matched_locations) == (4, 3, 2)
    assert (receipt.total_slices, receipt.changed_slices, receipt.reused_slices, receipt.copied_edge_rows) == (
        3,
        3,
        0,
        3,
    )
    for network_id, office in [(42, 0), (7, 0), (88, 2)]:
        entry = refs[(network_id, _bucket(db.keys[office]))]
        assert entry["content_sha256"] == _digest([db.keys[office]])
        assert entry["artifact_candidate_id"] == UUID(copy_target.candidate_id) and entry["row_count"] == 1
    assert (
        await db.connection.fetchval("SELECT to_regclass($1)", copy_target.schema_name + ".entity_address_unified")
        is None
    )
    assert (
        await db.connection.fetch(f'SELECT * FROM "{db.source_schema}".retained_addresses ORDER BY location_key')
        == before
    )
    assert (
        await db.connection.fetchval("SELECT to_regclass($1)::oid", db.source_schema + ".retained_addresses")
        == address_oid
    )
    indexes = await db.connection.fetch("SELECT indexdef FROM pg_indexes WHERE schemaname=$1", copy_target.schema_name)
    assert any("(location_key, slice_id)" in entry["indexdef"] for entry in indexes)


@pytest.mark.asyncio
async def test_reuse_add_remove_move_and_empty_bucket(slice_db):
    db = slice_db
    first = await _candidate(db, [(42, 0), (7, 0), (88, 2)])
    async with db.connection.transaction():
        first_receipt = await _prepare(db, first)
    original_refs = await _refs(db, first)
    original_edges = await db.connection.fetch(
        f'SELECT * FROM "{first.schema_name}".network_office_edge ORDER BY slice_id,location_key'
    )
    second = await _candidate(db, [(42, 0), (7, 1), (99, 3)])
    async with db.connection.transaction():
        second_receipt = await _prepare(db, second, previous=_pin(first, first_receipt))
    current_refs = await _refs(db, second)
    unchanged = (42, _bucket(db.keys[0]))
    assert current_refs[unchanged] == original_refs[unchanged]
    assert (88, _bucket(db.keys[2])) not in current_refs
    assert (second_receipt.reused_slices, second_receipt.changed_slices, second_receipt.copied_edge_rows) == (1, 2, 2)
    assert (
        await db.connection.fetch(
            f'SELECT * FROM "{first.schema_name}".network_office_edge ORDER BY slice_id,location_key'
        )
        == original_edges
    )
    assert (
        await db.connection.fetchval("SELECT to_regclass($1)::oid", first.schema_name + ".network_office_edge")
        == first_receipt.edge_heap_oid
    )
    third = await _candidate(db, [(42, 0), (7, 1), (99, 3)])
    async with db.connection.transaction():
        third_receipt = await _prepare(db, third, previous=_pin(second, second_receipt))
    assert await _refs(db, third) == current_refs
    assert (third_receipt.changed_slices, third_receipt.copied_edge_rows, third_receipt.reused_slices) == (0, 0, 3)
    empty = await _candidate(db, [])
    async with db.connection.transaction():
        empty_receipt = await _prepare(db, empty, previous=_pin(third, third_receipt))
    assert (empty_receipt.total_slices, empty_receipt.removed_slices, empty_receipt.copied_edge_rows) == (0, 3, 0)


@pytest.mark.asyncio
async def test_selective_buckets_retain_exact_unexamined_refs(slice_db):
    db = slice_db
    first = await _candidate(db, [(42, 0), (88, 2)])
    async with db.connection.transaction():
        first_receipt = await _prepare(db, first)
    prior = await _refs(db, first)
    second = await _candidate(db, [(42, 0), (99, 3)])
    await db.connection.execute(
        f'CREATE TABLE "{second.schema_name}".changed(network_id integer,office_bucket integer)'
    )
    await db.connection.copy_records_to_table(
        "changed",
        schema_name=second.schema_name,
        records=[
            (88, _bucket(db.keys[2])),
            (99, _bucket(db.keys[3])),
        ],
    )
    affected = AffectedMembershipBuckets(
        second.schema_name,
        "changed",
        await db.connection.fetchval("SELECT to_regclass($1)::oid", second.schema_name + ".changed"),
    )
    async with db.connection.transaction():
        receipt = await _prepare(db, second, previous=_pin(first, first_receipt), affected_buckets=affected)
    refs = await _refs(db, second)
    assert refs[(42, _bucket(db.keys[0]))] == prior[(42, _bucket(db.keys[0]))]
    assert (receipt.reused_slices, receipt.changed_slices, receipt.removed_slices, receipt.copied_edge_rows) == (
        1,
        1,
        1,
        1,
    )
    assert (receipt.membership_rows, receipt.distinct_memberships, receipt.matched_locations) == (2, 2, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["wrong_entity", "missing_site", "invalid_network", "accounting"])
async def test_validation_failure_rolls_back_only_local_preparation(slice_db, damage):
    db = slice_db
    target = await _candidate(db, [(42, 0)])
    if damage in {"wrong_entity", "missing_site"}:
        value = (
            "entity_id='different-provider'"
            if damage == "wrong_entity"
            else "location_id='00000000-0000-0000-0000-000000000001'"
        )
        await db.connection.execute(
            f'UPDATE "{target.schema_name}".provider_location_binding SET {value} WHERE location_id=$1',
            db.location_ids[0],
        )
    elif damage == "invalid_network":
        await db.connection.execute(f'UPDATE "{target.schema_name}".network_membership SET network_id=0')
    else:
        await db.connection.execute(f'UPDATE "{db.source_schema}".network_membership_candidate SET accepted_rows=2')
    async with db.connection.transaction():
        with pytest.raises(NetworkMembershipSliceError):
            await _prepare(db, target)
        assert await db.connection.fetchval("SELECT to_regclass($1)", target.schema_name + ".network_slice_ref") is None
        assert await db.connection.fetchval("SELECT 1") == 1
    assert await db.connection.fetchval(f'SELECT count(*) FROM "{target.schema_name}".network_membership') == 1


@pytest.mark.asyncio
async def test_requires_transaction_pinned_generation_and_prior_oid(slice_db):
    db = slice_db
    first = await _candidate(db, [(42, 0)])
    with pytest.raises(NetworkMembershipSliceError, match="caller-owned"):
        await _prepare(db, first)
    async with db.connection.transaction():
        with pytest.raises(NetworkAddressProjectionError, match="generation mismatch"):
            await prepare_network_membership_slices(
                db.connection, first, replace(db.source, generation_id="different"), control_schema=db.source_schema
            )
        receipt = await _prepare(db, first)
    second = await _candidate(db, [(42, 0)])
    async with db.connection.transaction():
        with pytest.raises(NetworkMembershipSliceError, match="relation changed"):
            await _prepare(db, second, previous=replace(_pin(first, receipt), ref_map_oid=receipt.edge_heap_oid))
        assert await db.connection.fetchval("SELECT to_regclass($1)", second.schema_name + ".network_slice_ref") is None


@pytest.mark.asyncio
async def test_bounded_pages_and_statement_count_is_not_per_slice(slice_db, monkeypatch):
    import process.network_membership_slices as slices

    db = slice_db
    monkeypatch.setattr(slices, "_BATCH_ROWS", 7)
    monkeypatch.setattr(slices, "_METADATA_BATCH_ROWS", 11)
    pairs = [(network_id, office) for network_id in range(1, 37) for office in range(4)]
    copy_target = await _candidate(db, pairs)
    queries = []
    db.connection.add_query_logger(queries.append)
    db.connection = _CopyObserver(db.connection)
    async with db.connection.transaction():
        receipt = await _prepare(db, copy_target)
        await asyncio.sleep(0)
    assert receipt.copied_edge_rows == 144 and receipt.total_slices == len(
        {(network_id, _bucket(db.keys[office])) for network_id, office in pairs}
    )
    assert len(db.connection.cursor_requests) == 1
    assert db.connection.cursor_requests[0][1] == {"prefetch": 7}
    assert max(db.connection.ref_page_rows) <= 11
    assert len(db.connection.edge_copy_queries) == 21
    assert not any("SELECT convert_to(location_key" in query.query for query in queries)
    print(
        {
            "slice_count": receipt.total_slices,
            "edge_rows": receipt.copied_edge_rows,
            "digest_statements": len(db.connection.cursor_requests),
            "metadata_copy_pages": len(db.connection.ref_page_rows),
            "edge_copy_batches": len(db.connection.edge_copy_queries),
            "logged_statements": len(queries),
            "copy_row_bound": slices._BATCH_ROWS,
        }
    )


@pytest.mark.asyncio
async def test_digest_uses_utf8_byte_lengths_and_c_order(slice_db):
    db = slice_db
    first = "office-α"
    second = next(f"office-é-{index}" for index in range(1000) if _bucket(f"office-é-{index}") == _bucket(first))
    db.keys[0], db.keys[1] = first, second
    await db.connection.execute(
        f'UPDATE "{db.source_schema}".retained_addresses SET location_key=$1 WHERE location_key=$2', first, "office-a"
    )
    await db.connection.execute(
        f'UPDATE "{db.source_schema}".retained_addresses SET location_key=$1 WHERE location_key=$2', second, "office-b"
    )
    target = await _candidate(db, [(42, 1), (42, 0), (42, 0)])
    async with db.connection.transaction():
        receipt = await _prepare(db, target)
    refs = await _refs(db, target)
    assert receipt.total_slices == 1 and refs[(42, _bucket(first))]["row_count"] == 2
    assert refs[(42, _bucket(first))]["content_sha256"] == _digest([first, second])


@pytest.mark.asyncio
async def test_copy_failure_can_resume_same_candidate(slice_db, monkeypatch):
    import process.network_bootstrap_sources as bootstrap

    db = slice_db
    target = await _candidate(db, [(42, 0), (7, 1)])
    original_copy = bootstrap._copy_native_batch

    async def fail_after_copy(*arguments, **kwargs):
        await original_copy(*arguments, **kwargs)
        raise bootstrap.NetworkBootstrapSourceError("synthetic copy failure")

    async with db.connection.transaction():
        with monkeypatch.context() as patch:
            patch.setattr(bootstrap, "_copy_native_batch", fail_after_copy)
            with pytest.raises(NetworkMembershipSliceError, match="synthetic copy failure"):
                await _prepare(db, target)
        for name in ("network_slice_ref", "network_office_edge"):
            assert await db.connection.fetchval("SELECT to_regclass($1)", target.schema_name + "." + name) is None
        receipt = await _prepare(db, target)
    assert receipt.copied_edge_rows == 2
