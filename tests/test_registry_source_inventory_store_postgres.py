# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private native inventory roundtrips; metadata rows do not prove admitted Sources or custody."""

import asyncio
import hashlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import registry_source_inventory_store as store
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import with_fhir_successor
from process.network_fhir_retained_capture_contract import SEMANTIC_PROFILE, capture_digest
from process.network_membership_candidate_lifecycle import (
    MembershipCandidateError,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_inventory_contract import (
    RegistrySourceInventoryError,
    SourceInventoryAccumulator,
    encode_source_inventory_entry,
    make_source_inventory_entry,
    source_inventory_root,
)
from process.registry_source_inventory_reference import SourceInventoryReference
from process.registry_source_inventory_summaries import contribution_summary_v3
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes
from tests.registry_compact_target_postgres_support import coverage_reader
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe
from tests.test_registry_source_inventory_contract import _entries
from tests.test_registry_source_inventory_schema_postgres import _migration
from tests.test_registry_source_recipe_store_postgres import _cms_custody_recipe, _cms_epoch_recipe, _example


def _capture_recipe():
    captures = []
    for dataset in ("dataset-one", "dataset-two"):
        capture = retained()
        origin = replace(
            capture.origin,
            dataset_id=dataset,
            admission_profile=SEMANTIC_PROFILE,
            original_admission_refusal=None,
            admission_metadata_sha256="1" * 64,
            semantic_projection_as_of="2026-01-01",
            descriptor_counts=capture.origin.resource_counts,
        )
        capture = replace(capture, origin=origin)
        captures.append(replace(capture, capture_sha256=capture_digest(capture)))
    source = with_fhir_successor(pin(captures[0]), pin(captures[1]))
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source.source_id, source.schema_name, source.dataset_id, "fixture-producer", captures[0].capture_id
    )
    return RegistrySourceMembershipRecipe(source, coordinates)


@pytest.mark.parametrize("recipe", [_example(), _cms_epoch_recipe(), _ptg_recipe(), _capture_recipe()])
def test_inventory_store_typed_values_come_only_from_the_existing_recipe_codec(recipe):
    entry = make_source_inventory_entry(1, recipe)
    values = store.source_inventory_record(entry)
    record_by_field = dict(zip(store._COLUMNS, values, strict=True))
    assert store.source_inventory_entry(record_by_field) == entry
    assert (
        record_by_field["recipe_sha256"]
        == hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    )
    assert tuple(record_by_field[field] for field in store._SCOPE_FIELDS) == recipe.binding_coordinates.sql_parameters
    if recipe.binding_coordinates.source_system != "fhir":
        assert record_by_field["fhir_scope_key"] is record_by_field["fhir_scope_value"] is None
    elif recipe.source_pin.retained_capture is None:
        assert (record_by_field["fhir_scope_key"], record_by_field["fhir_scope_value"]) == (
            "alias_scope",
            recipe.source_pin.alias_scope,
        )
    else:
        assert record_by_field["fhir_scope_key"] == "capture_sha256"
        assert record_by_field["fhir_scope_value"] == recipe.source_pin.retained_capture.capture_sha256
        assert (
            record_by_field["fhir_scope_value"] != recipe.source_pin.retained_successor.retained_capture.capture_sha256
        )


@pytest.mark.parametrize("field", store._DERIVED_FIELDS)
def test_inventory_store_refuses_typed_column_substitution_even_with_unchanged_canonical_root(field):
    entry = make_source_inventory_entry(1, _example())
    record_by_field = dict(zip(store._COLUMNS, store.source_inventory_record(entry), strict=True))
    record_by_field[field] = "changed"
    with pytest.raises(RegistrySourceInventoryError, match="stored_row_invalid"):
        store.source_inventory_entry(record_by_field)


def test_encoded_payload_budget_and_native_transfer_ceiling_are_independent():
    entry = make_source_inventory_entry(1, _example())
    derived_bytes = store.source_inventory_record_bytes(entry) - len(encode_source_inventory_entry(entry))
    maximum_by_field = {
        "ordinal": 1,
        "encoded_bytes": store.MAX_INVENTORY_PAGE_BYTES - 2,
        "transfer_bytes": store.MAX_INVENTORY_PAGE_BYTES - 2 + derived_bytes,
    }
    assert store._fitted_metadata((maximum_by_field,), 1, store.MAX_INVENTORY_PAGE_BYTES) == [maximum_by_field]
    with pytest.raises(RegistrySourceInventoryError, match="page_too_small"):
        store._fitted_metadata((maximum_by_field,), 1, store.MAX_INVENTORY_PAGE_BYTES - 1)
    metadata = [dict(ordinal=index, encoded_bytes=1300, transfer_bytes=2000) for index in range(1, 601)]
    fitted = store._fitted_metadata(metadata, 600, store.MAX_INVENTORY_PAGE_BYTES)
    assert 1 < len(fitted) < 600
    assert sum(row["transfer_bytes"] for row in fitted) + store._NATIVE_PROTOCOL_RESERVE <= store.MAX_NATIVE_WIRE_BYTES


@pytest.mark.asyncio
async def test_async_inventory_entry_adapter_yields_without_prefetching():
    entries = _entries(_example(index) for index in range(101))
    events = []

    async def source_entries():
        for entry in entries:
            await asyncio.sleep(0)
            events.append(("produced", entry.ordinal))
            yield entry

    async for entry in store._inventory_entries(source_entries()):
        events.append(("consumed", entry.ordinal))
    assert events == [
        event for entry in entries for event in (("produced", entry.ordinal), ("consumed", entry.ordinal))
    ]
    assert [entry async for entry in store._inventory_entries(iter(entries))] == list(entries)


@pytest.mark.asyncio
@pytest.mark.parametrize("after,ordinals", [(0, (1, 1)), (1, (2, 2)), (0, (1, 3))])
async def test_raw_page_refuses_duplicate_or_gapped_scalar_lookahead_before_payload(after, ordinals):
    metadata = [dict(ordinal=ordinal, encoded_bytes=800, transfer_bytes=1000) for ordinal in ordinals]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=metadata))
    with pytest.raises(RegistrySourceInventoryError, match="stored_ordinals_invalid"):
        await store._read_page(connection, '"fixture".network_source_inventory', after, 1, 1048576)
    assert connection.fetch.await_count == 1


@pytest.fixture
async def inventory_db(custom_db):
    context = SimpleNamespace(fixture=custom_db, targets=[])
    try:
        await custom_db.connection.execute(_migration()._ddl(custom_db.control_schema))
        async with coverage_reader(context):
            yield context
    finally:
        for copy_target in reversed(context.targets):
            await custom_db.connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
            assert await custom_db.connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None


async def _new_target(context, entries=()):
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    context.targets.append(copy_target)
    async with context.fixture.connection.transaction():
        await create_network_candidate(
            context.fixture.connection,
            copy_target,
            source_generations={},
            approved_custom_revision=0,
            expected_head=0,
            expected_rows=0,
            control_schema=context.fixture.control_schema,
            source_inventory=SourceInventoryReference(source_inventory_root(entries)),
            source_contributions=contribution_summary_v3(source_inventory_root(entries), ()) if not entries else None,
        )
    return copy_target


async def _asynchronous_entries(entries):
    for offset in range(0, len(entries), 7):
        await asyncio.sleep(0)
        for entry in entries[offset : offset + 7]:
            yield entry


async def _write(context, copy_target, entries, **options):
    async with context.fixture.connection.transaction():
        receipt = await store.write_source_inventory(
            context.fixture.connection,
            copy_target,
            _asynchronous_entries(entries),
            expected_root=source_inventory_root(entries),
            control_schema=context.fixture.control_schema,
            **options,
        )
    reader = context.fixture.roles["reader"]
    await context.fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{copy_target.schema_name}" TO "{reader}"')
    await context.fixture.connection.execute(
        f'GRANT SELECT ON "{copy_target.schema_name}".{store.INVENTORY_TABLE} TO "{reader}"'
    )
    return receipt


async def _verify(context, copy_target, receipt, **options):
    async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
        return await store.verify_source_inventory(
            context.coverage_reader,
            copy_target,
            expected_table_oid=receipt.table_oid,
            expected_root=receipt.root,
            control_schema=context.fixture.control_schema,
            **options,
        )


@pytest.mark.parametrize("count", [0, 101])
async def test_open_inventory_replay_preserves_rows_and_rejects_changed_stream(inventory_db, count):
    context = inventory_db
    entries = _entries(_example(index) for index in range(count))
    copy_target = await _new_target(context, entries)
    receipt = await _write(context, copy_target, entries)
    relation = '"' + copy_target.schema_name + '".' + store.INVENTORY_TABLE
    before = await context.fixture.connection.fetch(
        f"SELECT xmin::text,ctid::text,to_jsonb(item)::text FROM {relation} AS item ORDER BY ordinal"
    )
    assert await _write(context, copy_target, entries) == receipt
    assert (
        await context.fixture.connection.fetch(
            f"SELECT xmin::text,ctid::text,to_jsonb(item)::text FROM {relation} AS item ORDER BY ordinal"
        )
        == before
    )
    assert (
        await context.fixture.connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation) == receipt.table_oid
    )
    candidate_record = await context.fixture.connection.fetchrow(
        f'SELECT * FROM "{context.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(copy_target.candidate_id),
    )
    candidate_by_field = dict(candidate_record)
    async with context.fixture.connection.transaction(isolation="repeatable_read"):
        assert (
            await store._registered_inventory_entries(
                context.fixture.connection, copy_target, candidate_record, control_schema=context.fixture.control_schema
            )
            == entries
        )
        loaded, stored_entries = await store._load_locked_source_inventory(
            context.fixture.connection,
            copy_target,
            candidate_by_field,
            expected_table_oid=receipt.table_oid,
            control_schema=context.fixture.control_schema,
        )
        assert loaded.root == receipt.root and stored_entries == entries
    async with context.fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(RegistrySourceInventoryError, match="physical_identity_invalid"):
            await store._load_locked_source_inventory(
                context.fixture.connection,
                copy_target,
                candidate_by_field,
                expected_table_oid=receipt.table_oid + 1,
                control_schema=context.fixture.control_schema,
            )
    if count:
        with pytest.raises(RegistrySourceInventoryError, match="root_changed"):
            async with context.fixture.connection.transaction():
                await store.write_source_inventory(
                    context.fixture.connection,
                    copy_target,
                    iter(entries[:-1]),
                    expected_root=receipt.root,
                    control_schema=context.fixture.control_schema,
                )
        assert await _verify(context, copy_target, receipt) == receipt.root


@pytest.mark.asyncio
async def test_native_inventory_copy_and_limited_reader_keysets_preserve_complete_root(inventory_db):
    context = inventory_db
    entries = _entries(
        (*(_example(index) for index in range(98)), _cms_custody_recipe(), _cms_epoch_recipe(), _ptg_recipe())
    )
    copy_target = await _new_target(context, entries)
    receipt = await _write(context, copy_target, entries, row_limit=7, byte_limit=4096)
    assert receipt.root.entry_count == 101 and await _verify(context, copy_target, receipt) == receipt.root
    accumulator, after, page_count = SourceInventoryAccumulator(), 0, 0
    async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
        while True:
            page = await store.read_source_inventory_page(
                context.coverage_reader,
                copy_target,
                expected_table_oid=receipt.table_oid,
                after_ordinal=after,
                row_limit=17,
                byte_limit=4096,
                control_schema=context.fixture.control_schema,
            )
            assert len(page.payload) <= 4096 and len(page.entries) <= 17
            for entry in page.entries:
                accumulator.accept(entry)
            page_count += 1
            if page.next_after_ordinal is None:
                break
            after = page.next_after_ordinal
    assert page_count > 1 and accumulator.finish() == receipt.root
    async with context.coverage_reader.transaction():
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with context.coverage_reader.transaction():
                await context.coverage_reader.execute(
                    f'UPDATE "{copy_target.schema_name}".{store.INVENTORY_TABLE} SET ordinal=ordinal'
                )
    assert (
        await context.fixture.connection.fetchval(
            "SELECT count(*) FROM pg_constraint WHERE conrelid=$1::oid AND contype='f'", receipt.table_oid
        )
        == 0
    )
    assert (
        await context.fixture.connection.fetchval(
            "SELECT count(*) FROM pg_trigger WHERE tgrelid=$1::oid AND NOT tgisinternal", receipt.table_oid
        )
        == 0
    )


async def _restore_row(context, copy_target, entry):
    relation = '"' + copy_target.schema_name + '".' + store.INVENTORY_TABLE
    async with context.fixture.connection.transaction():
        await context.fixture.connection.execute(f"DELETE FROM {relation} WHERE ordinal=$1", entry.ordinal)
        await context.fixture.connection.copy_records_to_table(
            store.INVENTORY_TABLE,
            schema_name=copy_target.schema_name,
            columns=store._COLUMNS,
            records=[store.source_inventory_record(entry)],
        )


@pytest.mark.asyncio
async def test_native_inventory_verifies_committed_gaps_derived_values_and_complete_row_root(inventory_db):
    context = inventory_db
    entries = _entries((_example(0), _example(1), _example(2)))
    copy_target = await _new_target(context, entries)
    receipt = await _write(context, copy_target, entries)
    relation = '"' + copy_target.schema_name + '".' + store.INVENTORY_TABLE
    for kind in ("gap", "derived", "substitution"):
        damaged_entry = entries[-1] if kind == "substitution" else entries[1]
        try:
            if kind == "gap":
                await context.fixture.connection.execute(f"DELETE FROM {relation} WHERE ordinal=2")
            elif kind == "derived":
                await context.fixture.connection.execute(f"UPDATE {relation} SET source_id='changed' WHERE ordinal=2")
            else:
                substituted = make_source_inventory_entry(damaged_entry.ordinal, _example(99))
                await _restore_row(context, copy_target, substituted)
            with pytest.raises(RegistrySourceInventoryError, match="root_changed" if kind == "substitution" else None):
                await _verify(context, copy_target, receipt, row_limit=1)
        finally:
            await _restore_row(context, copy_target, damaged_entry)
        assert await _verify(context, copy_target, receipt) == receipt.root


@pytest.mark.asyncio
async def test_native_inventory_full_root_refuses_extra_zero_row_with_intact_pk(inventory_db):
    context = inventory_db
    entries = _entries((_example(0), _example(1), _example(2)))
    copy_target = await _new_target(context, entries)
    receipt = await _write(context, copy_target, entries)
    relation = '"' + copy_target.schema_name + '".' + store.INVENTORY_TABLE
    constraint = await context.fixture.connection.fetchrow(
        "SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint "
        "WHERE conrelid=$1::oid AND contype='c' AND conkey=ARRAY[1]::smallint[]",
        receipt.table_oid,
    )
    primary_key = await context.fixture.connection.fetchval(
        "SELECT oid FROM pg_constraint WHERE conrelid=$1::oid AND contype='p'", receipt.table_oid
    )
    assert constraint is not None and primary_key is not None
    try:
        await context.fixture.connection.execute(f'ALTER TABLE {relation} DROP CONSTRAINT "{constraint["conname"]}"')
        extra = store.source_inventory_record(make_source_inventory_entry(4, _example(99)))
        await context.fixture.connection.copy_records_to_table(
            store.INVENTORY_TABLE,
            schema_name=copy_target.schema_name,
            columns=store._COLUMNS,
            records=[(0, *extra[1:])],
        )
        assert await context.fixture.connection.fetchval(f"SELECT count(*) FROM {relation} WHERE ordinal>0") == 3
        assert (
            await context.fixture.connection.fetchval(
                "SELECT oid FROM pg_constraint WHERE conrelid=$1::oid AND contype='p'", receipt.table_oid
            )
            == primary_key
        )
        with pytest.raises(RegistrySourceInventoryError, match="root_changed"):
            await _verify(context, copy_target, receipt)
    finally:
        await context.fixture.connection.execute(f"DELETE FROM {relation} WHERE ordinal=0")
        if not await context.fixture.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=$1::oid AND conname=$2)",
            receipt.table_oid,
            constraint["conname"],
        ):
            await context.fixture.connection.execute(
                f'ALTER TABLE {relation} ADD CONSTRAINT "{constraint["conname"]}" {constraint["definition"]}'
            )
    assert await _verify(context, copy_target, receipt) == receipt.root


@pytest.mark.asyncio
async def test_native_inventory_duplicate_scalar_boundary_refuses_before_payload(inventory_db):
    context = inventory_db
    entries = _entries((_example(0), _example(1), _example(2)))
    copy_target = await _new_target(context, entries)
    receipt = await _write(context, copy_target, entries)
    relation = '"' + copy_target.schema_name + '".' + store.INVENTORY_TABLE
    primary_key = await context.fixture.connection.fetchrow(
        "SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint "
        "WHERE conrelid=$1::oid AND contype='p'",
        receipt.table_oid,
    )
    extra = make_source_inventory_entry(2, _example(99))
    extra_record = store.source_inventory_record(extra)
    try:
        await context.fixture.connection.execute(f'ALTER TABLE {relation} DROP CONSTRAINT "{primary_key["conname"]}"')
        await context.fixture.connection.copy_records_to_table(
            store.INVENTORY_TABLE,
            schema_name=copy_target.schema_name,
            columns=store._COLUMNS,
            records=[extra_record],
        )
        queries = []
        async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
            with context.coverage_reader.query_logger(queries.append):
                with pytest.raises(RegistrySourceInventoryError, match="stored_ordinals_invalid"):
                    await store.read_source_inventory_page(
                        context.coverage_reader,
                        copy_target,
                        expected_table_oid=receipt.table_oid,
                        after_ordinal=1,
                        row_limit=1,
                        byte_limit=max(len(encode_source_inventory_entry(entry)) for entry in (entries[1], extra)) + 2,
                        control_schema=context.fixture.control_schema,
                    )
                await asyncio.sleep(0)
        assert not any(f"SELECT {store._SELECT_COLUMNS} FROM" in query.query for query in queries)
    finally:
        await context.fixture.connection.execute(f"DELETE FROM {relation} WHERE recipe_sha256=$1", extra_record[4])
        if not await context.fixture.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=$1::oid AND conname=$2)",
            receipt.table_oid,
            primary_key["conname"],
        ):
            await context.fixture.connection.execute(
                f'ALTER TABLE {relation} ADD CONSTRAINT "{primary_key["conname"]}" {primary_key["definition"]}'
            )
    assert await _verify(context, copy_target, receipt) == receipt.root


@pytest.mark.asyncio
async def test_native_inventory_write_savepoint_restores_whole_inventory_after_late_failure(inventory_db):
    context = inventory_db
    entries = _entries(_example(index) for index in range(101))
    copy_target = await _new_target(context, entries)
    expected = source_inventory_root(entries)
    other_entries = _entries(_example(index) for index in (*range(100), 999))

    async def interrupted_entries():
        async for entry in _asynchronous_entries(entries):
            yield entry
            if entry.ordinal == 100:
                raise RuntimeError("synthetic interruption after many native COPY pages")

    for supplied, root, error in (
        (interrupted_entries(), expected, RuntimeError),
        (iter(other_entries), expected, RegistrySourceInventoryError),
    ):
        async with context.fixture.connection.transaction():
            with pytest.raises(error):
                await store.write_source_inventory(
                    context.fixture.connection,
                    copy_target,
                    supplied,
                    expected_root=root,
                    row_limit=1,
                    control_schema=context.fixture.control_schema,
                )
            assert (
                await context.fixture.connection.fetchval(
                    "SELECT to_regclass($1)", copy_target.schema_name + "." + store.INVENTORY_TABLE
                )
                is None
            )
    assert (await _write(context, copy_target, entries)).root == expected


async def _assert_sql_sizes(context, copy_target, entry):
    sizes = await context.coverage_reader.fetchrow(
        f"SELECT ({store._ENCODED_ENTRY_BYTES_SQL}) AS encoded_bytes,"
        f"({store._TRANSFER_BYTES_SQL}) AS transfer_bytes "
        f'FROM "{copy_target.schema_name}".{store.INVENTORY_TABLE} WHERE ordinal=1'
    )
    assert sizes["encoded_bytes"] == len(encode_source_inventory_entry(entry))
    assert sizes["transfer_bytes"] == store.source_inventory_record_bytes(entry)


async def _assert_reader_boundaries(context, copy_target, receipt, entries):
    wrong_target = replace(copy_target, producer_id=str(uuid4()))
    async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
        exact_page = await store.read_source_inventory_page(
            context.coverage_reader,
            copy_target,
            expected_table_oid=receipt.table_oid,
            byte_limit=len(encode_source_inventory_entry(entries[0])) + 2,
            control_schema=context.fixture.control_schema,
        )
        assert exact_page.entries == entries and len(exact_page.payload) > len(exact_page.payload.decode())
        await _assert_sql_sizes(context, copy_target, entries[0])
        with pytest.raises(RegistrySourceInventoryError, match="scope_invalid"):
            await store.read_source_inventory_page(
                context.coverage_reader,
                wrong_target,
                expected_table_oid=receipt.table_oid,
                control_schema=context.fixture.control_schema,
            )
        with pytest.raises(RegistrySourceInventoryError, match="physical_identity"):
            await store.read_source_inventory_page(
                context.coverage_reader,
                copy_target,
                expected_table_oid=receipt.table_oid + 1,
                control_schema=context.fixture.control_schema,
            )
        queries = []
        with context.coverage_reader.query_logger(queries.append):
            with pytest.raises(RegistrySourceInventoryError, match="page_too_small"):
                await store.read_source_inventory_page(
                    context.coverage_reader,
                    copy_target,
                    expected_table_oid=receipt.table_oid,
                    byte_limit=len(encode_source_inventory_entry(entries[0])) + 1,
                    control_schema=context.fixture.control_schema,
                )
            await asyncio.sleep(0)
        assert not any(f"SELECT {store._SELECT_COLUMNS} FROM" in query.query for query in queries)


@pytest.mark.asyncio
async def test_native_inventory_scope_state_oid_snapshot_and_first_row_bounds(inventory_db):
    context = inventory_db
    original = _example()
    unicode_recipe = RegistrySourceMembershipRecipe(
        replace(original.source_pin, source_id='source-é😀"\\id'),
        replace(original.binding_coordinates, source_id='source-é😀"\\id'),
    )
    entries = _entries((unicode_recipe,))
    copy_target = await _new_target(context, entries)
    wrong_target = replace(copy_target, producer_id=str(uuid4()))
    async with context.fixture.connection.transaction():
        with pytest.raises(MembershipCandidateError, match="scope"):
            await store.write_source_inventory(
                context.fixture.connection,
                wrong_target,
                entries,
                expected_root=source_inventory_root(entries),
                control_schema=context.fixture.control_schema,
            )
    receipt = await _write(context, copy_target, entries)
    await _assert_reader_boundaries(context, copy_target, receipt, entries)
    async with context.coverage_reader.transaction():
        with pytest.raises(RegistrySourceInventoryError, match="snapshot"):
            await store.read_source_inventory_page(
                context.coverage_reader,
                copy_target,
                expected_table_oid=receipt.table_oid,
                control_schema=context.fixture.control_schema,
            )
    closed = await _new_target(context)
    await _write(context, closed, ())
    async with context.fixture.connection.transaction(isolation="repeatable_read"):
        await seal_network_candidate(context.fixture.connection, closed, control_schema=context.fixture.control_schema)
        with pytest.raises(MembershipCandidateError, match="closed"):
            await store.write_source_inventory(
                context.fixture.connection,
                closed,
                entries,
                expected_root=source_inventory_root(entries),
                control_schema=context.fixture.control_schema,
            )
