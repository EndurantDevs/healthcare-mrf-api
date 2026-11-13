# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named inventory reads preserve complete roots, logical scope and bounded payload transfer."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_source_inventory_store as store
from process import registry_source_recipe_store as recipes
from tests.test_registry_source_inventory_consumers import CONTROL, _inventory

pytestmark = pytest.mark.asyncio


class _NativeRow(dict):
    def __iter__(self):
        return iter(self.values())


class _InventoryConnection:
    def __init__(self, fixture):
        self.fixture, self.events = fixture, []
        self.modes = ("repeatable read", "on")
        self.registered_candidate = dict(fixture.candidate_by_field)
        self.execute = AsyncMock()
        self.rows = [
            dict(zip(store._COLUMNS, store.source_inventory_record(entry), strict=True)) for entry in fixture.entries
        ]

    def is_in_transaction(self):
        return True

    async def fetchval(self, query, *arguments):
        assert query == "SHOW transaction_isolation" and not arguments
        return self.modes[0]

    async def fetchrow(self, query, *arguments):
        if "transaction_isolation" in query:
            return self.modes
        if "SELECT dataset_id" in query:
            return self.registered_candidate
        assert "SELECT count(*)" in query
        self.events.append("count")
        return _NativeRow(total=len(self.rows), invalid=0)

    async def fetch(self, query, *arguments):
        if "AS encoded_bytes" in query:
            self.events.append("preflight")
            after, limit = arguments
            return [
                dict(
                    ordinal=entry.ordinal,
                    encoded_bytes=len(store.encode_source_inventory_entry(entry)),
                    transfer_bytes=store.source_inventory_record_bytes(entry),
                )
                for entry in self.fixture.entries
                if entry.ordinal > after
            ][:limit]
        self.events.append("payload")
        after, last = arguments
        return [row for row in self.rows if after < row["ordinal"] <= last]


@pytest.mark.parametrize("count", [0, 1, 101, 1000])
async def test_api_loader_preserves_complete_root(monkeypatch, count):
    fixture = _inventory(count)
    connection = _InventoryConnection(fixture)
    inventory, entries = await store.load_source_inventory_for_api(
        connection, fixture.copy_target, fixture.candidate_by_field, control_schema=CONTROL
    )
    assert inventory == fixture.reference and entries == fixture.entries
    assert connection.events[0] == "count"
    assert "payload" in connection.events if count else "payload" not in connection.events
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["ordinal", "json_bytes", "wire_bytes"])
async def test_invalid_preflight_refuses_before_payload_decode(monkeypatch, damage):
    fixture = _inventory()
    entry = fixture.entries[0]
    metadata = dict(
        ordinal=1,
        encoded_bytes=len(store.encode_source_inventory_entry(entry)),
        transfer_bytes=store.source_inventory_record_bytes(entry),
    )
    metadata[{"ordinal": "ordinal", "json_bytes": "encoded_bytes", "wire_bytes": "transfer_bytes"}[damage]] = {
        "ordinal": -1,
        "json_bytes": store.MAX_INVENTORY_PAGE_BYTES,
        "wire_bytes": store.MAX_NATIVE_WIRE_BYTES,
    }[damage]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[metadata]))
    decoder = Mock(side_effect=AssertionError("invalid payload decoded"))
    monkeypatch.setattr(store, "source_inventory_entry", decoder)
    with pytest.raises(store.RegistrySourceInventoryError):
        await store._read_page(connection, '"candidate".network_source_inventory', 0, 100, 1048576)
    decoder.assert_not_called()
    connection.fetch.assert_awaited_once()


async def test_empty_inventory_has_no_payload_decode(monkeypatch):
    fixture = _inventory(0)
    connection = _InventoryConnection(fixture)
    decoder = Mock(side_effect=AssertionError("empty inventory decoded"))
    monkeypatch.setattr(store, "source_inventory_entry", decoder)
    assert await store.load_source_inventory_for_api(
        connection, fixture.copy_target, fixture.candidate_by_field, control_schema=CONTROL
    ) == (fixture.reference, ())
    assert connection.events == ["count", "preflight"]
    decoder.assert_not_called()


async def test_no_complete_inventory_escapes_root_mismatch(monkeypatch):
    fixture = _inventory()
    connection = _InventoryConnection(fixture)
    replacement = _inventory(2).entries[1]
    fixture.entries = (replace(replacement, ordinal=1),)
    connection.rows = [dict(zip(store._COLUMNS, store.source_inventory_record(fixture.entries[0]), strict=True))]
    with pytest.raises(store.RegistrySourceInventoryError, match="root_changed"):
        await store.load_source_inventory_for_api(
            connection, fixture.copy_target, fixture.candidate_by_field, control_schema=CONTROL
        )
    assert connection.events[-1] == "payload"


@pytest.mark.parametrize("damage", ["mode", "missing", "scope", "root"])
async def test_metadata_refusal_precedes_inventory_data(monkeypatch, damage):
    fixture = _inventory()
    connection = _InventoryConnection(fixture)
    if damage == "mode":
        connection.modes = ("repeatable read", "off")
    elif damage == "missing":
        connection.registered_candidate = None
    elif damage == "scope":
        fixture.candidate_by_field["dataset_id"] = "00000000-0000-0000-0000-000000000001"
    else:
        fixture.candidate_by_field = dict(fixture.candidate_by_field)
        candidate_by_field = dict(fixture.candidate_by_field)
        candidate_by_field["source_recipes_json"] = _inventory(0).reference.as_dict()
        candidate_by_field["source_generations"] = {
            "registry_source_inventory": _inventory(0).reference.root.entries_sha256
        }
        with pytest.raises(store.RegistrySourceInventoryError, match="root_changed"):
            await store.load_source_inventory_for_api(
                connection, fixture.copy_target, candidate_by_field, control_schema=CONTROL
            )
        assert "count" not in connection.events
        return
    with pytest.raises((ValueError, store.RegistrySourceInventoryError)):
        await store.load_source_inventory_for_api(
            connection, fixture.copy_target, fixture.candidate_by_field, control_schema=CONTROL
        )
    assert "count" not in connection.events


async def test_named_read_context_keeps_logical_scope_without_writes():
    fixture = _inventory()
    connection = _InventoryConnection(fixture)
    relation = await store._read_context(connection, fixture.copy_target, CONTROL)
    assert relation == store._relation(fixture.copy_target)
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("field", ["dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"])
async def test_changed_logical_candidate_scope_refuses_before_inventory_data(field):
    fixture = _inventory(0)
    connection = _InventoryConnection(fixture)
    connection.registered_candidate[field] = "changed"
    with pytest.raises(store.RegistrySourceInventoryError, match="scope_invalid"):
        await store._read_context(connection, fixture.copy_target, CONTROL)
    assert connection.events == []


@pytest.mark.parametrize("count", [0, 101, 1000])
async def test_retained_api_resolver_selects_authenticated_loader(monkeypatch, count):
    fixture = _inventory(count)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(side_effect=["repeatable read", "on"]),
        fetchrow=AsyncMock(return_value=fixture.candidate_by_field),
    )
    resolver = AsyncMock(return_value=fixture.manifest)
    loader = AsyncMock(return_value=(fixture.reference, fixture.entries))
    monkeypatch.setattr(recipes, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(store, "load_source_inventory", loader)
    result = await recipes.resolve_retained_registry_source_recipes(
        connection, fixture.manifest, control_schema=CONTROL, api_read=True
    )
    assert result == fixture.selected_recipes
    assert resolver.await_args.kwargs["api_read"] is True
    assert loader.await_args.args[1] == fixture.copy_target


async def test_legacy_api_array_requires_read_only(monkeypatch):
    fixture = _inventory(1, legacy=True)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchval=AsyncMock(side_effect=["repeatable read", "off"])
    )
    resolver = AsyncMock()
    monkeypatch.setattr(recipes, "resolve_network_serving_manifest", resolver)
    with pytest.raises(recipes.RegistrySourceRecipeError, match="unavailable"):
        await recipes.resolve_retained_registry_source_recipes(
            connection, fixture.manifest, control_schema=CONTROL, api_read=True
        )
    resolver.assert_not_awaited()


@pytest.mark.parametrize("flag", [None, 1, "true"])
async def test_api_mode_is_not_selected_by_truthy_input(flag):
    connection = SimpleNamespace(is_in_transaction=Mock(), fetchval=AsyncMock())
    with pytest.raises(recipes.RegistrySourceRecipeError):
        await recipes.resolve_retained_registry_source_recipes(connection, _inventory().manifest, api_read=flag)
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("damage", ["missing_payload", "declared_size"])
async def test_inventory_page_refuses_missing_or_mismeasured_payload(damage):
    fixture = _inventory(1)
    entry = fixture.entries[0]
    metadata = {
        "ordinal": 1,
        "encoded_bytes": len(store.encode_source_inventory_entry(entry)) + int(damage == "declared_size"),
        "transfer_bytes": store.source_inventory_record_bytes(entry),
    }
    records = (
        []
        if damage == "missing_payload"
        else [dict(zip(store._COLUMNS, store.source_inventory_record(entry), strict=True))]
    )
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[[metadata], records]))
    with pytest.raises(store.RegistrySourceInventoryError, match="stored_bytes_invalid"):
        await store._read_page(connection, '"candidate".network_source_inventory', 0, 100, 1048576)
    assert connection.fetch.await_count == 2


async def test_short_inventory_page_carries_the_exact_next_ordinal():
    fixture = _inventory(2)
    connection = _InventoryConnection(fixture)
    page = await store._read_page(connection, '"candidate".network_source_inventory', 0, 1, 1048576)
    assert page.entries == fixture.entries[:1] and page.next_after_ordinal == 1
    assert connection.events == ["preflight", "payload"]


async def test_inventory_transfer_bound_refuses_even_when_json_fits():
    metadata = [{"ordinal": 1, "encoded_bytes": 1, "transfer_bytes": store.MAX_NATIVE_WIRE_BYTES}]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=metadata))
    with pytest.raises(store.RegistrySourceInventoryError, match="unsupported_bound"):
        await store._read_page(connection, '"candidate".network_source_inventory', 0, 100, 1048576)
    connection.fetch.assert_awaited_once()


async def test_untyped_inventory_root_never_reaches_physical_storage():
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(store.RegistrySourceInventoryError, match="inventory_invalid"):
        await store.verify_source_inventory(
            connection, _inventory(0).copy_target, expected_root={}, control_schema=CONTROL
        )
    connection.fetchrow.assert_not_awaited()


async def test_raw_inventory_copy_chunks_preserve_every_closed_entry():
    fixture = _inventory(2)
    connection = SimpleNamespace(copy_records_to_table=AsyncMock())
    await store._copy_inventory(connection, fixture.copy_target, fixture.entries, fixture.reference.root, 1, 1048576)
    copied_records = [
        record for call in connection.copy_records_to_table.await_args_list for record in call.kwargs["records"]
    ]
    assert copied_records == [store.source_inventory_record(entry) for entry in fixture.entries]
    assert connection.copy_records_to_table.await_count == 2
    assert all(
        call.kwargs["schema_name"] == fixture.copy_target.schema_name
        for call in connection.copy_records_to_table.await_args_list
    )


@pytest.mark.parametrize("bound", ["json", "wire"])
async def test_raw_inventory_copy_refuses_an_unrepresentable_entry_before_copy(monkeypatch, bound):
    fixture = _inventory(1)
    connection = SimpleNamespace(copy_records_to_table=AsyncMock())
    if bound == "wire":
        monkeypatch.setattr(store, "MAX_NATIVE_WIRE_BYTES", store._NATIVE_PROTOCOL_RESERVE + 1)
    with pytest.raises(store.RegistrySourceInventoryError, match="page_too_small|unsupported_bound"):
        await store._copy_inventory(
            connection,
            fixture.copy_target,
            fixture.entries,
            fixture.reference.root,
            100,
            1 if bound == "json" else 1048576,
        )
    connection.copy_records_to_table.assert_not_awaited()


async def test_raw_inventory_copy_requires_the_complete_expected_root():
    fixture = _inventory(1)
    connection = SimpleNamespace(copy_records_to_table=AsyncMock())
    with pytest.raises(store.RegistrySourceInventoryError, match="root_changed"):
        await store._copy_inventory(
            connection, fixture.copy_target, fixture.entries, _inventory(0).reference.root, 100, 1048576
        )
    connection.copy_records_to_table.assert_awaited_once()
