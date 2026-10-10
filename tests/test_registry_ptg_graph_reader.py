# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure retained-reader guards; compiled codec cases run in the native family."""

import hashlib
import json
import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_graph_reader as reader
from process.ptg_parts.ptg2_shared_blocks import SharedBlock
from process.ptg_parts.ptg2_v4_snapshot_maps import encode_v4_snapshot_map_pack


def _identity():
    return {
        "snapshot_key": 11,
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": "01" * 32,
        "map_sha256": "02" * 32,
        "finalizer_map_sha256": "03" * 32,
        "source_assignments_sha256": "04" * 32,
    }


def _page(edges=((5, 5),)):
    return {
        "contract": "registry_ptg_source_witness_page.v1",
        "graph_identity": _identity(),
        "after_ordinal": 0,
        "last_ordinal": len(edges),
        "row_count": len(edges),
        "edge_count": len(edges),
        "selected_edges": b"".join(struct.pack(">II", *edge) for edge in edges),
    }


def _manifest():
    return {
        "snapshot_key": 11,
        "relation": "group_npis_exact",
        "member_object_kind": "v4_group_npis_exact_members_v1",
        "locator_object_kind": "v4_group_npis_exact_locators_v1",
        "owner_base": 5,
        "owner_count": 1,
        "logical_member_count": 3,
        "vector_member_count": 3,
        "member_width": 4,
        "member_page_bytes": 12,
        "locator_page_bytes": 12,
        "locator_owner_span": 1,
    }


def _block_fields(block):
    return {
        "block_hash": block.block_hash,
        "format_version": block.format_version,
        "object_kind": block.object_kind,
        "codec": block.codec,
        "entry_count": block.entry_count,
        "raw_byte_count": block.raw_byte_count,
        "stored_byte_count": block.stored_byte_count,
        "payload_byte_count": len(block.payload),
    }


def _blocks():
    locator = struct.pack("<QI", 0, 3)
    members = struct.pack("<III", 2, 5, 9)
    return [
        SharedBlock("v4_group_npis_exact_locators_v1", 5, 0, 1, "none", len(locator), locator),
        SharedBlock("v4_group_npis_exact_members_v1", 0, 0, 3, "none", len(members), members),
    ]


class _Result:
    def __init__(self, rows=(), scalar=None):
        self.rows, self.scalar = rows, scalar

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)

    def scalar_one(self):
        return self.scalar


class _Session:
    def __init__(self, blocks=None):
        self.transaction = object()
        self.info = {}
        self.isolation = "repeatable read"
        self.queries = []
        self.manifest = _manifest()
        self.heavy = []
        self.packs = []
        self.cas = {}
        blocks_by_kind = {}
        for block in blocks or _blocks():
            self.cas[block.block_hash] = _block_fields(block) | {"payload": block.payload}
            blocks_by_kind.setdefault(block.object_kind, []).append(block)
        for kind, graph_blocks in blocks_by_kind.items():
            graph_blocks.sort(key=lambda block: (block.block_key, block.fragment_no))
            map_payload = encode_v4_snapshot_map_pack(kind, [block.reference() for block in graph_blocks])
            pack = SharedBlock(
                "snapshot_coordinate_map_v1", 0, 0, len(graph_blocks), "none", len(map_payload), map_payload
            )
            self.cas[pack.block_hash] = _block_fields(pack) | {"payload": map_payload}
            first, last = graph_blocks[0], graph_blocks[-1]
            self.packs.append(
                {
                    "snapshot_key": 11,
                    "object_kind": kind,
                    "pack_no": 0,
                    "first_block_key": first.block_key,
                    "first_fragment_no": first.fragment_no,
                    "last_block_key": last.block_key,
                    "last_fragment_no": last.fragment_no,
                    "coordinate_count": len(graph_blocks),
                    "pack_entry_count": sum(block.entry_count for block in graph_blocks),
                    **{"map_" + key: field_value for key, field_value in _block_fields(pack).items()},
                }
            )

    def in_transaction(self):
        return self.transaction is not None

    def get_transaction(self):
        return self.transaction

    def _matching_pack_rows(self, parameters):
        wanted_coordinates = set(zip(parameters["object_kinds"], parameters["block_keys"], parameters["fragment_nos"]))
        rows = [
            row.copy()
            for row in self.packs
            if any(
                kind == row["object_kind"]
                and (row["first_block_key"], row["first_fragment_no"])
                <= (key, fragment)
                <= (row["last_block_key"], row["last_fragment_no"])
                for kind, key, fragment in wanted_coordinates
            )
        ]
        return _Result(rows=rows[: parameters["row_limit"]])

    async def execute(self, query, parameters=None):
        sql = str(query)
        self.queries.append((sql, parameters))
        assert not any(command in sql.upper() for command in ("INSERT ", "UPDATE ", "DELETE ", "CREATE "))
        if sql == "SHOW transaction_isolation":
            return _Result(scalar=self.isolation)
        if "ptg2_v4_relation_manifest" in sql:
            return _Result(rows=[self.manifest] if self.manifest else [])
        if "ptg2_v4_heavy_owner" in sql:
            return _Result(rows=self.heavy)
        if "ptg2_v4_snapshot_map_pack" in sql:
            return self._matching_pack_rows(parameters)
        if "ptg2_v3_block" in sql:
            rows = [self.cas[key].copy() for key in parameters["block_hashes"] if key in self.cas]
            if "SELECT block_hash,payload" in sql:
                rows = [{"block_hash": row["block_hash"], "payload": row["payload"]} for row in rows]
            else:
                rows = [{key: value for key, value in row.items() if key != "payload"} for row in rows]
            return _Result(rows=rows[: parameters["row_limit"]])
        raise AssertionError(sql)


@pytest.fixture(autouse=True)
def _source_state(monkeypatch):
    monkeypatch.setattr(reader.authority, "_physical_binding", AsyncMock(return_value=None))
    actual = AsyncMock(return_value=(_identity(), []))
    monkeypatch.setattr(reader.authority, "_source_state", actual)
    return actual


def _specification():
    return SimpleNamespace(ptg_schema_name="synthetic_ptg", snapshot_id="synthetic-snapshot")


async def _verify(session, page=None, budget=None):
    return await reader.verify_registry_ptg_graph_page(
        session, _specification(), page or _page(), read_budget=budget or reader.RegistryPTGGraphReadBudget(1048576)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["read committed", "read uncommitted"])
async def test_reader_refuses_unpinned_isolation_before_source_reads(isolation, _source_state):
    session = _Session()
    session.isolation = isolation
    with pytest.raises(reader.RegistryPTGGraphReadError, match="transaction_required"):
        await _verify(session)
    _source_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_refuses_absent_transaction_before_sql(_source_state):
    session = _Session()
    session.transaction = None
    with pytest.raises(reader.RegistryPTGGraphReadError, match="transaction_required"):
        await _verify(session)
    assert session.queries == []
    _source_state.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"selected_edges": bytearray(b"12345678")},
        {"row_count": True},
        {"last_ordinal": 1000001},
        {"edge_count": 4097},
        {"extra": 1},
    ],
)
async def test_witness_control_refuses_unbounded_mutable_or_wrongly_typed_input(change, _source_state):
    session = _Session()
    with pytest.raises(reader.RegistryPTGGraphReadError):
        await _verify(session, _page() | change)
    assert session.queries == []
    _source_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_missing_or_non_bytes_result_has_no_python_fallback(monkeypatch):
    monkeypatch.setattr(reader.importlib, "import_module", lambda name: SimpleNamespace())
    with pytest.raises(reader.RegistryPTGGraphReadError, match="native_invalid"):
        await _verify(_Session())


@pytest.mark.asyncio
async def test_final_transaction_guard_runs_without_native_codec(monkeypatch, _source_state):
    session = _Session()

    async def source(*args):
        if _source_state.await_count == 2:
            session.transaction = object()
        return _identity(), []

    def planner(function, metadata, selected, payloads=None):
        assert function in ("plan_registry_ptg_graph_locator_pages", "plan_registry_ptg_graph_member_pages")
        return {"owner_keys": [5], "coordinates": []}

    _source_state.side_effect = source
    monkeypatch.setattr(reader, "_native", planner)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="^registry_ptg_transaction_required$"):
        await _verify(session)
    assert _source_state.await_count == 2


def _aggregate_fixture():
    selected = _page()["selected_edges"]
    expected = reader._witness_context(_page())
    state = SimpleNamespace(packs={0: ({}, b"map")}, blocks={0: ({}, b"members")})
    proof_by_field = {
        "contract": "registry_ptg_graph_batch.v1",
        "context": expected,
        "selected_edges_sha256": hashlib.sha256(selected).hexdigest(),
        "edge_count": 1,
        "verified_edge_count": 1,
        "missing_edge_count": 0,
        "selected_owner_count": 1,
        "map_pack_count": 1,
        "authenticated_graph_page_count": 1,
        "authenticated_raw_bytes": 10,
        "decoded_bytes": 10,
        "checked_member_count": 3,
    }
    return proof_by_field, expected, selected, state, _manifest()


@pytest.mark.parametrize(
    "field,value",
    [
        ("verified_edge_count", True),
        ("selected_edges_sha256", "0" * 64),
        ("authenticated_raw_bytes", 0),
        ("extra", 1),
    ],
)
def test_closed_aggregate_checker_refuses_changed_codec_output(field, value):
    proof, expected, selected, state, manifest = _aggregate_fixture()
    assert json.loads(reader._checked_proof(proof, expected, selected, state, manifest)) == proof
    proof[field] = value
    with pytest.raises(reader.RegistryPTGGraphReadError, match="^registry_ptg_graph_native_invalid$"):
        reader._checked_proof(proof, expected, selected, state, manifest)


def test_closed_aggregate_context_refuses_boolean_integer_alias():
    proof, expected, selected, state, manifest = _aggregate_fixture()
    proof["context"] = expected | {"row_count": True}
    with pytest.raises(reader.RegistryPTGGraphReadError, match="^registry_ptg_graph_native_invalid$"):
        reader._checked_proof(proof, expected, selected, state, manifest)


@pytest.mark.parametrize("value", [0, -1, True, 268435457])
def test_caller_budget_cannot_raise_native_raw_ceiling(value):
    with pytest.raises(reader.RegistryPTGGraphReadError, match="budget"):
        reader.RegistryPTGGraphReadBudget(value)


@pytest.mark.asyncio
async def test_sparse_search_allows_only_32_fetch_rounds_plus_terminal_check(monkeypatch):
    member_calls = []
    read_rounds = []

    def native(function, metadata, selected, payloads=None):
        if function == "plan_registry_ptg_graph_locator_pages":
            return {"owner_keys": [5], "coordinates": []}
        assert function == "plan_registry_ptg_graph_member_pages"
        member_calls.append(function)
        return {"owner_keys": [5], "coordinates": [["v4_group_npis_exact_members_v1", len(member_calls), 0]]}

    async def load(state, coordinates):
        read_rounds.append(coordinates)

    monkeypatch.setattr(reader, "_native", native)
    monkeypatch.setattr(reader._ReadState, "_load", load)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="no_progress"):
        await _verify(_Session())
    assert len(member_calls) == 33 and len(read_rounds) == 33


@pytest.mark.parametrize("value", [32, bytearray(32), memoryview(bytes(64)), b"x" * 31])
def test_cas_hash_type_and_byte_extent_are_checked_before_conversion(value):
    with pytest.raises(reader.RegistryPTGGraphReadError):
        reader._hash_bytes(value)


@pytest.mark.asyncio
async def test_terminal_empty_plan_after_32_rounds_is_still_only_a_plan(monkeypatch):
    member_calls = []

    def native(function, metadata, selected, payloads=None):
        member_calls.append(function)
        return {
            "owner_keys": [5],
            "coordinates": []
            if len(member_calls) == 33
            else [["v4_group_npis_exact_members_v1", len(member_calls), 0]],
        }

    state = SimpleNamespace(references={}, blocks={}, _inputs=lambda: ([], [], ()), _load=AsyncMock())
    monkeypatch.setattr(reader, "_native", native)
    metadata, payloads = await reader._load_members(state, {}, {}, [], b"selected")
    assert len(member_calls) == 33 and state._load.await_count == 32
    assert "contract" not in metadata and payloads == ()
    assert all(function == "plan_registry_ptg_graph_member_pages" for function in member_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [{"map_sha256": "f" * 65536}, {"snapshot_key": True}, {"layout_generation": "shared_blocks_v4 "}, {"unknown": 1}],
)
async def test_graph_identity_is_closed_and_bounded_before_json_or_database(change, _source_state):
    page = _page()
    page["graph_identity"] |= change
    session = _Session()
    with pytest.raises(reader.RegistryPTGGraphReadError):
        await _verify(session, page)
    assert session.queries == []
    _source_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_graph_page_cap_refuses_before_cas_metadata_or_payload(monkeypatch):
    kind = "v4_group_npis_exact_locators_v1"
    blocks = [SharedBlock(kind, owner, 0, 1, "none", 12, struct.pack("<QI", owner - 5, 1)) for owner in (5, 6)]
    session = _Session(blocks)
    monkeypatch.setattr(reader, "_MAX_PAGES", 1)
    budget = reader.RegistryPTGGraphReadBudget(1048576, maximum_pages=2)
    state = reader._ReadState(session, '"synthetic_ptg"', 11, budget)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="budget"):
        await state._load(((kind, 5, 0), (kind, 6, 0)))
    assert len(state.packs) == 1 and state.blocks == {}
    assert len(session.queries) == 2
