# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded retained graph reads; source census and producer admission are separate gates."""

from __future__ import annotations

import hashlib
import importlib
import json
import struct
from dataclasses import dataclass, field
from typing import Any, Mapping

from sqlalchemy import text

from process import registry_ptg_cohort_authority as authority
from process.network_address_projection import _identifier
from process.ptg_parts.ptg2_v4_snapshot_maps import _decode_persisted_map_payload

_MAX_PAGE_BYTES = 4 * 1024 * 1024
_MAX_READ_BYTES = 256 * 1024 * 1024
_MAX_PAGES = 16384
_MAX_COORDINATES = 65536
_KINDS = frozenset(f"v4_group_npis_exact_{suffix}_v1" for suffix in ("locators", "members", "heavy_bitmap"))
_CAS_FIELDS = (
    "block_hash",
    "format_version",
    "object_kind",
    "codec",
    "entry_count",
    "raw_byte_count",
    "stored_byte_count",
)
_PACK_FIELDS = (
    "snapshot_key",
    "object_kind",
    "pack_no",
    "first_block_key",
    "first_fragment_no",
    "last_block_key",
    "last_fragment_no",
    "coordinate_count",
    "pack_entry_count",
)

_MANIFEST_SQL = """
SELECT snapshot_key,relation,member_object_kind,locator_object_kind,owner_base,owner_count,
       logical_member_count,vector_member_count,member_width,member_page_bytes,locator_page_bytes,locator_owner_span
  FROM {schema}.ptg2_v4_relation_manifest WHERE snapshot_key=:snapshot_key AND relation='group_npis_exact' LIMIT 2
"""
_HEAVY_SQL = """
SELECT snapshot_key,relation,owner_key,object_kind,member_count,member_base,member_span,fragment_count
  FROM {schema}.ptg2_v4_heavy_owner WHERE snapshot_key=:snapshot_key AND relation='group_npis_exact'
   AND owner_key=ANY(CAST(:owner_keys AS bigint[])) ORDER BY owner_key LIMIT 4097
"""
_PACK_SQL = """
SELECT pack.snapshot_key,pack.object_kind,pack.pack_no,pack.first_block_key,pack.first_fragment_no,
       pack.last_block_key,pack.last_fragment_no,pack.coordinate_count,pack.entry_count AS pack_entry_count,
       pack.map_block_hash,block.format_version AS map_format_version,block.object_kind AS map_object_kind,
       block.codec AS map_codec,block.entry_count AS map_entry_count,block.raw_byte_count AS map_raw_byte_count,
       block.stored_byte_count AS map_stored_byte_count,octet_length(block.payload) AS map_payload_byte_count
  FROM {schema}.ptg2_v4_snapshot_map_pack pack JOIN {schema}.ptg2_v3_block block ON block.block_hash=pack.map_block_hash
 WHERE pack.snapshot_key=:snapshot_key AND EXISTS (
   SELECT 1 FROM unnest(CAST(:object_kinds AS text[]),CAST(:block_keys AS bigint[]),CAST(:fragment_nos AS integer[]))
     AS wanted(object_kind,block_key,fragment_no)
    WHERE wanted.object_kind=pack.object_kind AND ROW(wanted.block_key,wanted.fragment_no)
      BETWEEN ROW(pack.first_block_key,pack.first_fragment_no) AND ROW(pack.last_block_key,pack.last_fragment_no))
 ORDER BY pack.object_kind,pack.pack_no LIMIT :row_limit
"""
_CAS_METADATA_SQL = """
SELECT block_hash,format_version,object_kind,codec,entry_count,raw_byte_count,stored_byte_count,
       octet_length(payload) AS payload_byte_count FROM {schema}.ptg2_v3_block
 WHERE block_hash=ANY(CAST(:block_hashes AS bytea[])) ORDER BY block_hash LIMIT :row_limit
"""
_PAYLOAD_SQL = """
SELECT block_hash,payload FROM {schema}.ptg2_v3_block
 WHERE block_hash=ANY(CAST(:block_hashes AS bytea[])) ORDER BY block_hash LIMIT :row_limit
"""


class RegistryPTGGraphReadError(ValueError):
    """The pinned retained graph cannot complete a bounded native check."""


@dataclass
class RegistryPTGGraphReadBudget:
    """Caller-owned read limits accumulate across witness pages and failed attempts."""

    maximum_bytes: int
    maximum_pages: int = _MAX_PAGES
    maximum_coordinates: int = _MAX_COORDINATES
    read_bytes: int = field(default=0, init=False)
    read_pages: int = field(default=0, init=False)
    coordinates: int = field(default=0, init=False)

    def __post_init__(self):
        for value, maximum in (
            (self.maximum_bytes, _MAX_READ_BYTES),
            (self.maximum_pages, _MAX_PAGES * 2),
            (self.maximum_coordinates, _MAX_COORDINATES),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise RegistryPTGGraphReadError("registry_ptg_graph_budget")

    def reserve(self, frames, coordinate_count=0):
        """Reserve before allocation or fetch; failed attempts retain their charges."""
        byte_count = sum(frame["raw_byte_count"] for frame in frames)
        if (
            self.read_bytes + byte_count > self.maximum_bytes
            or self.read_pages + len(frames) > self.maximum_pages
            or self.coordinates + coordinate_count > self.maximum_coordinates
        ):
            raise RegistryPTGGraphReadError("registry_ptg_graph_budget")
        self.read_bytes += byte_count
        self.read_pages += len(frames)
        self.coordinates += coordinate_count


def _json(value, maximum=16 * 1024 * 1024):
    encoded = authority._canonical(value)
    if len(encoded) > maximum:
        raise RegistryPTGGraphReadError("registry_ptg_graph_budget")
    return encoded


def _unique_object(pairs):
    fields_by_name = {}
    for key, value in pairs:
        if key in fields_by_name:
            raise RegistryPTGGraphReadError("registry_ptg_graph_native_invalid")
        fields_by_name[key] = value
    return fields_by_name


def _native(function, metadata, selected, payloads=None):
    try:
        native = importlib.import_module("ptg2_address_canon")
        arguments = (_json(metadata, 16384 if payloads is None else 16 * 1024 * 1024), selected)
        if payloads is not None:
            arguments += (payloads,)
        encoded = getattr(native, function)(*arguments)
        if type(encoded) is not bytes or not 1 <= len(encoded) <= _MAX_PAGE_BYTES:
            raise ValueError
        return json.loads(encoded, object_pairs_hook=_unique_object)
    except RegistryPTGGraphReadError:
        raise
    except ImportError, AttributeError, TypeError, ValueError, RuntimeError, UnicodeError, RecursionError:
        raise RegistryPTGGraphReadError("registry_ptg_graph_native_invalid") from None


def _coordinates(plan):
    if type(plan) is not dict or set(plan) != {"owner_keys", "coordinates"}:
        raise RegistryPTGGraphReadError("registry_ptg_graph_native_invalid")
    owners, raw = plan["owner_keys"], plan["coordinates"]
    if (
        type(owners) is not list
        or len(owners) > 4096
        or any(type(key) is not int or not 0 <= key <= 2147483647 for key in owners)
    ):
        raise RegistryPTGGraphReadError("registry_ptg_graph_native_invalid")
    if owners != sorted(set(owners)) or type(raw) is not list or len(raw) > _MAX_PAGES:
        raise RegistryPTGGraphReadError("registry_ptg_graph_native_invalid")
    coordinates = []
    for coordinate in raw:
        if (
            type(coordinate) is not list
            or len(coordinate) != 3
            or coordinate[0] not in _KINDS
            or type(coordinate[1]) is not int
            or not 0 <= coordinate[1] <= 9223372036854775807
            or type(coordinate[2]) is not int
            or not 0 <= coordinate[2] <= 2147483647
        ):
            raise RegistryPTGGraphReadError("registry_ptg_graph_native_invalid")
        coordinates.append(tuple(coordinate))
    if len(set(coordinates)) != len(coordinates):
        raise RegistryPTGGraphReadError("registry_ptg_graph_native_invalid")
    return owners, tuple(coordinates)


async def _rows(session, query, parameters, maximum):
    rows = (await session.execute(text(query), parameters)).mappings().all()
    if len(rows) > maximum:
        raise RegistryPTGGraphReadError("registry_ptg_graph_budget")
    return [dict(row) for row in rows]


def _hash_bytes(value):
    if (
        not isinstance(value, (bytes, memoryview))
        or (value.nbytes if isinstance(value, memoryview) else len(value)) != 32
    ):
        raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
    return bytes(value)


def _cas_frame(row, prefix=""):
    frame_by_field = {name: row[prefix + name] for name in _CAS_FIELDS}
    frame_by_field["block_hash"] = _hash_bytes(frame_by_field["block_hash"]).hex()
    if (
        frame_by_field["format_version"] != 2
        or type(frame_by_field["format_version"]) is not int
        or frame_by_field["codec"] != "none"
        or frame_by_field["object_kind"] not in _KINDS | {"snapshot_coordinate_map_v1"}
        or any(
            type(frame_by_field[name]) is not int or frame_by_field[name] < 0
            for name in ("entry_count", "raw_byte_count", "stored_byte_count")
        )
        or frame_by_field["raw_byte_count"] != frame_by_field["stored_byte_count"]
        or frame_by_field["raw_byte_count"] != row[prefix + "payload_byte_count"]
        or not 1 <= frame_by_field["raw_byte_count"] <= _MAX_PAGE_BYTES
    ):
        raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
    return frame_by_field


@dataclass
class _ReadState:
    session: Any
    schema: str
    snapshot_key: int
    budget: RegistryPTGGraphReadBudget
    packs: dict = field(default_factory=dict)
    references: dict = field(default_factory=dict)
    blocks: dict = field(default_factory=dict)

    async def _payloads(self, frames):
        self.budget.reserve(frames)
        frames_by_hash = {bytes.fromhex(frame["block_hash"]): frame for frame in frames}
        if len(frames_by_hash) != len(frames):
            raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
        if not frames_by_hash:
            return {}
        rows = await _rows(
            self.session,
            _PAYLOAD_SQL.format(schema=self.schema),
            {"block_hashes": tuple(frames_by_hash), "row_limit": len(frames_by_hash) + 1},
            len(frames_by_hash),
        )
        payloads_by_hash = {}
        for row in rows:
            key, payload = _hash_bytes(row["block_hash"]), row["payload"]
            if (
                key not in frames_by_hash
                or key in payloads_by_hash
                or not isinstance(payload, (bytes, memoryview))
                or (payload.nbytes if isinstance(payload, memoryview) else len(payload))
                != frames_by_hash[key]["raw_byte_count"]
            ):
                raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
            payloads_by_hash[key] = bytes(payload)
        if set(payloads_by_hash) != set(frames_by_hash):
            raise RegistryPTGGraphReadError("registry_ptg_graph_page_missing")
        return payloads_by_hash

    async def _load_packs(self, coordinates):
        missing_coordinates = [coordinate for coordinate in coordinates if coordinate not in self.references]
        if not missing_coordinates:
            return
        parameters_by_name = {
            "snapshot_key": self.snapshot_key,
            "object_kinds": tuple(pack_row[0] for pack_row in missing_coordinates),
            "block_keys": tuple(pack_row[1] for pack_row in missing_coordinates),
            "fragment_nos": tuple(pack_row[2] for pack_row in missing_coordinates),
            "row_limit": _MAX_PAGES - len(self.packs) + 1,
        }
        pack_rows = await _rows(
            self.session, _PACK_SQL.format(schema=self.schema), parameters_by_name, _MAX_PAGES - len(self.packs)
        )
        fresh_packs = []
        observed_pack_keys = set(self.packs)
        for pack_row in pack_rows:
            key = (pack_row["object_kind"], pack_row["pack_no"])
            if (
                pack_row["snapshot_key"] != self.snapshot_key
                or pack_row["object_kind"] not in _KINDS
                or key in observed_pack_keys
            ):
                raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
            if (
                type(pack_row["coordinate_count"]) is not int
                or not 1 <= pack_row["coordinate_count"] <= _MAX_COORDINATES
            ):
                raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
            observed_pack_keys.add(key)
            fresh_packs.append((key, pack_row, _cas_frame(pack_row, "map_")))
        count = sum(pack_row["coordinate_count"] for _, pack_row, _ in fresh_packs)
        self.budget.reserve([], count)
        payloads = await self._payloads([frame for _, _, frame in fresh_packs])
        for key, pack_row, frame in fresh_packs:
            pack_payload = payloads[bytes.fromhex(frame["block_hash"])]
            if len(pack_payload) < 16 or struct.unpack_from(">I", pack_payload, 12)[0] != pack_row["coordinate_count"]:
                raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
            decoded = _decode_persisted_map_payload(
                pack_row | {"map_payload": pack_payload}, object_kind=pack_row["object_kind"]
            )
            if len(decoded) != pack_row["coordinate_count"]:
                raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
            self.packs[key] = ({name: pack_row[name] for name in _PACK_FIELDS} | {"block": frame}, pack_payload)
            for coordinate in decoded:
                identity = (coordinate.object_kind, coordinate.block_key, coordinate.fragment_no)
                if identity in self.references:
                    raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
                self.references[identity] = coordinate
        if any(coordinate not in self.references for coordinate in coordinates):
            raise RegistryPTGGraphReadError("registry_ptg_graph_page_missing")

    async def _load(self, coordinates):
        await self._load_packs(coordinates)
        hashes = {self.references[coordinate].block_hash for coordinate in coordinates} - self.blocks.keys()
        if not hashes:
            return
        if len(self.blocks) + len(hashes) > _MAX_PAGES:
            raise RegistryPTGGraphReadError("registry_ptg_graph_budget")
        rows = await _rows(
            self.session,
            _CAS_METADATA_SQL.format(schema=self.schema),
            {"block_hashes": tuple(sorted(hashes)), "row_limit": len(hashes) + 1},
            len(hashes),
        )
        frames = [_cas_frame(row) for row in rows]
        if {bytes.fromhex(frame["block_hash"]) for frame in frames} != hashes:
            raise RegistryPTGGraphReadError("registry_ptg_graph_page_missing")
        payloads = await self._payloads(frames)
        for frame in frames:
            key = bytes.fromhex(frame["block_hash"])
            self.blocks[key] = (frame, payloads[key])

    def _inputs(self):
        packs = [self.packs[key] for key in sorted(self.packs)]
        blocks = [self.blocks[key] for key in sorted(self.blocks)]
        payloads = tuple(payload for _, payload in packs + blocks)
        pack_frames = [
            metadata | {"block": metadata["block"] | {"payload_index": index}}
            for index, (metadata, _) in enumerate(packs)
        ]
        block_frames = [metadata | {"payload_index": index + len(packs)} for index, (metadata, _) in enumerate(blocks)]
        return pack_frames, block_frames, payloads


def _witness_context(page):
    if (
        not isinstance(page, Mapping)
        or set(page)
        != {"contract", "graph_identity", "after_ordinal", "last_ordinal", "row_count", "edge_count", "selected_edges"}
        or page["contract"] != "registry_ptg_source_witness_page.v1"
        or type(page["selected_edges"]) is not bytes
        or any(type(page[name]) is not int for name in ("after_ordinal", "last_ordinal", "row_count", "edge_count"))
        or not 0 <= page["after_ordinal"] < page["last_ordinal"] <= 1000000
        or page["last_ordinal"] - page["after_ordinal"] != page["row_count"]
        or not 1 <= page["edge_count"] <= page["row_count"] <= 4096
        or len(page["selected_edges"]) != page["edge_count"] * 8
    ):
        raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
    identity = page["graph_identity"]
    hash_fields = authority._GRAPH_FIELDS - {"snapshot_key", "layout_generation"}
    if (
        type(identity) is not dict
        or set(identity) != authority._GRAPH_FIELDS
        or type(identity["snapshot_key"]) is not int
        or not 1 <= identity["snapshot_key"] <= 9223372036854775807
        or type(identity["layout_generation"]) is not str
        or identity["layout_generation"] != "shared_blocks_v4"
        or any(
            type(identity[name]) is not str
            or len(identity[name]) != 64
            or any(character not in "0123456789abcdef" for character in identity[name])
            for name in hash_fields
        )
    ):
        raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
    return {name: page[name] for name in ("graph_identity", "after_ordinal", "last_ordinal", "row_count")}


def _checked_proof(proof, expected, selected, state, manifest):
    fields = {
        "contract",
        "context",
        "selected_edges_sha256",
        "edge_count",
        "verified_edge_count",
        "missing_edge_count",
        "selected_owner_count",
        "map_pack_count",
        "authenticated_graph_page_count",
        "authenticated_raw_bytes",
        "decoded_bytes",
        "checked_member_count",
    }
    counts = fields - {"contract", "context", "selected_edges_sha256"}
    if (
        type(proof) is not dict
        or set(proof) != fields
        or proof["contract"] != "registry_ptg_graph_batch.v1"
        or type(proof["context"]) is not dict
        or _json(proof["context"], 16384) != _json(expected, 16384)
        or any(type(proof[name]) is not int or proof[name] < 0 for name in counts)
        or proof["selected_edges_sha256"] != hashlib.sha256(selected).hexdigest()
        or proof["edge_count"] != len(selected) // 8
        or proof["verified_edge_count"] != proof["edge_count"]
        or proof["missing_edge_count"] != 0
        or not 1 <= proof["selected_owner_count"] <= proof["edge_count"]
        or proof["map_pack_count"] != len(state.packs)
        or proof["authenticated_graph_page_count"] != len(state.blocks)
        or proof["authenticated_raw_bytes"]
        != sum(len(raw) for _, raw in (*state.packs.values(), *state.blocks.values()))
        or proof["decoded_bytes"] > _MAX_READ_BYTES
        or proof["checked_member_count"] > manifest["logical_member_count"]
    ):
        raise RegistryPTGGraphReadError("registry_ptg_graph_native_invalid")
    return _json(proof, _MAX_PAGE_BYTES)


async def _load_members(state, context, manifest, heavy, selected):
    for round_number in range(33):
        packs, blocks, payloads = state._inputs()
        metadata = {"context": context, "manifest": manifest, "map_packs": packs, "blocks": blocks, "heavy": heavy}
        _, coordinates = _coordinates(_native("plan_registry_ptg_graph_member_pages", metadata, selected, payloads))
        if not coordinates:
            break
        if round_number == 32 or any(
            coordinate in state.references and state.references[coordinate].block_hash in state.blocks
            for coordinate in coordinates
        ):
            raise RegistryPTGGraphReadError("registry_ptg_graph_no_progress")
        await state._load(coordinates)
    return metadata, payloads


async def _verify_page(
    session: Any, specification: Any, witness_page: Mapping[str, Any], *, read_budget: RegistryPTGGraphReadBudget
) -> bytes:
    """Verify selected retained edges in one pinned transaction; never admit a producer.

    The caller must exhaust the authenticated source witness census and obtain
    producer/capture authority independently before admitting any capture.
    """
    expected = json.loads(_json(_witness_context(witness_page), 16384))
    selected = witness_page["selected_edges"]
    transaction = session.get_transaction()
    if (
        transaction is None
        or not session.in_transaction()
        or (await session.execute(text("SHOW transaction_isolation"))).scalar_one()
        not in {"repeatable read", "serializable"}
    ):
        raise RegistryPTGGraphReadError("registry_ptg_transaction_required")
    context = expected | {
        "graph_identity": (await authority._source_state(session, specification, expected["graph_identity"]))[0]
    }
    binding = await authority._physical_binding(session, specification)
    if binding is not None and binding.payload_snapshot_key != context["graph_identity"]["snapshot_key"]:
        raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
    state = _ReadState(
        session,
        _identifier(binding.schema_name if binding is not None else specification.ptg_schema_name),
        context["graph_identity"]["snapshot_key"],
        read_budget,
    )
    manifest_rows = await _rows(
        session, _MANIFEST_SQL.format(schema=state.schema), {"snapshot_key": state.snapshot_key}, 1
    )
    if len(manifest_rows) != 1:
        raise RegistryPTGGraphReadError("registry_ptg_graph_page_missing")
    manifest = manifest_rows[0]
    owners, coordinates = _coordinates(
        _native("plan_registry_ptg_graph_locator_pages", {"context": context, "manifest": manifest}, selected)
    )
    heavy = await _rows(
        session,
        _HEAVY_SQL.format(schema=state.schema),
        {"snapshot_key": state.snapshot_key, "owner_keys": tuple(owners)},
        4096,
    )
    await state._load(coordinates)
    metadata, payloads = await _load_members(state, context, manifest, heavy, selected)
    if not session.in_transaction() or session.get_transaction() is not transaction:
        raise RegistryPTGGraphReadError("registry_ptg_transaction_required")
    final_identity, _ = await authority._source_state(session, specification, expected["graph_identity"])
    if not session.in_transaction() or session.get_transaction() is not transaction:
        raise RegistryPTGGraphReadError("registry_ptg_transaction_required")
    metadata.pop("context")
    metadata |= {"expected_context": expected, "actual_context": context | {"graph_identity": final_identity}}
    proof = _native("verify_registry_ptg_graph_batch", metadata, selected, payloads)
    return _checked_proof(proof, expected, selected, state, manifest)


async def verify_registry_ptg_graph_page(
    session: Any, specification: Any, witness_page: Mapping[str, Any], *, read_budget: RegistryPTGGraphReadBudget
) -> bytes:
    """Verify one retained graph page without conferring census or producer authority."""
    if type(read_budget) is not RegistryPTGGraphReadBudget:
        raise RegistryPTGGraphReadError("registry_ptg_graph_budget")
    try:
        return await _verify_page(session, specification, witness_page, read_budget=read_budget)
    except RegistryPTGGraphReadError:
        raise
    except KeyError, TypeError, ValueError, RuntimeError, struct.error:
        raise RegistryPTGGraphReadError("registry_ptg_graph_invalid") from None
