# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reserve exact CAS allocations before bounded office discovery reads."""

from typing import Any

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_snapshot_candidates import snapshot_candidate_relation
from process.ptg_parts.ptg2_v4_snapshot_maps import PTG2_V4_MAP_BLOCK_KIND
from process.registry_ptg_graph_reader import RegistryPTGGraphReadError


def _graph_frames(metadata_rows, kind, maximum, maps):
    frames_by_hash = {}
    coordinates = 0
    for metadata_by_field in metadata_rows:
        key = metadata_by_field.get("block_hash")
        size = metadata_by_field.get("raw_byte_count")
        if (
            not isinstance(key, (bytes, memoryview))
            or (key.nbytes if isinstance(key, memoryview) else len(key)) != 32
            or type(metadata_by_field.get("format_version")) is not int
            or metadata_by_field["format_version"] != 2
            or metadata_by_field.get("object_kind") != kind
            or metadata_by_field.get("codec") != "none"
            or type(size) is not int
            or not 1 <= size <= maximum
            or type(metadata_by_field.get("stored_byte_count")) is not int
            or size != metadata_by_field["stored_byte_count"]
            or type(metadata_by_field.get("payload_byte_count")) is not int
            or size != metadata_by_field["payload_byte_count"]
        ):
            raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
        key = bytes(key)
        if key in frames_by_hash and frames_by_hash[key]["raw_byte_count"] != size:
            raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
        frames_by_hash[key] = {"raw_byte_count": size}
        if maps:
            count = metadata_by_field.get("coordinate_count")
            if type(count) is not int or not 1 <= count <= 65536:
                raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
            coordinates += count
    return frames_by_hash, coordinates


async def budgeted_graph_payloads(session, metadata_result, schema_name, budget, kind, maximum, *, maps=False):
    """Read metadata first, then reserve the whole batch before payload transfer."""
    metadata_rows = [
        dict(getattr(metadata_by_field, "_mapping", metadata_by_field)) for metadata_by_field in metadata_result
    ]
    frames_by_hash, coordinates = _graph_frames(metadata_rows, kind, maximum, maps)
    budget.reserve(list(frames_by_hash.values()), coordinates)
    if not frames_by_hash:
        return metadata_rows
    payload_rows = (
        (
            await session.execute(
                text(
                    f"SELECT block_hash,payload FROM {_identifier(schema_name)}.ptg2_v3_block "
                    "WHERE block_hash=ANY(CAST(:block_hashes AS bytea[])) ORDER BY block_hash LIMIT :row_limit"
                ),
                {"block_hashes": tuple(frames_by_hash), "row_limit": len(frames_by_hash) + 1},
            )
        )
        .mappings()
        .all()
    )
    payloads_by_hash = {}
    for metadata_by_field in payload_rows:
        key, block_payload = metadata_by_field["block_hash"], metadata_by_field["payload"]
        if (
            not isinstance(key, (bytes, memoryview))
            or bytes(key) not in frames_by_hash
            or bytes(key) in payloads_by_hash
            or not isinstance(block_payload, (bytes, memoryview))
            or (block_payload.nbytes if isinstance(block_payload, memoryview) else len(block_payload))
            != frames_by_hash[bytes(key)]["raw_byte_count"]
        ):
            raise RegistryPTGGraphReadError("registry_ptg_graph_invalid")
        payloads_by_hash[bytes(key)] = bytes(block_payload)
    if set(payloads_by_hash) != set(frames_by_hash):
        raise RegistryPTGGraphReadError("registry_ptg_graph_page_missing")
    return [
        metadata_by_field | {"payload": payloads_by_hash[bytes(metadata_by_field["block_hash"])]}
        for metadata_by_field in metadata_rows
    ]


async def query_v4_map_packs(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    missing_pairs: list[tuple[int, int]],
    budget=None,
) -> Any:
    """Read selected map packs, reserving bounded payloads before transfer."""
    payload_expression = "block.payload" if budget is None else "octet_length(block.payload) AS payload_byte_count"
    query_result = await session.execute(
        text(
            f"""
            SELECT pack.pack_no, pack.first_block_key, pack.first_fragment_no,
                   pack.last_block_key, pack.last_fragment_no,
                   pack.coordinate_count, pack.map_block_hash AS block_hash,
                   block.format_version, block.object_kind, block.codec,
                   block.entry_count AS block_entry_count,
                   block.raw_byte_count, block.stored_byte_count, {payload_expression}
              FROM {snapshot_candidate_relation(session, _quote_ident(schema_name), "ptg2_v4_snapshot_map_pack")} AS pack
              JOIN {_quote_ident(schema_name)}.ptg2_v3_block AS block
                ON block.block_hash = pack.map_block_hash
             WHERE pack.snapshot_key = :snapshot_key
               AND pack.object_kind = :object_kind
               AND EXISTS (
                   SELECT 1
                     FROM unnest(
                              CAST(:block_keys AS bigint[]),
                              CAST(:fragment_nos AS integer[])
                          ) AS wanted(block_key, fragment_no)
                    WHERE ROW(wanted.block_key, wanted.fragment_no)
                          BETWEEN ROW(
                              pack.first_block_key,
                              pack.first_fragment_no
                          ) AND ROW(
                              pack.last_block_key,
                              pack.last_fragment_no
                          )
               )
             ORDER BY pack.pack_no
             {"" if budget is None else "LIMIT :read_page_limit"}
            """
        ),
        {
            "snapshot_key": snapshot_key,
            "object_kind": object_kind,
            "block_keys": [pair[0] for pair in missing_pairs],
            "fragment_nos": [pair[1] for pair in missing_pairs],
            "read_page_limit": 1 if budget is None else budget.maximum_pages - budget.read_pages + 1,
        },
    )

    if budget is None:
        return query_result
    return await budgeted_graph_payloads(
        session, query_result, schema_name, budget, PTG2_V4_MAP_BLOCK_KIND, 4 * 1024 * 1024, maps=True
    )
