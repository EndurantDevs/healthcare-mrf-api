# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated packed mappings for V4 finalizer blocks."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_SHARED_FORMAT_VERSION
from process.ptg_parts.ptg2_v4_finalizer_map_sql import (
    _MAP_PACK_SQL,
    _ROOT_SELECTION_SQL,
    _TARGET_ANCHOR_SQL,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_FORMAT,
    PTG2_V4_SHARED_GENERATION,
    V4SnapshotMapCoordinate,
    _decode_persisted_map_payload,
)


PTG2_V4_FINALIZER_MAP_CONTRACT = "packed_finalizer_map_v2"
PTG2_V4_FINALIZER_MAP_ROOT_TABLE = "ptg2_v4_finalizer_map_root"
PTG2_V4_FINALIZER_MAP_PACK_TABLE = "ptg2_v4_finalizer_map_pack"
PTG2_V4_FINALIZER_MAP_TARGET_TABLE = "ptg2_v4_finalizer_map_target"
PTG2_V4_FINALIZER_MAP_MANIFEST_KEY = "finalizer_mapping"
PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS = (
    "by_code_price_dictionary",
    "by_code_price_page_v4",
    "by_code_provider_shard_v1",
    "provider_set_codes_v3",
    "provider_set_count_dictionary",
    "provider_set_page_v3_s2",
)
PTG2_V4_FINALIZER_PACKED_OBJECT_KIND_SET = frozenset(
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
)
_MAP_PACK_PAGE_ROWS = 128

_SUMMARY_COUNT_FIELDS = (
    "object_kind_count",
    "map_pack_count",
    "coordinate_count",
    "entry_count",
    "logical_byte_count",
    "stored_map_byte_count",
    "target_block_count",
)
_MANIFEST_FIELDS = frozenset(
    (
        *_SUMMARY_COUNT_FIELDS,
        "contract",
        "map_format",
        "map_digest",
        "object_kinds",
        "canonical_mapping_digest",
        "canonical_byte_count",
        "target_identity_digest",
    )
)
class FinalizerMapError(RuntimeError):
    """Raised when an explicit packed-finalizer contract cannot be proven."""


class FinalizerMapReadLimitError(FinalizerMapError):
    """Raised before packed mapping metadata exceeds its bounded read limit."""


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _first_row(query_result: Any) -> dict[str, Any]:
    first = getattr(query_result, "first", None)
    row = first() if callable(first) else next(iter(query_result), None)
    return _row_mapping(row) if row is not None else {}


def _manifest_mapping(raw_manifest: Any) -> dict[str, Any]:
    if isinstance(raw_manifest, Mapping):
        return dict(raw_manifest)
    raise FinalizerMapError("packed finalizer map manifest is not an object")


def _strict_count(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FinalizerMapError(f"packed finalizer map {label} is invalid")
    return value


def _validate_root_manifest_identity(
    root_fields: Mapping[str, Any],
    finalizer_manifest: Mapping[str, Any],
) -> None:
    if (
        root_fields.get("layout_state") != "sealed"
        or root_fields.get("layout_generation") != PTG2_V4_SHARED_GENERATION
        or root_fields.get("root_state") != "complete"
        or root_fields.get("root_completed_at") is None
        or root_fields.get("root_contract") != PTG2_V4_FINALIZER_MAP_CONTRACT
        or root_fields.get("root_map_format") != PTG2_V4_MAP_FORMAT
    ):
        raise FinalizerMapError("packed finalizer map root is unavailable or incomplete")
    if set(finalizer_manifest) != _MANIFEST_FIELDS:
        raise FinalizerMapError("packed finalizer map manifest fields are incompatible")
    manifest_object_kinds = finalizer_manifest.get("object_kinds")
    if (
        finalizer_manifest.get("contract") != PTG2_V4_FINALIZER_MAP_CONTRACT
        or finalizer_manifest.get("map_format") != PTG2_V4_MAP_FORMAT
        or not isinstance(manifest_object_kinds, (list, tuple))
        or tuple(manifest_object_kinds)
        != PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    ):
        raise FinalizerMapError("packed finalizer map manifest contract is incompatible")
    map_digest = bytes(root_fields.get("root_map_digest") or b"")
    if (
        len(map_digest) != 32
        or finalizer_manifest.get("map_digest") != map_digest.hex()
    ):
        raise FinalizerMapError("packed finalizer map digest does not match its manifest")
    canonical_digest = bytes(
        root_fields.get("root_canonical_mapping_digest") or b""
    )
    target_digest = bytes(root_fields.get("root_target_identity_digest") or b"")
    canonical_bytes = _strict_count(
        root_fields.get("root_canonical_byte_count"),
        "canonical_byte_count",
    )
    if (
        len(canonical_digest) != 32
        or len(target_digest) != 32
        or canonical_bytes <= 0
        or finalizer_manifest.get("canonical_mapping_digest")
        != canonical_digest.hex()
        or finalizer_manifest.get("canonical_byte_count") != canonical_bytes
        or finalizer_manifest.get("target_identity_digest") != target_digest.hex()
    ):
        raise FinalizerMapError(
            "packed finalizer native receipt does not match its manifest"
        )


def _validated_root(
    root_fields: Mapping[str, Any],
    finalizer_manifest: Mapping[str, Any],
) -> None:
    """Fail closed unless root state and manifest identity agree exactly."""

    _validate_root_manifest_identity(root_fields, finalizer_manifest)
    count_by_field: dict[str, int] = {}
    for field_name in _SUMMARY_COUNT_FIELDS:
        root_value = _strict_count(root_fields.get(f"root_{field_name}"), field_name)
        manifest_value = _strict_count(finalizer_manifest.get(field_name), field_name)
        if root_value != manifest_value:
            raise FinalizerMapError(
                f"packed finalizer map {field_name} does not match its manifest"
            )
        count_by_field[field_name] = root_value
    if (
        count_by_field["object_kind_count"]
        != len(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)
        or count_by_field["map_pack_count"] < count_by_field["object_kind_count"]
        or count_by_field["coordinate_count"] < count_by_field["map_pack_count"]
        or not 0
        < count_by_field["target_block_count"]
        <= count_by_field["coordinate_count"]
        or count_by_field["stored_map_byte_count"] <= 0
    ):
        raise FinalizerMapError("packed finalizer map root geometry is invalid")


async def has_complete_v4_finalizer_map(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> bool:
    """Select legacy absence or authenticate one explicit complete packed root."""

    normalized_snapshot_key = int(snapshot_key)
    schema = _quote_ident(schema_name)
    if not await _has_finalizer_map_tables(
        session,
        schema_name=schema_name,
    ):
        return False
    root_query = await session.execute(
        text(
            _ROOT_SELECTION_SQL.format(
                schema=schema,
                root_table=_quote_ident(PTG2_V4_FINALIZER_MAP_ROOT_TABLE),
                manifest_key=PTG2_V4_FINALIZER_MAP_MANIFEST_KEY,
            )
        ),
        {
            "snapshot_key": normalized_snapshot_key,
            "packed_object_kinds": PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
        },
    )
    root_fields = _first_row(root_query)
    manifest_present = bool(root_fields.get("manifest_present"))
    root_present = bool(root_fields.get("root_present"))
    if not manifest_present and not root_present:
        return False
    if not manifest_present or not root_present:
        raise FinalizerMapError("packed finalizer map root and manifest must appear together")
    finalizer_manifest = _manifest_mapping(root_fields.get("finalizer_manifest"))
    _validated_root(root_fields, finalizer_manifest)
    if bool(root_fields.get("relational_mapping_present")):
        raise FinalizerMapError("packed finalizer map snapshot also contains relational mappings")
    return True


async def _has_finalizer_map_tables(
    session: Any,
    *,
    schema_name: str,
) -> bool:
    """Accept clean legacy absence and reject a partial storage extension."""

    relation_name_by_table = {
        table_name: f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
        for table_name in (
            PTG2_V4_FINALIZER_MAP_ROOT_TABLE,
            PTG2_V4_FINALIZER_MAP_PACK_TABLE,
            PTG2_V4_FINALIZER_MAP_TARGET_TABLE,
        )
    }
    availability = await session.execute(
        text(
            "SELECT "
            + ", ".join(
                f"to_regclass(:{table_name}) IS NOT NULL AS {table_name}"
                for table_name in relation_name_by_table
            )
        ),
        relation_name_by_table,
    )
    fields_by_name = _first_row(availability)
    present_count = sum(
        bool(fields_by_name.get(name)) for name in relation_name_by_table
    )
    if present_count not in (0, len(relation_name_by_table)):
        raise FinalizerMapError("packed finalizer map storage extension is partial")
    return present_count == len(relation_name_by_table)


async def has_valid_finalizer_map(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    layout_manifest: Mapping[str, Any],
) -> bool:
    """Validate a sealed packed pair while permitting pre-table legacy absence."""

    serving_index = layout_manifest.get("serving_index")
    manifest_present = isinstance(serving_index, Mapping) and (
        PTG2_V4_FINALIZER_MAP_MANIFEST_KEY in serving_index
    )
    if not await _has_finalizer_map_tables(
        session,
        schema_name=schema_name,
    ):
        if manifest_present:
            raise FinalizerMapError(
                "packed finalizer map manifest has no storage contract"
            )
        return False
    return await has_complete_v4_finalizer_map(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
    )


def _validate_pack_geometry(
    pack_row: Mapping[str, Any],
    coordinates: Sequence[V4SnapshotMapCoordinate],
) -> None:
    first = (coordinates[0].block_key, coordinates[0].fragment_no)
    last = (coordinates[-1].block_key, coordinates[-1].fragment_no)
    stored_first = (
        int(pack_row.get("first_block_key") or 0),
        int(pack_row.get("first_fragment_no") or 0),
    )
    stored_last = (
        int(pack_row.get("last_block_key") or 0),
        int(pack_row.get("last_fragment_no") or 0),
    )
    if (
        first != stored_first
        or last != stored_last
        or int(pack_row.get("coordinate_count") or 0) != len(coordinates)
        or int(pack_row.get("entry_count") or 0)
        != sum(coordinate.entry_count for coordinate in coordinates)
    ):
        raise FinalizerMapError("packed finalizer map pack geometry is inconsistent")


async def _load_target_metadata(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    target_hashes: set[bytes],
) -> dict[bytes, tuple[str, int, int]]:
    if not target_hashes:
        return {}
    anchor_query = await session.execute(
        text(
            _TARGET_ANCHOR_SQL.format(
                schema=schema,
                target_table=_quote_ident(PTG2_V4_FINALIZER_MAP_TARGET_TABLE),
            )
        ),
        {"snapshot_key": snapshot_key, "block_hashes": tuple(sorted(target_hashes))},
    )
    metadata_by_hash: dict[bytes, tuple[str, int, int]] = {}
    for raw_anchor in anchor_query:
        anchor_fields = _row_mapping(raw_anchor)
        block_hash = bytes(anchor_fields.get("block_hash") or b"")
        codec = str(anchor_fields.get("codec") or "")
        object_kind = str(anchor_fields.get("object_kind") or "")
        entry_count = int(anchor_fields.get("entry_count") or 0)
        raw_byte_count = int(anchor_fields.get("raw_byte_count") or 0)
        stored_byte_count = int(anchor_fields.get("stored_byte_count") or 0)
        if (
            block_hash not in target_hashes
            or block_hash in metadata_by_hash
            or int(anchor_fields.get("format_version") or 0)
            != PTG2_V3_SHARED_FORMAT_VERSION
            or object_kind not in PTG2_V4_FINALIZER_PACKED_OBJECT_KIND_SET
            or codec not in {"none", "zlib"}
            or min(entry_count, raw_byte_count, stored_byte_count) < 0
            or (codec == "none" and raw_byte_count != stored_byte_count)
        ):
            raise FinalizerMapError(
                "packed finalizer map target CAS metadata is invalid"
            )
        metadata_by_hash[block_hash] = (object_kind, entry_count, raw_byte_count)
    if set(metadata_by_hash) != target_hashes:
        raise FinalizerMapError("packed finalizer map is missing a durable target anchor")
    return metadata_by_hash


async def _load_map_packs(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    object_kind: str,
    block_keys: tuple[int, ...],
    fragment_nos: tuple[int, ...],
    has_fragment_filter: bool,
    after_pack_no: int,
) -> list[tuple[dict[str, Any], tuple[V4SnapshotMapCoordinate, ...]]]:
    pack_query = await session.execute(
        text(
            _MAP_PACK_SQL.format(
                schema=schema,
                pack_table=_quote_ident(PTG2_V4_FINALIZER_MAP_PACK_TABLE),
            )
        ),
        {
            "snapshot_key": snapshot_key,
            "object_kind": object_kind,
            "block_keys": block_keys,
            "fragment_nos": fragment_nos,
            "has_fragment_filter": has_fragment_filter,
            "after_pack_no": after_pack_no,
            "pack_limit": _MAP_PACK_PAGE_ROWS,
        },
    )
    return _decode_map_packs(pack_query, object_kind=object_kind)


def _decode_map_packs(
    pack_query: Iterable[Any],
    *,
    object_kind: str,
) -> list[tuple[dict[str, Any], tuple[V4SnapshotMapCoordinate, ...]]]:
    decoded_packs: list[
        tuple[dict[str, Any], tuple[V4SnapshotMapCoordinate, ...]]
    ] = []
    previous_pack_no = -1
    previous_last: tuple[int, int] | None = None
    for raw_pack in pack_query:
        pack_fields = _row_mapping(raw_pack)
        pack_no = int(pack_fields.get("pack_no") or 0)
        if (
            pack_fields.get("object_kind") != object_kind
            or pack_no <= previous_pack_no
        ):
            raise FinalizerMapError(
                "packed finalizer map query returned an unexpected pack"
            )
        try:
            coordinates = _decode_persisted_map_payload(
                pack_fields,
                object_kind=object_kind,
            )
        except (RuntimeError, ValueError) as exc:
            raise FinalizerMapError(str(exc)) from exc
        _validate_pack_geometry(pack_fields, coordinates)
        first = (coordinates[0].block_key, coordinates[0].fragment_no)
        last = (coordinates[-1].block_key, coordinates[-1].fragment_no)
        if previous_last is not None and first <= previous_last:
            raise FinalizerMapError(
                "packed finalizer map pack ranges overlap"
            )
        previous_pack_no = pack_no
        previous_last = last
        decoded_packs.append((pack_fields, coordinates))
    return decoded_packs


async def load_v4_finalizer_mapping_records(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    block_keys: Iterable[int],
    fragment_nos: Iterable[int] | None,
    row_limit: int,
) -> tuple[dict[str, Any], ...] | None:
    """Return packed logical rows, or ``None`` for an explicit legacy layout."""

    normalized_kind = str(object_kind)
    if normalized_kind not in PTG2_V4_FINALIZER_PACKED_OBJECT_KIND_SET:
        return None
    is_packed = await has_complete_v4_finalizer_map(
        session, schema_name=schema_name, snapshot_key=int(snapshot_key)
    )
    if not is_packed:
        return None
    normalized_row_limit = int(row_limit)
    if normalized_row_limit < 1:
        raise ValueError("packed finalizer map row limit must be positive")
    normalized_block_keys = tuple(sorted({int(block_key) for block_key in block_keys}))
    has_fragment_filter = fragment_nos is not None
    normalized_fragment_nos = (
        tuple(sorted({int(fragment_no) for fragment_no in fragment_nos}))
        if fragment_nos is not None
        else ()
    )
    if not normalized_block_keys or (has_fragment_filter and not normalized_fragment_nos):
        return ()
    schema = _quote_ident(schema_name)
    from process.ptg_parts.ptg2_v4_finalizer_mapping_reader import (
        load_selected_mapping_rows,
    )

    return await load_selected_mapping_rows(
        session,
        schema=schema,
        snapshot_key=int(snapshot_key),
        object_kind=normalized_kind,
        block_keys=normalized_block_keys,
        fragment_nos=normalized_fragment_nos,
        has_fragment_filter=has_fragment_filter,
        row_limit=normalized_row_limit,
    )
