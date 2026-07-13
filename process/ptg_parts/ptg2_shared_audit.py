# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded publish-time audit samples for strict shared-block PTG V3 layouts."""

from __future__ import annotations

import hashlib
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from api.ptg2_db_serving_v3 import (
    PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND,
    _decode_price_membership_block,
)
from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    PTG2_V3_SHARED_FORMAT_VERSION,
    PTG2_V3_SHARED_GENERATION,
    SharedBlockReference,
    SharedLayoutBuildOwnership,
    lock_shared_layout_for_dense_write,
    shared_block_hash,
    shared_mapping_digest,
    shared_support_digest,
)
from process.ptg_parts.ptg2_shared_graph import (
    PTG2_V3_GRAPH_CHUNK_BYTES,
    PTG2_V3_GRAPH_GROUP_TO_NPI,
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
)


PTG2_V3_AUDIT_CONTRACT = "persisted_served_occurrence_sample_v2"
PTG2_V3_AUDIT_METHOD = "publish_time_stratified_v1"
PTG2_V3_AUDIT_CANDIDATE_FORMAT = "ptg2_v3_audit_candidates_v2"
PTG2_V3_AUDIT_CANDIDATE_SELECTION = "equal_interval_assigned_rows_v1"
PTG2_V3_AUDIT_CANDIDATE_RECORD_BYTES = 20
PTG2_V3_AUDIT_MAX_CANDIDATES = 4096
PTG2_V3_AUDIT_MAX_SAMPLE_ROWS = 2560
PTG2_V3_AUDIT_MAX_BLOCK_BYTES = 256 * 1024 * 1024
PTG2_V3_AUDIT_MAX_MEMBER_ATTEMPTS = 1_000_000
PTG2_V3_AUDIT_MAX_COMBINATION_ATTEMPTS = 1_000_000

_CANDIDATE_RECORD = struct.Struct(">IIIII")
_OCCURRENCE_ID_COORDINATES = struct.Struct(">QIIIQQQQ")
_STORED_COORDINATES = struct.Struct(">IIIQQQQ")
_CORE_LAYOUT_DOMAIN = b"PTG2V3AUDITNS\x02"
_OCCURRENCE_DOMAIN = b"PTG2V3AUDITOCC\x02"
_SAMPLE_DIGEST_DOMAIN = b"PTG2V3AUDITROWS\x02"
_GRAPH_SELECTION_DOMAIN = b"PTG2V3AUDITGRAPH\x01"
_GRAPH_LAYOUT = {
    PTG2_V3_GRAPH_GROUP_TO_NPI: ("graph_group_npis_v1", 8),
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP: ("graph_provider_set_groups_v1", 4),
}


@dataclass(frozen=True)
class AuditCandidate:
    code_key: int
    provider_set_key: int
    price_key: int
    source_key: int
    provider_count: int
    candidate_ordinal: int = 0


@dataclass(frozen=True)
class AuditOccurrence:
    occurrence_id: bytes
    code_key: int
    provider_set_key: int
    price_key: int
    source_key: int
    npi: int
    atom_ordinal: int
    atom_key: int
    candidate_ordinal: int


@dataclass(frozen=True)
class SharedAuditPublication:
    metadata: Mapping[str, Any]
    support_digest: bytes
    row_count: int
    core_layout_id: bytes


@dataclass(frozen=True)
class _GraphOwnerLocator:
    first_chunk: int
    member_offset: int
    member_count: int


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"strict V3 finalizer summary is missing {name}")
    return dict(value)


def _integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name}") from exc
    if normalized < 0:
        raise RuntimeError(f"strict V3 finalizer summary has negative {name}")
    return normalized


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _candidate_path(
    finalizer_summary: Mapping[str, Any],
    candidate_summary: Mapping[str, Any],
) -> Path:
    output_root = Path(str(finalizer_summary.get("output_directory") or "")).resolve()
    relative_path = Path(str(candidate_summary.get("path") or ""))
    if not str(relative_path) or relative_path.is_absolute():
        raise RuntimeError("strict V3 audit candidate path is invalid")
    path = (output_root / relative_path).resolve()
    try:
        path.relative_to(output_root)
    except ValueError as exc:
        raise RuntimeError(
            "strict V3 audit candidate path escapes its output directory"
        ) from exc
    if not path.is_file():
        raise RuntimeError("strict V3 audit candidate file is missing")
    return path


def load_audit_candidates(
    finalizer_summary: Mapping[str, Any],
) -> tuple[tuple[AuditCandidate, ...], dict[str, Any]]:
    """Validate and decode the finalizer's bounded, headerless candidate file."""

    candidate_summary = _validated_candidate_summary(finalizer_summary)
    candidate_path = _candidate_path(finalizer_summary, candidate_summary)
    raw_candidate_records = candidate_path.read_bytes()
    candidate_count = _integer(
        candidate_summary.get("row_count"),
        "audit candidate row count",
    )
    expected_byte_count = candidate_count * PTG2_V3_AUDIT_CANDIDATE_RECORD_BYTES
    if len(raw_candidate_records) != expected_byte_count:
        raise RuntimeError(
            "strict V3 audit candidate file size does not match its summary"
        )
    expected_digest = str(candidate_summary.get("row_digest") or "").strip().lower()
    if hashlib.sha256(raw_candidate_records).hexdigest() != expected_digest:
        raise RuntimeError("strict V3 audit candidate digest does not match its file")

    candidates = tuple(
        AuditCandidate(
            *_CANDIDATE_RECORD.unpack_from(raw_candidate_records, byte_offset),
            candidate_ordinal=byte_offset // _CANDIDATE_RECORD.size,
        )
        for byte_offset in range(
            0,
            len(raw_candidate_records),
            _CANDIDATE_RECORD.size,
        )
    )
    dense_count_by_field = _dense_count_by_candidate_field(finalizer_summary)
    for candidate in candidates:
        for field_name, dense_count in dense_count_by_field.items():
            if int(getattr(candidate, field_name)) >= dense_count:
                raise RuntimeError(
                    f"strict V3 audit candidate {field_name} is outside its dense dictionary"
                )
    return candidates, candidate_summary


def _validated_candidate_summary(
    finalizer_summary: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_summary = _mapping(
        finalizer_summary.get("audit_candidates"),
        "audit candidates",
    )
    if candidate_summary.get("record_format") != PTG2_V3_AUDIT_CANDIDATE_FORMAT:
        raise RuntimeError("strict V3 audit candidate format is unsupported")
    if (
        _integer(
            candidate_summary.get("format_version"),
            "audit candidate format version",
        )
        != 2
    ):
        raise RuntimeError("strict V3 audit candidate format version is unsupported")
    if (
        _integer(
            candidate_summary.get("record_bytes"),
            "audit candidate record bytes",
        )
        != PTG2_V3_AUDIT_CANDIDATE_RECORD_BYTES
    ):
        raise RuntimeError("strict V3 audit candidate record width is unsupported")
    maximum_rows = _integer(
        candidate_summary.get("maximum_rows"),
        "audit candidate maximum rows",
    )
    if maximum_rows != PTG2_V3_AUDIT_MAX_CANDIDATES:
        raise RuntimeError("strict V3 audit candidate maximum is unsupported")
    if (
        candidate_summary.get("selection_method")
        != PTG2_V3_AUDIT_CANDIDATE_SELECTION
    ):
        raise RuntimeError("strict V3 audit candidate selection method is unsupported")
    candidate_count = _integer(
        candidate_summary.get("row_count"),
        "audit candidate row count",
    )
    source_row_count = _integer(
        candidate_summary.get("source_row_count"),
        "audit candidate source row count",
    )
    expected_candidate_count = min(source_row_count, PTG2_V3_AUDIT_MAX_CANDIDATES)
    if candidate_count != expected_candidate_count:
        raise RuntimeError(
            "strict V3 audit candidate count does not match the assigned-row population"
        )
    expected_digest = str(candidate_summary.get("row_digest") or "").strip().lower()
    if len(expected_digest) != 64:
        raise RuntimeError("strict V3 audit candidate digest is invalid")
    return candidate_summary


def _dense_count_by_candidate_field(
    finalizer_summary: Mapping[str, Any],
) -> dict[str, int]:
    dense_keys = _mapping(finalizer_summary.get("dense_keys"), "dense keys")
    return {
        "code_key": _integer(
            _mapping(dense_keys.get("code"), "dense code keys").get("count"),
            "dense code key count",
        ),
        "provider_set_key": _integer(
            _mapping(
                dense_keys.get("provider_set"),
                "dense provider-set keys",
            ).get("count"),
            "dense provider-set key count",
        ),
        "price_key": _integer(
            _mapping(dense_keys.get("price"), "dense price keys").get("count"),
            "dense price key count",
        ),
        "source_key": _integer(
            finalizer_summary.get("source_count"),
            "dense source key count",
        ),
    }


class _ReadBudget:
    def __init__(self) -> None:
        self.block_bytes = 0
        self.member_attempts = 0
        self.combination_attempts = 0
        self._blocks: set[tuple[str, int, int]] = set()

    def add_block(
        self, object_kind: str, block_key: int, fragment_no: int, raw_bytes: int
    ) -> None:
        """Charge one unique stored block fragment against the byte cap."""

        identity = (str(object_kind), int(block_key), int(fragment_no))
        if identity in self._blocks:
            return
        next_total = self.block_bytes + int(raw_bytes)
        if next_total > PTG2_V3_AUDIT_MAX_BLOCK_BYTES:
            raise RuntimeError("strict V3 audit block-byte cap was exceeded")
        self._blocks.add(identity)
        self.block_bytes = next_total

    def available_members(self) -> int:
        """Return the number of graph-member reads still permitted."""

        return max(PTG2_V3_AUDIT_MAX_MEMBER_ATTEMPTS - self.member_attempts, 0)

    def add_members(self, count: int) -> None:
        """Charge graph-member reads, failing before the cap is exceeded."""

        normalized = int(count)
        if normalized < 0 or normalized > self.available_members():
            raise RuntimeError("strict V3 audit graph-member cap was exceeded")
        self.member_attempts += normalized

    def add_combination(self) -> None:
        """Charge one occurrence-coordinate attempt against the hard cap."""

        self.combination_attempts += 1
        if self.combination_attempts > PTG2_V3_AUDIT_MAX_COMBINATION_ATTEMPTS:
            raise RuntimeError("strict V3 audit coordinate-attempt cap was exceeded")


class _BuildingBlockReader:
    def __init__(
        self,
        session: Any,
        *,
        schema_name: str,
        snapshot_key: int,
        budget: _ReadBudget,
    ) -> None:
        self.session = session
        self.schema_name = str(schema_name)
        self.snapshot_key = int(snapshot_key)
        self.budget = budget
        self._cache: dict[tuple[str, int], tuple[bytes, int]] = {}

    async def logical_blocks(
        self,
        object_kind: str,
        block_keys: Iterable[int],
    ) -> dict[int, tuple[bytes, int]]:
        """Return complete logical blocks after validating every stored fragment."""

        requested_block_keys = tuple(
            sorted({int(block_key) for block_key in block_keys})
        )
        if not requested_block_keys:
            return {}
        missing_block_keys = tuple(
            block_key
            for block_key in requested_block_keys
            if (str(object_kind), block_key) not in self._cache
        )
        if missing_block_keys:
            await self._fetch(str(object_kind), missing_block_keys)
        absent_block_keys = [
            block_key
            for block_key in requested_block_keys
            if (str(object_kind), block_key) not in self._cache
        ]
        if absent_block_keys:
            raise RuntimeError(
                "strict V3 audit is missing "
                f"{object_kind} blocks: {absent_block_keys[:8]}"
            )
        return {
            block_key: self._cache[(str(object_kind), block_key)]
            for block_key in requested_block_keys
        }

    async def _fetch(self, object_kind: str, block_keys: Sequence[int]) -> None:
        """Fetch, validate, decode, and cache requested logical blocks."""

        fragment_rows_by_block = await self._fetch_fragment_rows(
            object_kind,
            block_keys,
        )
        for block_key, fragments_by_number in fragment_rows_by_block.items():
            self._cache[(object_kind, block_key)] = self._assemble_fragments(
                fragments_by_number
            )

    async def _fetch_fragment_rows(
        self,
        object_kind: str,
        block_keys: Sequence[int],
    ) -> dict[int, dict[int, tuple[bytes, int]]]:
        schema = _quote_ident(self.schema_name)
        fragment_result = await self.session.execute(
            db.text(
                f"""
                SELECT mapping.object_kind, mapping.block_key, mapping.fragment_no,
                       mapping.entry_count AS mapping_entry_count, mapping.block_hash,
                       block.format_version, block.object_kind AS block_object_kind,
                       block.codec, block.entry_count AS block_entry_count,
                       block.raw_byte_count, block.stored_byte_count, block.payload
                  FROM {schema}.ptg2_v3_snapshot_block mapping
                  JOIN {schema}.ptg2_v3_block block
                    ON block.block_hash = mapping.block_hash
                 WHERE mapping.snapshot_key = :snapshot_key
                   AND mapping.object_kind = :object_kind
                   AND mapping.block_key = ANY(CAST(:block_keys AS bigint[]))
                 ORDER BY mapping.block_key, mapping.fragment_no
                """
            ),
            {
                "snapshot_key": self.snapshot_key,
                "object_kind": object_kind,
                "block_keys": tuple(block_keys),
            },
        )
        fragment_rows_by_block: dict[int, dict[int, tuple[bytes, int]]] = {}
        for raw_database_row in fragment_result:
            database_row = _row_mapping(raw_database_row)
            block_key, fragment_no, fragment_content = self._decode_fragment(
                object_kind,
                database_row,
            )
            fragments_by_number = fragment_rows_by_block.setdefault(block_key, {})
            if fragment_no in fragments_by_number:
                raise RuntimeError("strict V3 audit block contains duplicate fragments")
            fragments_by_number[fragment_no] = fragment_content
        return fragment_rows_by_block

    def _decode_fragment(
        self,
        object_kind: str,
        database_row: Mapping[str, Any],
    ) -> tuple[int, int, tuple[bytes, int]]:
        block_key = int(database_row["block_key"])
        fragment_no = int(database_row["fragment_no"])
        mapping_entry_count = int(database_row["mapping_entry_count"])
        if (
            str(database_row["object_kind"]) != object_kind
            or str(database_row["block_object_kind"]) != object_kind
        ):
            raise RuntimeError("strict V3 audit block kind changed during publication")
        if int(database_row["block_entry_count"]) != mapping_entry_count:
            raise RuntimeError("strict V3 audit block entry count is inconsistent")
        format_version = int(database_row["format_version"])
        if format_version != PTG2_V3_SHARED_FORMAT_VERSION:
            raise RuntimeError("strict V3 audit block format version is incompatible")
        stored_payload = bytes(database_row["payload"])
        if len(stored_payload) != int(database_row["stored_byte_count"]):
            raise RuntimeError("strict V3 audit stored block length is inconsistent")
        codec = str(database_row["codec"])
        expected_hash = shared_block_hash(
            format_version=format_version,
            object_kind=object_kind,
            codec=codec,
            payload=stored_payload,
        )
        if bytes(database_row["block_hash"]) != expected_hash:
            raise RuntimeError("strict V3 audit block hash validation failed")
        raw_byte_count = int(database_row["raw_byte_count"])
        self.budget.add_block(object_kind, block_key, fragment_no, raw_byte_count)
        raw_payload = self._decompress_payload(codec, stored_payload)
        if len(raw_payload) != raw_byte_count:
            raise RuntimeError("strict V3 audit raw block length is inconsistent")
        return block_key, fragment_no, (raw_payload, mapping_entry_count)

    @staticmethod
    def _decompress_payload(codec: str, stored_payload: bytes) -> bytes:
        if codec == "none":
            return stored_payload
        if codec == "zlib":
            try:
                return zlib.decompress(stored_payload)
            except zlib.error as exc:
                raise RuntimeError(
                    "strict V3 audit block decompression failed"
                ) from exc
        raise RuntimeError("strict V3 audit block codec is unsupported")

    @staticmethod
    def _assemble_fragments(
        fragments_by_number: Mapping[int, tuple[bytes, int]],
    ) -> tuple[bytes, int]:
        fragment_numbers = tuple(sorted(fragments_by_number))
        if fragment_numbers != tuple(range(len(fragment_numbers))):
            raise RuntimeError("strict V3 audit block fragments are not contiguous")
        entry_count = fragments_by_number[0][1]
        if any(
            fragments_by_number[fragment_no][1] != 0
            for fragment_no in fragment_numbers[1:]
        ):
            raise RuntimeError("strict V3 audit continuation fragment has entries")
        raw_payload = b"".join(
            fragments_by_number[fragment_no][0]
            for fragment_no in fragment_numbers
        )
        return raw_payload, entry_count


async def _validate_building_layout(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
) -> None:
    await lock_shared_layout_for_dense_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )


async def _validate_candidate_provider_counts(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    candidates: Sequence[AuditCandidate],
) -> None:
    expected_count_by_provider_set = {}
    for candidate in candidates:
        previous_count = expected_count_by_provider_set.setdefault(
            candidate.provider_set_key, candidate.provider_count
        )
        if previous_count != candidate.provider_count:
            raise RuntimeError(
                "strict V3 audit candidates disagree on one provider-set count"
            )
    if not expected_count_by_provider_set:
        return
    schema = _quote_ident(schema_name)
    provider_count_result = await session.execute(
        db.text(
            f"""
            SELECT provider_set_key, provider_count
              FROM {schema}.ptg2_v3_provider_set
             WHERE snapshot_key = :snapshot_key
               AND provider_set_key = ANY(CAST(:provider_set_keys AS integer[]))
             ORDER BY provider_set_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "provider_set_keys": tuple(sorted(expected_count_by_provider_set)),
        },
    )
    observed_count_by_provider_set = {
        int(_row_mapping(database_row)["provider_set_key"]): int(
            _row_mapping(database_row)["provider_count"]
        )
        for database_row in provider_count_result
    }
    if observed_count_by_provider_set != expected_count_by_provider_set:
        raise RuntimeError(
            "strict V3 audit candidate provider counts disagree with PostgreSQL"
        )


async def _validate_snapshot_source_dictionary(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: str,
    source_count: int,
    required_source_keys: Iterable[int],
) -> None:
    normalized_source_count = int(source_count)
    if normalized_source_count <= 0:
        raise RuntimeError("strict V3 audit requires a positive source_count")
    schema = _quote_ident(schema_name)
    source_key_result = await session.execute(
        db.text(
            f"""
            SELECT source_key
              FROM {schema}.ptg2_v3_snapshot_source
             WHERE snapshot_id = :snapshot_id
             ORDER BY source_key
            """
        ),
        {"snapshot_id": str(logical_snapshot_id)},
    )
    observed_source_keys = tuple(
        int(_row_mapping(database_row)["source_key"])
        for database_row in source_key_result
    )
    expected_source_keys = tuple(range(normalized_source_count))
    if observed_source_keys != expected_source_keys:
        raise RuntimeError(
            "strict V3 audit snapshot source dictionary is not complete and dense"
        )
    missing_source_keys = {
        int(source_key) for source_key in required_source_keys
    } - set(observed_source_keys)
    if missing_source_keys:
        raise RuntimeError(
            "strict V3 audit occurrence source keys are absent from the snapshot source dictionary"
        )


async def _graph_owner_locators(
    reader: _BuildingBlockReader,
    *,
    direction: int,
    owner_keys: Iterable[int],
) -> dict[int, _GraphOwnerLocator]:
    try:
        _GRAPH_LAYOUT[int(direction)]
    except (KeyError, ValueError) as exc:
        raise ValueError(
            f"unsupported strict V3 audit graph direction: {direction!r}"
        ) from exc
    requested_owner_keys = tuple(sorted({int(owner_key) for owner_key in owner_keys}))
    if not requested_owner_keys:
        return {}
    schema = _quote_ident(reader.schema_name)
    locator_result = await reader.session.execute(
        db.text(
            f"""
            SELECT owner_key, first_chunk, member_offset, member_count
              FROM {schema}.ptg2_v3_graph_owner
             WHERE snapshot_key = :snapshot_key
               AND direction = :direction
               AND owner_key = ANY(CAST(:owner_keys AS bigint[]))
             ORDER BY owner_key
            """
        ),
        {
            "snapshot_key": reader.snapshot_key,
            "direction": int(direction),
            "owner_keys": requested_owner_keys,
        },
    )
    locator_by_owner_key: dict[int, _GraphOwnerLocator] = {}
    for raw_database_row in locator_result:
        database_row = _row_mapping(raw_database_row)
        owner_key = int(database_row["owner_key"])
        member_count = int(database_row["member_count"])
        locator = _GraphOwnerLocator(
            first_chunk=int(database_row["first_chunk"]),
            member_offset=int(database_row["member_offset"]),
            member_count=member_count,
        )
        if (
            locator.first_chunk < 0
            or locator.member_offset < 0
            or locator.member_offset >= PTG2_V3_GRAPH_CHUNK_BYTES
            or locator.member_count < 0
        ):
            raise RuntimeError("strict V3 audit graph owner locator is invalid")
        if owner_key in locator_by_owner_key:
            raise RuntimeError("strict V3 audit graph owner locator is duplicated")
        locator_by_owner_key[owner_key] = locator
    return locator_by_owner_key


def _deterministic_member_ordinal(
    core_layout_id: bytes,
    *,
    candidate_ordinal: int,
    selection_kind: bytes,
    owner_key: int,
    expansion_ordinal: int,
    member_count: int,
) -> int:
    if len(core_layout_id) != 32:
        raise ValueError("strict V3 audit core layout id must contain 32 bytes")
    normalized_kind = bytes(selection_kind)
    if not normalized_kind or len(normalized_kind) > 32:
        raise ValueError("strict V3 audit graph selection kind is invalid")
    normalized_count = int(member_count)
    if normalized_count <= 0:
        raise ValueError("strict V3 audit cannot select from an empty graph owner")
    digest = hashlib.sha256()
    digest.update(_GRAPH_SELECTION_DOMAIN)
    digest.update(core_layout_id)
    digest.update(struct.pack(">B", len(normalized_kind)))
    digest.update(normalized_kind)
    digest.update(
        struct.pack(
            ">QQQ",
            int(candidate_ordinal),
            int(owner_key),
            int(expansion_ordinal),
        )
    )
    return int.from_bytes(digest.digest()[:8], "big", signed=False) % normalized_count


async def _graph_targeted_members(
    reader: _BuildingBlockReader,
    *,
    direction: int,
    member_ordinals_by_owner: Mapping[int, Iterable[int]],
    locators: Mapping[int, _GraphOwnerLocator] | None = None,
) -> dict[int, dict[int, int]]:
    """Read only graph chunks containing the requested owner ordinals."""

    try:
        object_kind, member_width = _GRAPH_LAYOUT[int(direction)]
    except (KeyError, ValueError) as exc:
        raise ValueError(
            f"unsupported strict V3 audit graph direction: {direction!r}"
        ) from exc
    requested_ordinals_by_owner = {
        int(owner_key): tuple(sorted({int(ordinal) for ordinal in ordinals}))
        for owner_key, ordinals in member_ordinals_by_owner.items()
    }
    if not requested_ordinals_by_owner:
        return {}
    locator_by_owner_key = (
        dict(locators)
        if locators is not None
        else await _graph_owner_locators(
            reader,
            direction=direction,
            owner_keys=requested_ordinals_by_owner,
        )
    )
    selected_count = sum(
        len(ordinals) for ordinals in requested_ordinals_by_owner.values()
    )
    reader.budget.add_members(selected_count)
    block_keys = _required_graph_block_keys(
        requested_ordinals_by_owner,
        locator_by_owner_key,
        member_width,
    )
    block_payload_by_key = await reader.logical_blocks(object_kind, block_keys)
    return _decode_graph_members(
        requested_ordinals_by_owner,
        locator_by_owner_key,
        block_payload_by_key,
        member_width,
    )


def _required_graph_block_keys(
    requested_ordinals_by_owner: Mapping[int, Sequence[int]],
    locator_by_owner_key: Mapping[int, _GraphOwnerLocator],
    member_width: int,
) -> set[int]:
    block_keys: set[int] = set()
    for owner_key, ordinals in requested_ordinals_by_owner.items():
        locator = locator_by_owner_key.get(owner_key)
        if locator is None:
            if ordinals:
                raise RuntimeError("strict V3 audit graph owner is missing")
            continue
        if locator.member_offset % member_width:
            raise RuntimeError("strict V3 audit graph owner offset is unaligned")
        for ordinal in ordinals:
            if ordinal < 0 or ordinal >= locator.member_count:
                raise RuntimeError(
                    "strict V3 audit graph member ordinal is out of range"
                )
            absolute_offset = locator.member_offset + ordinal * member_width
            first_chunk = (
                locator.first_chunk + absolute_offset // PTG2_V3_GRAPH_CHUNK_BYTES
            )
            last_chunk = (
                locator.first_chunk
                + (absolute_offset + member_width - 1) // PTG2_V3_GRAPH_CHUNK_BYTES
            )
            block_keys.add(first_chunk)
            block_keys.add(last_chunk)
    return block_keys


def _decode_graph_members(
    requested_ordinals_by_owner: Mapping[int, Sequence[int]],
    locator_by_owner_key: Mapping[int, _GraphOwnerLocator],
    block_payload_by_key: Mapping[int, tuple[bytes, int]],
    member_width: int,
) -> dict[int, dict[int, int]]:
    member_by_ordinal_by_owner: dict[int, dict[int, int]] = {}
    for owner_key, ordinals in requested_ordinals_by_owner.items():
        locator = locator_by_owner_key.get(owner_key)
        if locator is None:
            continue
        member_by_ordinal: dict[int, int] = {}
        for ordinal in ordinals:
            member_by_ordinal[ordinal] = _decode_graph_member(
                block_payload_by_key,
                locator,
                ordinal,
                member_width,
            )
        ordered_members = tuple(
            member_by_ordinal[ordinal] for ordinal in sorted(member_by_ordinal)
        )
        if any(
            left >= right for left, right in zip(ordered_members, ordered_members[1:])
        ):
            raise RuntimeError("strict V3 audit graph members are not strictly ordered")
        member_by_ordinal_by_owner[owner_key] = member_by_ordinal
    return member_by_ordinal_by_owner


def _decode_graph_member(
    block_payload_by_key: Mapping[int, tuple[bytes, int]],
    locator: _GraphOwnerLocator,
    ordinal: int,
    member_width: int,
) -> int:
    absolute_offset = locator.member_offset + ordinal * member_width
    first_chunk = locator.first_chunk + absolute_offset // PTG2_V3_GRAPH_CHUNK_BYTES
    local_offset = absolute_offset % PTG2_V3_GRAPH_CHUNK_BYTES
    first_payload = block_payload_by_key[first_chunk][0]
    if local_offset > len(first_payload):
        raise RuntimeError("strict V3 audit graph member offset is truncated")
    member_slice = slice(local_offset, local_offset + member_width)
    member_bytes = first_payload[member_slice]
    if len(member_bytes) != member_width:
        next_payload = block_payload_by_key.get(first_chunk + 1, (b"", 0))[0]
        member_bytes += next_payload[: member_width - len(member_bytes)]
    if len(member_bytes) != member_width:
        raise RuntimeError("strict V3 audit graph member stream is truncated")
    return int.from_bytes(member_bytes, "little", signed=False)


async def _candidate_npis_by_ordinal(
    reader: _BuildingBlockReader,
    *,
    candidates: Sequence[AuditCandidate],
    price_memberships: Mapping[int, Sequence[int]],
    core_layout_id: bytes,
) -> dict[int, tuple[int, ...]]:
    """Select bounded, deterministic NPIs for each candidate occurrence."""

    provider_locator_by_key = await _graph_owner_locators(
        reader,
        direction=PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
        owner_keys=(candidate.provider_set_key for candidate in candidates),
    )
    active_candidates = tuple(
        candidate
        for candidate in candidates
        if provider_locator_by_key.get(
            candidate.provider_set_key,
            _GraphOwnerLocator(0, 0, 0),
        ).member_count
        > 0
        and price_memberships.get(candidate.price_key)
    )
    if not active_candidates:
        return {candidate.candidate_ordinal: () for candidate in candidates}
    selections_per_candidate = min(
        PTG2_V3_AUDIT_MAX_SAMPLE_ROWS,
        max(
            1,
            (PTG2_V3_AUDIT_MAX_SAMPLE_ROWS + len(active_candidates) - 1)
            // len(active_candidates),
        ),
    )
    group_key_by_selection = await _selected_provider_groups(
        reader,
        active_candidates=active_candidates,
        provider_locator_by_key=provider_locator_by_key,
        core_layout_id=core_layout_id,
        selections_per_candidate=selections_per_candidate,
    )
    npi_ordinal_by_selection, npi_by_ordinal_by_group = await _selected_group_npis(
        reader,
        active_candidates=active_candidates,
        group_key_by_selection=group_key_by_selection,
        core_layout_id=core_layout_id,
        selections_per_candidate=selections_per_candidate,
    )
    return _candidate_npis_from_selections(
        candidates,
        selections_per_candidate=selections_per_candidate,
        group_key_by_selection=group_key_by_selection,
        npi_ordinal_by_selection=npi_ordinal_by_selection,
        npi_by_ordinal_by_group=npi_by_ordinal_by_group,
    )


async def _selected_provider_groups(
    reader: _BuildingBlockReader,
    *,
    active_candidates: Sequence[AuditCandidate],
    provider_locator_by_key: Mapping[int, _GraphOwnerLocator],
    core_layout_id: bytes,
    selections_per_candidate: int,
) -> dict[tuple[int, int], int]:
    group_ordinals_by_owner: dict[int, set[int]] = {}
    group_ordinal_by_selection: dict[tuple[int, int], int] = {}
    for candidate in active_candidates:
        locator = provider_locator_by_key[candidate.provider_set_key]
        for expansion_ordinal in range(selections_per_candidate):
            group_ordinal = _deterministic_member_ordinal(
                core_layout_id,
                candidate_ordinal=candidate.candidate_ordinal,
                selection_kind=b"provider-group",
                owner_key=candidate.provider_set_key,
                expansion_ordinal=expansion_ordinal,
                member_count=locator.member_count,
            )
            group_ordinals_by_owner.setdefault(candidate.provider_set_key, set()).add(
                group_ordinal
            )
            group_ordinal_by_selection[
                (candidate.candidate_ordinal, expansion_ordinal)
            ] = group_ordinal
    group_by_ordinal_by_owner = await _graph_targeted_members(
        reader,
        direction=PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
        member_ordinals_by_owner=group_ordinals_by_owner,
        locators=provider_locator_by_key,
    )
    group_key_by_selection: dict[tuple[int, int], int] = {}
    for candidate in active_candidates:
        for expansion_ordinal in range(selections_per_candidate):
            selection = (candidate.candidate_ordinal, expansion_ordinal)
            group_ordinal = group_ordinal_by_selection[selection]
            group_key_by_selection[selection] = group_by_ordinal_by_owner[
                candidate.provider_set_key
            ][
                group_ordinal
            ]
    return group_key_by_selection


async def _selected_group_npis(
    reader: _BuildingBlockReader,
    *,
    active_candidates: Sequence[AuditCandidate],
    group_key_by_selection: Mapping[tuple[int, int], int],
    core_layout_id: bytes,
    selections_per_candidate: int,
) -> tuple[dict[tuple[int, int], int], dict[int, dict[int, int]]]:
    group_locator_by_key = await _graph_owner_locators(
        reader,
        direction=PTG2_V3_GRAPH_GROUP_TO_NPI,
        owner_keys=group_key_by_selection.values(),
    )
    npi_ordinals_by_owner: dict[int, set[int]] = {}
    npi_ordinal_by_selection: dict[tuple[int, int], int] = {}
    for candidate in active_candidates:
        for expansion_ordinal in range(selections_per_candidate):
            selection = (candidate.candidate_ordinal, expansion_ordinal)
            group_key = group_key_by_selection[selection]
            locator = group_locator_by_key.get(group_key)
            if locator is None or locator.member_count <= 0:
                continue
            npi_ordinal = _deterministic_member_ordinal(
                core_layout_id,
                candidate_ordinal=candidate.candidate_ordinal,
                selection_kind=b"group-npi",
                owner_key=group_key,
                expansion_ordinal=expansion_ordinal,
                member_count=locator.member_count,
            )
            npi_ordinals_by_owner.setdefault(group_key, set()).add(npi_ordinal)
            npi_ordinal_by_selection[selection] = npi_ordinal
    npi_by_ordinal_by_group = await _graph_targeted_members(
        reader,
        direction=PTG2_V3_GRAPH_GROUP_TO_NPI,
        member_ordinals_by_owner=npi_ordinals_by_owner,
        locators=group_locator_by_key,
    )
    return npi_ordinal_by_selection, npi_by_ordinal_by_group


def _candidate_npis_from_selections(
    candidates: Sequence[AuditCandidate],
    *,
    selections_per_candidate: int,
    group_key_by_selection: Mapping[tuple[int, int], int],
    npi_ordinal_by_selection: Mapping[tuple[int, int], int],
    npi_by_ordinal_by_group: Mapping[int, Mapping[int, int]],
) -> dict[int, tuple[int, ...]]:
    candidate_npis_by_ordinal: dict[int, tuple[int, ...]] = {}
    for candidate in candidates:
        unique_npis_by_value: dict[int, None] = {}
        for expansion_ordinal in range(selections_per_candidate):
            selection = (candidate.candidate_ordinal, expansion_ordinal)
            npi_ordinal = npi_ordinal_by_selection.get(selection)
            if npi_ordinal is None:
                continue
            group_key = group_key_by_selection[selection]
            npi = npi_by_ordinal_by_group[group_key][npi_ordinal]
            unique_npis_by_value.setdefault(npi, None)
        candidate_npis_by_ordinal[candidate.candidate_ordinal] = tuple(
            unique_npis_by_value
        )
    return candidate_npis_by_ordinal


async def _price_memberships(
    reader: _BuildingBlockReader,
    *,
    price_keys: Iterable[int],
    atom_key_bits: int,
    block_span: int,
) -> dict[int, tuple[int, ...]]:
    requested_price_keys = tuple(sorted({int(price_key) for price_key in price_keys}))
    normalized_block_span = int(block_span)
    if normalized_block_span <= 0 or normalized_block_span > 1 << 24:
        raise RuntimeError("strict V3 audit price-membership block span is invalid")
    block_keys = {
        price_key // normalized_block_span for price_key in requested_price_keys
    }
    block_payload_by_key = await reader.logical_blocks(
        PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND,
        block_keys,
    )
    requested_price_key_set = set(requested_price_keys)
    membership_by_price_key: dict[int, tuple[int, ...]] = {}
    for block_key, (block_payload, entry_count) in block_payload_by_key.items():
        membership_by_price_key.update(
            _decode_price_membership_block(
                block_payload,
                block_key=block_key,
                entry_count=entry_count,
                atom_key_bits=int(atom_key_bits),
                block_span=normalized_block_span,
                requested_price_keys=requested_price_key_set,
            )
        )
    missing_price_keys = requested_price_key_set.difference(membership_by_price_key)
    empty_price_keys = {
        price_key
        for price_key, atom_keys in membership_by_price_key.items()
        if not atom_keys
    }
    if missing_price_keys or empty_price_keys:
        raise RuntimeError(
            "strict V3 audit candidate price membership is missing or empty"
        )
    return membership_by_price_key


def occurrence_id(
    core_layout_id: bytes,
    *,
    code_key: int,
    provider_set_key: int,
    price_key: int,
    source_key: int,
    npi: int,
    atom_ordinal: int,
    atom_key: int,
    candidate_ordinal: int = 0,
) -> bytes:
    """Hash one fully qualified served occurrence into its stable identity."""

    if len(core_layout_id) != 32:
        raise ValueError("strict V3 audit core layout id must contain 32 bytes")
    coordinates = _OCCURRENCE_ID_COORDINATES.pack(
        int(candidate_ordinal),
        int(code_key),
        int(provider_set_key),
        int(price_key),
        int(source_key),
        int(npi),
        int(atom_ordinal),
        int(atom_key),
    )
    return hashlib.sha256(_OCCURRENCE_DOMAIN + core_layout_id + coordinates).digest()


def _occurrence(
    core_layout_id: bytes,
    candidate: AuditCandidate,
    npi: int,
    atom_ordinal: int,
    atom_key: int,
) -> AuditOccurrence:
    if int(npi) < 1_000_000_000 or int(npi) > 9_999_999_999:
        raise RuntimeError("strict V3 audit graph contains an invalid NPI")
    return AuditOccurrence(
        occurrence_id=occurrence_id(
            core_layout_id,
            code_key=candidate.code_key,
            provider_set_key=candidate.provider_set_key,
            price_key=candidate.price_key,
            source_key=candidate.source_key,
            npi=int(npi),
            atom_ordinal=int(atom_ordinal),
            atom_key=int(atom_key),
            candidate_ordinal=candidate.candidate_ordinal,
        ),
        code_key=candidate.code_key,
        provider_set_key=candidate.provider_set_key,
        price_key=candidate.price_key,
        source_key=candidate.source_key,
        npi=int(npi),
        atom_ordinal=int(atom_ordinal),
        atom_key=int(atom_key),
        candidate_ordinal=candidate.candidate_ordinal,
    )


def build_audit_occurrences(
    *,
    candidates: Sequence[AuditCandidate],
    provider_npis: Mapping[int, Sequence[int]],
    price_memberships: Mapping[int, Sequence[int]],
    core_layout_id: bytes,
    budget: _ReadBudget,
    maximum_rows: int = PTG2_V3_AUDIT_MAX_SAMPLE_ROWS,
) -> tuple[AuditOccurrence, ...]:
    """Choose one coordinate per serving candidate, then bounded expansions."""

    row_limit = int(maximum_rows)
    occurrence_by_id: dict[bytes, AuditOccurrence] = {}

    for candidate in candidates:
        candidate_npis = tuple(provider_npis.get(candidate.candidate_ordinal, ()))
        atom_keys = tuple(price_memberships.get(candidate.price_key, ()))
        if candidate_npis and atom_keys:
            _store_audit_occurrence(
                occurrence_by_id,
                core_layout_id=core_layout_id,
                candidate=candidate,
                npi=int(candidate_npis[0]),
                atom_ordinal=0,
                atom_key=int(atom_keys[0]),
                budget=budget,
                row_limit=row_limit,
            )
        if len(occurrence_by_id) >= row_limit:
            break

    if len(occurrence_by_id) < row_limit:
        for candidate in candidates:
            _store_candidate_expansions(
                occurrence_by_id,
                core_layout_id=core_layout_id,
                candidate=candidate,
                candidate_npis=provider_npis.get(candidate.candidate_ordinal, ()),
                atom_keys=price_memberships.get(candidate.price_key, ()),
                budget=budget,
                row_limit=row_limit,
            )
            if len(occurrence_by_id) >= row_limit:
                break
    return tuple(
        sorted(
            occurrence_by_id.values(),
            key=lambda occurrence: occurrence.occurrence_id,
        )
    )


def _store_candidate_expansions(
    occurrence_by_id: dict[bytes, AuditOccurrence],
    *,
    core_layout_id: bytes,
    candidate: AuditCandidate,
    candidate_npis: Sequence[int],
    atom_keys: Sequence[int],
    budget: _ReadBudget,
    row_limit: int,
) -> None:
    for npi in candidate_npis:
        for atom_ordinal, atom_key in enumerate(atom_keys):
            _store_audit_occurrence(
                occurrence_by_id,
                core_layout_id=core_layout_id,
                candidate=candidate,
                npi=int(npi),
                atom_ordinal=atom_ordinal,
                atom_key=int(atom_key),
                budget=budget,
                row_limit=row_limit,
            )
            if len(occurrence_by_id) >= row_limit:
                return


def _store_audit_occurrence(
    occurrence_by_id: dict[bytes, AuditOccurrence],
    *,
    core_layout_id: bytes,
    candidate: AuditCandidate,
    npi: int,
    atom_ordinal: int,
    atom_key: int,
    budget: _ReadBudget,
    row_limit: int,
) -> None:
    if len(occurrence_by_id) >= row_limit:
        return
    budget.add_combination()
    occurrence = _occurrence(core_layout_id, candidate, npi, atom_ordinal, atom_key)
    previous_occurrence = occurrence_by_id.setdefault(
        occurrence.occurrence_id,
        occurrence,
    )
    if previous_occurrence != occurrence:
        raise RuntimeError("strict V3 audit occurrence hash collision")


def _sample_digest(occurrences: Sequence[AuditOccurrence]) -> bytes:
    digest = hashlib.sha256()
    digest.update(_SAMPLE_DIGEST_DOMAIN)
    for occurrence in sorted(
        occurrences,
        key=lambda audit_occurrence: audit_occurrence.occurrence_id,
    ):
        digest.update(occurrence.occurrence_id)
        digest.update(
            _STORED_COORDINATES.pack(
                occurrence.code_key,
                occurrence.provider_set_key,
                occurrence.price_key,
                occurrence.source_key,
                occurrence.npi,
                occurrence.atom_ordinal,
                occurrence.atom_key,
            )
        )
    return digest.digest()


def persisted_audit_sample_digest(rows: Sequence[Mapping[str, Any]]) -> str:
    """Return the sealed digest for rows read from ``ptg2_v3_audit_occurrence``."""

    occurrences = tuple(
        AuditOccurrence(
            occurrence_id=bytes(row["occurrence_id"]),
            code_key=int(row["code_key"]),
            provider_set_key=int(row["provider_set_key"]),
            price_key=int(row["price_key"]),
            source_key=int(row["source_key"]),
            npi=int(row["npi"]),
            atom_ordinal=int(row["atom_ordinal"]),
            atom_key=int(row["atom_key"]),
            candidate_ordinal=0,
        )
        for row in rows
    )
    return _sample_digest(occurrences).hex()


async def _insert_occurrences(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    audit_occurrences: Sequence[AuditOccurrence],
) -> None:
    schema = _quote_ident(schema_name)
    await session.execute(
        db.text(
            f"DELETE FROM {schema}.ptg2_v3_audit_occurrence "
            "WHERE snapshot_key = :snapshot_key"
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    insert_statement = db.text(
        f"""
        INSERT INTO {schema}.ptg2_v3_audit_occurrence
            (snapshot_key, occurrence_id, code_key, provider_set_key,
             price_key, source_key, npi, atom_ordinal, atom_key)
        VALUES
            (:snapshot_key, :occurrence_id, :code_key, :provider_set_key,
             :price_key, :source_key, :npi, :atom_ordinal, :atom_key)
        """
    )
    for batch_start in range(0, len(audit_occurrences), 1000):
        batch_end = batch_start + 1000
        occurrence_batch = audit_occurrences[batch_start:batch_end]
        await session.execute(
            insert_statement,
            [
                {
                    "snapshot_key": int(snapshot_key),
                    "occurrence_id": occurrence.occurrence_id,
                    "code_key": occurrence.code_key,
                    "provider_set_key": occurrence.provider_set_key,
                    "price_key": occurrence.price_key,
                    "source_key": occurrence.source_key,
                    "npi": occurrence.npi,
                    "atom_ordinal": occurrence.atom_ordinal,
                    "atom_key": occurrence.atom_key,
                }
                for occurrence in occurrence_batch
            ],
        )


async def publish_shared_audit_sample(
    *,
    schema_name: str,
    build_ownership: SharedLayoutBuildOwnership,
    logical_snapshot_id: str,
    finalizer_summary: Mapping[str, Any],
    expected_blocks: Sequence[SharedBlockReference],
    core_support_digest: bytes,
    atom_key_bits: int,
    price_membership_block_span: int,
) -> SharedAuditPublication:
    """Persist a bounded sample before sealing a physical shared layout."""

    normalized_core_support = bytes(core_support_digest)
    if len(normalized_core_support) != 32:
        raise ValueError("strict V3 audit core support digest must contain 32 bytes")
    candidates, candidate_summary = load_audit_candidates(finalizer_summary)
    source_count = _integer(finalizer_summary.get("source_count"), "source_count")
    mapping_digest = shared_mapping_digest(expected_blocks)
    core_layout_id = hashlib.sha256(
        _CORE_LAYOUT_DOMAIN + mapping_digest + normalized_core_support
    ).digest()
    audit_occurrences, budget = await _persist_publication_occurrences(
        schema_name=schema_name,
        build_ownership=build_ownership,
        logical_snapshot_id=logical_snapshot_id,
        candidates=candidates,
        source_count=source_count,
        core_layout_id=core_layout_id,
        atom_key_bits=atom_key_bits,
        price_membership_block_span=price_membership_block_span,
    )
    metadata = _publication_metadata(
        audit_occurrences=audit_occurrences,
        candidates=candidates,
        candidate_summary=candidate_summary,
        source_count=source_count,
        core_layout_id=core_layout_id,
        price_membership_block_span=price_membership_block_span,
        budget=budget,
    )
    return SharedAuditPublication(
        metadata=metadata,
        support_digest=shared_support_digest({"audit_sample": metadata}),
        row_count=len(audit_occurrences),
        core_layout_id=core_layout_id,
    )


async def _persist_publication_occurrences(
    *,
    schema_name: str,
    build_ownership: SharedLayoutBuildOwnership,
    logical_snapshot_id: str,
    candidates: Sequence[AuditCandidate],
    source_count: int,
    core_layout_id: bytes,
    atom_key_bits: int,
    price_membership_block_span: int,
) -> tuple[tuple[AuditOccurrence, ...], _ReadBudget]:
    snapshot_key = int(build_ownership.snapshot_key)
    budget = _ReadBudget()
    async with db.transaction() as session:
        await _validate_publication_context(
            session,
            schema_name=schema_name,
            build_ownership=build_ownership,
            logical_snapshot_id=logical_snapshot_id,
            candidates=candidates,
            source_count=source_count,
        )
        reader = _BuildingBlockReader(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            budget=budget,
        )
        price_memberships = await _price_memberships(
            reader,
            price_keys=(candidate.price_key for candidate in candidates),
            atom_key_bits=int(atom_key_bits),
            block_span=int(price_membership_block_span),
        )
        candidate_npis = await _candidate_npis_by_ordinal(
            reader,
            candidates=candidates,
            price_memberships=price_memberships,
            core_layout_id=core_layout_id,
        )
        audit_occurrences = build_audit_occurrences(
            candidates=candidates,
            provider_npis=candidate_npis,
            price_memberships=price_memberships,
            core_layout_id=core_layout_id,
            budget=budget,
        )
        await _insert_occurrences(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            audit_occurrences=audit_occurrences,
        )
    return audit_occurrences, budget


async def _validate_publication_context(
    session: Any,
    *,
    schema_name: str,
    build_ownership: SharedLayoutBuildOwnership,
    logical_snapshot_id: str,
    candidates: Sequence[AuditCandidate],
    source_count: int,
) -> None:
    snapshot_key = int(build_ownership.snapshot_key)
    await _validate_building_layout(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_ownership.build_token,
    )
    await _validate_candidate_provider_counts(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        candidates=candidates,
    )
    await _validate_snapshot_source_dictionary(
        session,
        schema_name=schema_name,
        logical_snapshot_id=str(logical_snapshot_id),
        source_count=source_count,
        required_source_keys=(candidate.source_key for candidate in candidates),
    )


def _publication_metadata(
    *,
    audit_occurrences: Sequence[AuditOccurrence],
    candidates: Sequence[AuditCandidate],
    candidate_summary: Mapping[str, Any],
    source_count: int,
    core_layout_id: bytes,
    price_membership_block_span: int,
    budget: _ReadBudget,
) -> dict[str, Any]:
    sample_digest = _sample_digest(audit_occurrences)
    return {
        "contract": PTG2_V3_AUDIT_CONTRACT,
        "format_version": 2,
        "method": PTG2_V3_AUDIT_METHOD,
        "sample_count": len(audit_occurrences),
        "maximum_rows": PTG2_V3_AUDIT_MAX_SAMPLE_ROWS,
        "complete_population": False,
        "sample_digest": sample_digest.hex(),
        "core_layout_id": core_layout_id.hex(),
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "source_count": source_count,
        "serving_multiplicity_semantics": PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
        "candidate_count": len(candidates),
        "candidate_maximum": PTG2_V3_AUDIT_MAX_CANDIDATES,
        "candidate_selection": PTG2_V3_AUDIT_CANDIDATE_SELECTION,
        "provider_selection": "hash_targeted_owner_ordinals_v1",
        "price_membership_block_span": int(price_membership_block_span),
        "candidate_row_digest": str(candidate_summary["row_digest"]).lower(),
        "source_row_count": _integer(
            candidate_summary.get("source_row_count"),
            "audit candidate source row count",
        ),
        "caps": {
            "block_bytes": PTG2_V3_AUDIT_MAX_BLOCK_BYTES,
            "member_attempts": PTG2_V3_AUDIT_MAX_MEMBER_ATTEMPTS,
            "combination_attempts": PTG2_V3_AUDIT_MAX_COMBINATION_ATTEMPTS,
        },
        "work": {
            "block_bytes": budget.block_bytes,
            "member_attempts": budget.member_attempts,
            "combination_attempts": budget.combination_attempts,
        },
    }


async def sealed_audit_sample_metadata(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    logical_snapshot_id: str,
) -> dict[str, Any]:
    """Return the authoritative audit contract from a sealed reused layout."""

    layout_manifest = await _sealed_layout_manifest(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
    )
    metadata = _audit_metadata_from_layout(layout_manifest)
    sample_count, expected_digest = _validated_sealed_audit_contract(metadata)
    audit_occurrences = await _sealed_audit_occurrences(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
    )
    if (
        len(audit_occurrences) != sample_count
        or _sample_digest(audit_occurrences).hex() != expected_digest
    ):
        raise RuntimeError(
            "reused strict V3 layout audit rows disagree with its manifest"
        )
    await _validate_snapshot_source_dictionary(
        session,
        schema_name=schema_name,
        logical_snapshot_id=str(logical_snapshot_id),
        source_count=_integer(metadata.get("source_count"), "audit source_count"),
        required_source_keys=(
            occurrence.source_key for occurrence in audit_occurrences
        ),
    )
    return metadata


async def _sealed_layout_manifest(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> Mapping[str, Any]:
    schema = _quote_ident(schema_name)
    layout_result = await session.execute(
        db.text(
            f"""
            SELECT layout_manifest
              FROM {schema}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = :snapshot_key
               AND state = 'sealed'
               AND generation = :generation
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "generation": PTG2_V3_SHARED_GENERATION,
        },
    )
    layout_manifest = layout_result.scalar()
    if not isinstance(layout_manifest, Mapping):
        raise RuntimeError("reused strict V3 layout is missing its manifest")
    return layout_manifest


def _audit_metadata_from_layout(
    layout_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    serving_index = layout_manifest.get("serving_index")
    audit_sample = (
        serving_index.get("audit_sample")
        if isinstance(serving_index, Mapping)
        else None
    )
    if not isinstance(audit_sample, Mapping):
        raise RuntimeError(
            "reused strict V3 layout is missing its audit sample contract"
        )
    return dict(audit_sample)


def _validated_sealed_audit_contract(
    metadata: Mapping[str, Any],
) -> tuple[int, str]:
    if (
        metadata.get("contract") != PTG2_V3_AUDIT_CONTRACT
        or _integer(metadata.get("format_version"), "audit sample format version") != 2
        or metadata.get("method") != PTG2_V3_AUDIT_METHOD
        or metadata.get("serving_multiplicity_semantics")
        != PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS
    ):
        raise RuntimeError(
            "reused strict V3 layout has an incompatible audit sample contract"
        )
    sample_count = _integer(metadata.get("sample_count"), "audit sample count")
    if sample_count > PTG2_V3_AUDIT_MAX_SAMPLE_ROWS:
        raise RuntimeError("reused strict V3 layout exceeds the audit sample row cap")
    expected_digest = str(metadata.get("sample_digest") or "").strip().lower()
    if len(expected_digest) != 64:
        raise RuntimeError("reused strict V3 layout has an invalid audit sample digest")
    return sample_count, expected_digest


async def _sealed_audit_occurrences(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> tuple[AuditOccurrence, ...]:
    schema = _quote_ident(schema_name)
    occurrence_result = await session.execute(
        db.text(
            f"""
            SELECT occurrence_id, code_key, provider_set_key, price_key, source_key,
                   npi, atom_ordinal, atom_key
              FROM {schema}.ptg2_v3_audit_occurrence
             WHERE snapshot_key = :snapshot_key
             ORDER BY occurrence_id
             LIMIT :row_limit
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "row_limit": PTG2_V3_AUDIT_MAX_SAMPLE_ROWS + 1,
        },
    )
    return tuple(
        _stored_audit_occurrence(_row_mapping(raw_database_row))
        for raw_database_row in occurrence_result
    )


def _stored_audit_occurrence(database_row: Mapping[str, Any]) -> AuditOccurrence:
    return AuditOccurrence(
        occurrence_id=bytes(database_row["occurrence_id"]),
        code_key=int(database_row["code_key"]),
        provider_set_key=int(database_row["provider_set_key"]),
        price_key=int(database_row["price_key"]),
        source_key=int(database_row["source_key"]),
        npi=int(database_row["npi"]),
        atom_ordinal=int(database_row["atom_ordinal"]),
        atom_key=int(database_row["atom_key"]),
        candidate_ordinal=0,
    )


__all__ = [
    "AuditCandidate",
    "AuditOccurrence",
    "SharedAuditPublication",
    "build_audit_occurrences",
    "load_audit_candidates",
    "occurrence_id",
    "persisted_audit_sample_digest",
    "publish_shared_audit_sample",
    "sealed_audit_sample_metadata",
]
