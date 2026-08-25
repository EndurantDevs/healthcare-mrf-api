# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Setwise publication for explicit packed V4 finalizer mappings."""
from __future__ import annotations
import asyncio
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence
from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_block_build_pins import (
    PTG2_BLOCK_BUILD_PIN_TABLE,
    _lease_deadline,
    is_pin_lease_renewed,
)
from process.ptg_parts.ptg2_lifecycle_lock import (
    PTG2_SOURCE_POINTER_GC_LOCK_KEY,
    configure_ptg2_lifecycle_transaction,
)
from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_SHARED_FORMAT_VERSION
from process.ptg_parts.ptg2_shared_price import _await_cleanup_task
from process.ptg_parts.ptg2_shared_publish import _DigestingCopySource, _safe_identifier
from process.ptg_parts.ptg2_v4_finalizer_map_digest import v4_finalizer_map_root_digest
from process.ptg_parts.ptg2_v4_finalizer_map_sql import (
    _BLOCK_COLUMNS,
    _CAS_INSERT_SQL,
    _PACK_COLUMNS,
    _PACK_INSERT_SQL,
    _PACK_VALIDATE_SQL,
    _ROOT_COMPLETE_SQL,
    _ROOT_INSERT_SQL,
    _SENTINEL_PIN_SQL,
)
from process.ptg_parts.ptg2_v4_finalizer_map_sidecars import (
    PackedMapArtifact,
    PackedMapNativeReceipt,
    PackedMapSidecars,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
    PTG2_V4_FINALIZER_MAP_PACK_TABLE,
    PTG2_V4_FINALIZER_MAP_ROOT_TABLE,
    PTG2_V4_FINALIZER_MAP_TARGET_TABLE,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_DEFAULT_COORDINATES_PER_PACK,
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_MAP_FORMAT,
    lock_v4_shared_layout_for_map_write,
)
from process.ptg_parts.snapshot_tables import _ptg2_snapshot_index_name
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_KINDS_SQL = ", ".join(repr(kind) for kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)
_TARGET_PREDICATE = f"staged.object_kind IN ({_KINDS_SQL})"
_CAS_VALIDATE_SQL = f"""
SELECT COUNT(*)::bigint,
       COUNT(*) FILTER (WHERE {_TARGET_PREDICATE})::bigint,
       COUNT(*) FILTER (WHERE staged.object_kind = :map_kind)::bigint,
       COUNT(stored.block_hash)::bigint,
       COALESCE(BOOL_OR(staged.object_kind <> :map_kind AND NOT ({_TARGET_PREDICATE})), FALSE),
       COALESCE(BOOL_OR(stored.block_hash IS NULL
         OR stored.format_version <> staged.format_version OR stored.object_kind <> staged.object_kind
         OR stored.codec <> staged.codec OR stored.entry_count <> staged.entry_count
         OR stored.raw_byte_count <> staged.raw_byte_count
         OR stored.stored_byte_count <> staged.stored_byte_count), FALSE),
       COALESCE(BOOL_OR(staged.object_kind = :map_kind
         AND (staged.format_version <> :format_version OR staged.codec <> 'none'
              OR staged.raw_byte_count <> staged.stored_byte_count)), FALSE)
  FROM {{schema}}.{{stage}} AS staged
  LEFT JOIN {{schema}}.ptg2_v3_block AS stored USING (block_hash)
"""
_TARGET_INSERT_SQL = f"""
INSERT INTO {{schema}}.{{target}} (snapshot_key, block_hash)
SELECT :snapshot_key, staged.block_hash FROM {{schema}}.{{stage}} AS staged
 WHERE {_TARGET_PREDICATE} ORDER BY staged.block_hash
"""
@dataclass(frozen=True)
class V4FinalizerMapPublication:
    """Exact logical and physical totals for one packed finalizer map."""
    object_kinds: tuple[str, ...]
    mapping_count: int
    unique_block_count: int
    entry_count: int
    logical_byte_count: int
    stored_byte_count: int
    map_pack_count: int
    stored_map_byte_count: int
    map_digest: bytes
    canonical_mapping_digest: bytes
    canonical_byte_count: int
    target_identity_digest: bytes
    contract: str = PTG2_V4_FINALIZER_MAP_CONTRACT
    coordinate_count = property(lambda self: self.mapping_count)
    target_block_count = property(lambda self: self.unique_block_count)

    def manifest(self) -> dict[str, Any]:
        """Return the immutable reader-facing storage contract."""
        manifest_by_field = {
            "contract": self.contract,
            "map_format": PTG2_V4_MAP_FORMAT,
            "map_digest": self.map_digest.hex(),
            "object_kinds": list(self.object_kinds),
            "object_kind_count": len(self.object_kinds),
            "map_pack_count": self.map_pack_count,
            "coordinate_count": self.mapping_count,
            "entry_count": self.entry_count,
            "logical_byte_count": self.logical_byte_count,
            "stored_map_byte_count": self.stored_map_byte_count,
            "target_block_count": self.unique_block_count,
            "canonical_mapping_digest": self.canonical_mapping_digest.hex(),
            "canonical_byte_count": self.canonical_byte_count,
            "target_identity_digest": self.target_identity_digest.hex(),
        }
        return manifest_by_field
def _count(value: Any, label: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < int(positive):
        raise ValueError(f"packed finalizer {label} is invalid")
    return value
def _lane_values(sidecar: PackedMapSidecars, lane_no: int) -> tuple[dict[str, int], dict[str, bytes], set[Path]]:
    kinds = tuple(sidecar.object_kinds)
    if not kinds or kinds != tuple(sorted(set(kinds))):
        raise ValueError("packed finalizer lane object kinds are invalid")
    digest_by_kind = dict(sidecar.kind_digests)
    if set(digest_by_kind) != set(kinds) or any(
        len(bytes(digest_value)) != 32
        for digest_value in digest_by_kind.values()
    ):
        raise ValueError("packed finalizer lane digest set is incomplete")
    paths: set[Path] = set()
    artifacts = (
        ("target blocks", sidecar.target_blocks),
        ("map blocks", sidecar.map_blocks),
        ("map packs", sidecar.map_packs),
    )
    for name, artifact in artifacts:
        _count(artifact.row_count, f"lane {lane_no} {name} rows", positive=True)
        _count(artifact.byte_count, f"lane {lane_no} {name} bytes", positive=True)
        if not _SHA256_RE.fullmatch(str(artifact.sha256)) or not Path(artifact.path).is_file():
            raise ValueError(f"packed finalizer lane {lane_no} {name} artifact is invalid")
        paths.add(Path(artifact.path).resolve())
    packs = _count(sidecar.map_pack_count, "map pack count", positive=True)
    coordinates = _count(sidecar.coordinate_count, "coordinate count", positive=True)
    target_count = _count(
        sidecar.target_block_count, "target block count", positive=True
    )
    if (sidecar.map_blocks.row_count != packs or sidecar.map_packs.row_count != packs
            or not packs <= coordinates <= packs * PTG2_V4_DEFAULT_COORDINATES_PER_PACK
            or target_count > coordinates
    ):
        raise ValueError("packed finalizer lane aggregates are inconsistent")
    if sidecar.target_blocks.row_count != target_count:
        raise ValueError("packed finalizer native target artifact count changed")
    totals_by_field = {
        "mapping_count": coordinates, "unique_block_count": target_count,
        "entry_count": _count(sidecar.entry_count, "entry count"),
        "logical_byte_count": _count(sidecar.logical_byte_count, "logical bytes"),
        "stored_byte_count": _count(sidecar.stored_byte_count, "stored bytes"),
        "map_pack_count": packs,
        "stored_map_byte_count": _count(sidecar.stored_map_byte_count, "stored map bytes"),
    }
    return totals_by_field, {kind: bytes(digest_by_kind[kind]) for kind in kinds}, paths
def _combined_publication(
    native_receipt: PackedMapNativeReceipt,
) -> V4FinalizerMapPublication:
    sidecars = tuple(native_receipt.sidecars)
    if len(sidecars) != 2:
        raise ValueError("packed finalizer publication requires exactly two lanes")
    totals = dict.fromkeys((
        "mapping_count", "unique_block_count", "entry_count", "logical_byte_count",
        "stored_byte_count", "map_pack_count", "stored_map_byte_count",
    ), 0)
    kinds: set[str] = set()
    digest_by_kind: dict[str, bytes] = {}
    paths: set[Path] = set()
    for lane_no, sidecar in enumerate(sidecars):
        lane_totals, lane_digests, lane_paths = _lane_values(sidecar, lane_no)
        if kinds.intersection(sidecar.object_kinds) or paths.intersection(lane_paths):
            raise ValueError("packed finalizer lanes overlap or repeat artifacts")
        kinds.update(sidecar.object_kinds)
        paths.update(lane_paths)
        digest_by_kind.update(lane_digests)
        for field, total_value in lane_totals.items():
            totals[field] += total_value
    required = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    if kinds != set(required):
        raise ValueError("packed finalizer object-kind set is incomplete")
    digest = v4_finalizer_map_root_digest(digest_by_kind, required_object_kinds=required)
    canonical_digest = bytes(native_receipt.canonical_mapping_digest)
    target_digest = bytes(native_receipt.target_identity_digest)
    canonical_bytes = _count(
        native_receipt.canonical_byte_count,
        "native canonical bytes",
        positive=True,
    )
    if len(canonical_digest) != 32 or len(target_digest) != 32:
        raise ValueError("packed finalizer native receipt digest is invalid")
    return V4FinalizerMapPublication(
        object_kinds=required,
        map_digest=digest,
        canonical_mapping_digest=canonical_digest,
        canonical_byte_count=canonical_bytes,
        target_identity_digest=target_digest,
        **totals,
    )
def _pack_stage_name(stage_table: str) -> str:
    return _ptg2_snapshot_index_name(stage_table, "finalizer_map_pack_stage")
def _names(schema_name: str, stage_table: str, pack_stage_table: str) -> dict[str, str]:
    return {
        "schema": _quote_ident(schema_name), "stage": _quote_ident(stage_table),
        "pack_stage": _quote_ident(pack_stage_table),
        "root": _quote_ident(PTG2_V4_FINALIZER_MAP_ROOT_TABLE),
        "pack": _quote_ident(PTG2_V4_FINALIZER_MAP_PACK_TABLE),
        "target": _quote_ident(PTG2_V4_FINALIZER_MAP_TARGET_TABLE),
    }
async def _copy_artifact(artifact: PackedMapArtifact, *, schema_name: str,
                         stage_table: str, columns: Sequence[str]) -> None:
    path = Path(artifact.path)
    if path.stat().st_size != artifact.byte_count:
        raise RuntimeError("packed finalizer artifact byte count changed")
    async with db.acquire() as connection:
        raw = connection.raw_connection
        driver = getattr(raw, "driver_connection", raw)
        copy_to_table = getattr(driver, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("active database driver does not expose binary COPY")
        with path.open("rb") as source:
            measured = _DigestingCopySource(source)
            await copy_to_table(
                _safe_identifier(stage_table), source=measured,
                schema_name=_safe_identifier(schema_name), columns=list(columns), format="binary",
            )
    if (measured.byte_count != artifact.byte_count or measured.hexdigest() != artifact.sha256
            or path.stat().st_size != artifact.byte_count):
        raise RuntimeError("packed finalizer artifact changed during publication")
async def _create_pack_stage(names: dict[str, str], stage_table: str, pack_stage_table: str) -> None:
    target_index = _quote_ident(_ptg2_snapshot_index_name(stage_table, "packed_block_hash_key"))
    await db.status(
        f"CREATE UNIQUE INDEX {target_index} ON {names['schema']}.{names['stage']} "
        "(block_hash) WITH (fillfactor = 100);"
    )
    await db.status(
        f"DROP TABLE IF EXISTS {names['schema']}.{names['pack_stage']}"
    )
    await db.status(f"""
        CREATE UNLOGGED TABLE {names['schema']}.{names['pack_stage']} (
          object_kind varchar(64) NOT NULL, pack_no integer NOT NULL CHECK (pack_no >= 0),
          first_block_key bigint NOT NULL CHECK (first_block_key >= 0),
          first_fragment_no integer NOT NULL CHECK (first_fragment_no >= 0),
          last_block_key bigint NOT NULL CHECK (last_block_key >= 0),
          last_fragment_no integer NOT NULL CHECK (last_fragment_no >= 0),
          coordinate_count integer NOT NULL CHECK (coordinate_count BETWEEN 1 AND 256),
          entry_count bigint NOT NULL CHECK (entry_count >= 0),
          logical_byte_count bigint NOT NULL CHECK (logical_byte_count >= 0),
          map_block_hash bytea NOT NULL CHECK (octet_length(map_block_hash) = 32));
    """)
    names["pack_key"] = _quote_ident(_ptg2_snapshot_index_name(pack_stage_table, "kind_pack_key"))
    names["pack_start"] = _quote_ident(_ptg2_snapshot_index_name(pack_stage_table, "coordinate_start_key"))
async def _stage_sidecars(sidecars: Sequence[PackedMapSidecars], *, schema_name: str,
                          stage_table: str, pack_stage_table: str) -> None:
    names = _names(schema_name, stage_table, pack_stage_table)
    await _create_pack_stage(names, stage_table, pack_stage_table)
    for sidecar in sidecars:
        await _copy_artifact(
            sidecar.target_blocks,
            schema_name=schema_name,
            stage_table=stage_table,
            columns=_BLOCK_COLUMNS,
        )
        await _copy_artifact(
            sidecar.map_blocks, schema_name=schema_name, stage_table=stage_table, columns=_BLOCK_COLUMNS,
        )
        await _copy_artifact(
            sidecar.map_packs, schema_name=schema_name, stage_table=pack_stage_table, columns=_PACK_COLUMNS,
        )
    await db.status(
        f"CREATE UNIQUE INDEX {names['pack_key']} ON {names['schema']}.{names['pack_stage']} "
        "(object_kind, pack_no) WITH (fillfactor = 100);"
    )
    await db.status(
        f"CREATE UNIQUE INDEX {names['pack_start']} ON {names['schema']}.{names['pack_stage']} "
        "(object_kind, first_block_key, first_fragment_no) WITH (fillfactor = 100);"
    )
    await db.status(f"ANALYZE {names['schema']}.{names['stage']}")
    await db.status(f"ANALYZE {names['schema']}.{names['pack_stage']}")
def _validate_cas(publication: V4FinalizerMapPublication, values: Sequence[Any]) -> None:
    expected_total = publication.unique_block_count + publication.map_pack_count
    expected = (expected_total, publication.unique_block_count, publication.map_pack_count, expected_total)
    if tuple(map(int, values[:4])) != expected or any(map(bool, values[4:])):
        raise RuntimeError("packed finalizer CAS stage validation failed")
def _validate_packs(publication: V4FinalizerMapPublication, values: Sequence[Any]) -> None:
    expected = (
        publication.map_pack_count, len(publication.object_kinds), publication.map_pack_count,
        publication.mapping_count, publication.entry_count, publication.logical_byte_count,
        publication.stored_map_byte_count, publication.map_pack_count,
    )
    if (tuple(map(int, values[:8])) != expected
            or tuple(values[8] or ()) != tuple(sorted(publication.object_kinds))):
        raise RuntimeError("packed finalizer map-pack validation failed")
def _publication_parameters(publication: V4FinalizerMapPublication, *, snapshot_key: int,
                            build_token: str, stage_table: str) -> dict[str, Any]:
    return {
        "snapshot_key": snapshot_key, "build_token": build_token, "pin_token": stage_table,
        "lease_until": _lease_deadline(),
        "map_kind": PTG2_V4_MAP_BLOCK_KIND,
        "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "contract": publication.contract, "map_format": PTG2_V4_MAP_FORMAT,
        "map_digest": publication.map_digest,
        "canonical_mapping_digest": publication.canonical_mapping_digest,
        "canonical_byte_count": publication.canonical_byte_count,
        "target_identity_digest": publication.target_identity_digest,
        "object_kind_count": len(publication.object_kinds),
        "map_pack_count": publication.map_pack_count, "coordinate_count": publication.mapping_count,
        "entry_count": publication.entry_count, "logical_byte_count": publication.logical_byte_count,
        "stored_map_byte_count": publication.stored_map_byte_count,
        "target_block_count": publication.unique_block_count,
    }
async def _attach_rows(session: Any, publication: V4FinalizerMapPublication,
                       names: dict[str, str], parameters: dict[str, Any]) -> None:
    await session.execute(db.text(_ROOT_INSERT_SQL.format(**names)), parameters)
    await session.execute(db.text(_PACK_INSERT_SQL.format(**names)), parameters)
    await session.execute(db.text(_TARGET_INSERT_SQL.format(**names)), parameters)
    counts = await session.execute(db.text(f"""
        SELECT (SELECT COUNT(*) FROM {names['schema']}.{names['pack']}
                 WHERE snapshot_key = :snapshot_key)::bigint,
               (SELECT COUNT(*) FROM {names['schema']}.{names['target']}
                 WHERE snapshot_key = :snapshot_key)::bigint
    """), parameters)
    if tuple(map(int, counts.one())) != (publication.map_pack_count, publication.unique_block_count):
        raise RuntimeError("packed finalizer attach counts changed")
    await session.execute(db.text(f"""
        DELETE FROM {names['schema']}.ptg2_v3_gc_candidate AS candidate
         USING {names['schema']}.{names['stage']} AS staged
         WHERE candidate.block_hash = staged.block_hash
    """))
    candidates = await session.execute(db.text(f"""
        SELECT COUNT(*)::bigint FROM {names['schema']}.ptg2_v3_gc_candidate AS candidate
        JOIN {names['schema']}.{names['stage']} AS staged USING (block_hash)
    """))
    if int(candidates.one()[0]):
        raise RuntimeError("packed finalizer GC candidates remain attached")
async def _require_pin_lease(session: Any, schema_name: str,
                             parameters: dict[str, Any]) -> None:
    renewed = await is_pin_lease_renewed(
        session, schema_name=schema_name, snapshot_key=parameters["snapshot_key"],
        build_token=parameters["build_token"], pin_token=parameters["pin_token"],
    )
    if not renewed:
        raise RuntimeError("packed finalizer pin heartbeat lost ownership")
async def _seal_and_unpin(session: Any, names: dict[str, str], parameters: dict[str, Any],
                          schema_name: str) -> None:
    completed = await session.execute(db.text(_ROOT_COMPLETE_SQL.format(**names)), parameters)
    if int(completed.one()[0]) != int(parameters["snapshot_key"]):
        raise RuntimeError("packed finalizer root completion changed")
    await _require_pin_lease(session, schema_name, parameters)
    await session.execute(db.text(
        "SELECT set_config('lock_timeout', '0', true), set_config('statement_timeout', '0', true)"
    ))
    deleted = await session.execute(db.text(f"""
        WITH deleted AS (
          DELETE FROM {names['schema']}.{PTG2_BLOCK_BUILD_PIN_TABLE}
           WHERE snapshot_key = :snapshot_key AND build_token = :build_token AND pin_token = :pin_token
          RETURNING 1
        ) SELECT COUNT(*)::bigint FROM deleted
    """), parameters)
    if int(deleted.one()[0]) != 1:
        raise RuntimeError("packed finalizer build-pin sentinel changed before attach")
async def _publish_atomic_map(publication: V4FinalizerMapPublication, *, schema_name: str,
                              stage_table: str, pack_stage_table: str,
                              snapshot_key: int, build_token: str,
                              progress_callback: Callable[[str, int], None] | None) -> None:
    """Publish CAS and map state atomically under ownership and GC fences."""

    names = _names(schema_name, stage_table, pack_stage_table)
    parameters = _publication_parameters(
        publication, snapshot_key=snapshot_key, build_token=build_token, stage_table=stage_table,
    )
    async with db.transaction() as session:
        await configure_ptg2_lifecycle_transaction(
            session, lock_timeout="500ms", statement_timeout="0"
        )
        await session.execute(
            db.text("SELECT pg_advisory_xact_lock_shared(hashtext(:lock_key))"),
            {"lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY},
        )
        await lock_v4_shared_layout_for_map_write(
            session, schema_name=schema_name, snapshot_key=snapshot_key, build_token=build_token,
        )
        await session.execute(db.text(
            f"LOCK TABLE {names['schema']}.{names['stage']}, "
            f"{names['schema']}.{names['pack_stage']} IN SHARE MODE"
        ))
        await session.execute(db.text(
            "SELECT set_config('lock_timeout', '0', true), "
            "set_config('statement_timeout', '0', true)"
        ))
        pinned = await session.execute(
            db.text(_SENTINEL_PIN_SQL.format(**names)), parameters
        )
        if int(pinned.one()[0]) != 1:
            raise RuntimeError("packed finalizer build-pin sentinel changed")
        await session.execute(db.text(f"""
            DELETE FROM {names['schema']}.ptg2_v3_gc_candidate AS candidate
             USING {names['schema']}.{names['stage']} AS staged
             WHERE candidate.block_hash = staged.block_hash
        """))
        _report(progress_callback, "finalizer_pins_prepared")
        await session.execute(db.text(_CAS_INSERT_SQL.format(**names)))
        cas = await session.execute(db.text(_CAS_VALIDATE_SQL.format(**names)), parameters)
        _validate_cas(publication, cas.one())
        packs = await session.execute(db.text(_PACK_VALIDATE_SQL.format(**names)), parameters)
        _validate_packs(publication, packs.one())
        await _require_pin_lease(session, schema_name, parameters)
        await session.execute(db.text(
            "SELECT set_config('lock_timeout', '0', true), "
            "set_config('statement_timeout', '0', true)"
        ))
        _report(
            progress_callback,
            "finalizer_cas_published",
            publication.unique_block_count + publication.map_pack_count,
        )
        await _attach_rows(session, publication, names, parameters)
        _report(progress_callback, "finalizer_map_rows_attached", publication.mapping_count)
        await _seal_and_unpin(session, names, parameters, schema_name)
        _report(progress_callback, "finalizer_map_attached", publication.mapping_count)
def _report(callback: Callable[[str, int], None] | None, metric: str, amount: int = 1) -> None:
    if callback is not None:
        callback(metric, amount)
async def publish_v4_finalizer_maps(native_receipt: PackedMapNativeReceipt, *, schema_name: str,
                                    stage_table: str, snapshot_key: int, build_token: str,
                                    progress_callback: Callable[[str, int], None] | None = None,
                                    ) -> V4FinalizerMapPublication:
    """Publish two authenticated finalizer lanes without row materialization."""
    lanes = tuple(native_receipt.sidecars)
    publication = _combined_publication(native_receipt)
    schema_name = _safe_identifier(schema_name)
    stage_table = _safe_identifier(stage_table)
    build_token = str(build_token or "").strip()
    if not build_token or len(build_token) > 96 or len(stage_table) > 63:
        raise ValueError("packed finalizer build or pin token is invalid")
    pack_stage = _pack_stage_name(stage_table)
    try:
        await _stage_sidecars(
            lanes, schema_name=schema_name, stage_table=stage_table, pack_stage_table=pack_stage,
        )
        _report(progress_callback, "finalizer_sidecars_staged")
        await _publish_atomic_map(
            publication, schema_name=schema_name, stage_table=stage_table,
            pack_stage_table=pack_stage, snapshot_key=int(snapshot_key),
            build_token=build_token, progress_callback=progress_callback,
        )
        return publication
    finally:
        cleanup_task = asyncio.create_task(
            db.status(
                f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}."
                f"{_quote_ident(pack_stage)};"
            )
        )
        await _await_cleanup_task(cleanup_task, propagate_cancellation=True)
__all__ = ("V4FinalizerMapPublication", "publish_v4_finalizer_maps")
