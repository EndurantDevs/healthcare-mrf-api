# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain native generation authority beside the exact predecessor family."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, is_dataclass
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

from sqlalchemy import text

from process import reference_family_archive as native
from process.entity_address_snapshot_receipt import _projected_row_identity

CONTRACT = "scoped-catalog-retained.v1"
GENERATION_TABLES = {"code-sets": "code_sets_result_generation", "ms-drg": "ms_drg_result_generation"}
_COMMON_COLUMNS = (
    "id",
    "local_lineage_id",
    "local_generation",
    "origin_lineage_id",
    "origin_generation",
    "published_at",
)
_TAIL_COLUMNS = {
    "code-sets": ("code_catalog_oid", "row_count", "row_sha256"),
    "ms-drg": ("include_relationships", "receipt"),
}
_RECEIPT_FIELDS = frozenset(
    "contract importer dataset_id schema_name schema_oid owner_oid database_oid live_schema generation_table "
    "generation_oid previous_generations tables published_oids current_generation publication_handoff_sha256 receipt_sha256".split()
)
_HEX = re.compile(r"[0-9a-f]{64}\Z")


def canonical_metadata(value):
    """Encode bounded control metadata without lossy UUID or timezone conversions."""
    return json.dumps(value, default=_metadata_scalar, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _metadata_scalar(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.isoformat()
    raise TypeError("unsupported catalog metadata scalar")


def generation_value(value):
    """Normalize native generation values for exact durable JSON comparison."""
    return json.loads(canonical_metadata(asdict(value) if is_dataclass(value) else value))


async def _family_receipts(session, prepared, schema):
    receipts = []
    for model in prepared.spec.model_types:
        name = model.__tablename__
        count, digest = await _projected_row_identity(
            session, schema, name, row_json_sql="pg_catalog.to_jsonb(row_value)"
        )
        oid = await native._relation_oid(session, schema, name)
        receipts.append(
            {
                "table": name,
                "relation_oid": oid,
                "row_count": count,
                "row_sha256": digest,
                "schema_sha256": await native._family_schema_identity(
                    session, prepared.spec.importer_id, oid, schema, name
                ),
            }
        )
    return receipts


async def retain_catalog_authority(session, prepared, schema):
    """Copy the old singleton, including generation zero, before advancing the live row."""
    if schema != native.reference_family_predecessor_schema(prepared.ownership.dataset_id):
        raise native.ReferenceFamilyArchiveError("catalog predecessor namespace differs")
    table = GENERATION_TABLES[prepared.importer]
    previous = await copy_generation_authority(session, prepared.importer, prepared.incumbent.schema_name, schema)
    if canonical_metadata(previous) != canonical_metadata(generation_value(prepared.generations[prepared.importer])):
        raise native.ReferenceFamilyArchiveError("retained catalog generation differs")
    receipt_by_field = {
        "contract": CONTRACT,
        "importer": prepared.importer,
        "dataset_id": str(prepared.ownership.dataset_id),
        "schema_name": schema,
        "schema_oid": await native._schema_oid(session, schema),
        "owner_oid": prepared.owner_oid,
        "database_oid": await session.scalar(
            text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")
        ),
        "live_schema": prepared.incumbent.schema_name,
        "generation_table": table,
        "generation_oid": await native._relation_oid(session, schema, table),
        "previous_generations": {
            name: generation_value(generation) for name, generation in prepared.generations.items()
        },
        "tables": await _family_receipts(session, prepared, schema),
        "published_oids": [list(pair) for pair in prepared.ownership.relation_oids],
        "publication_handoff_sha256": None,
    }
    return receipt_by_field


async def copy_generation_authority(session, importer, source_schema, target_schema, *, read_only=False):
    """Keep the actual native singleton beside prepared or retained heap inventory."""
    from process.scoped_catalog_binding import _lock_generation

    for schema in (source_schema, target_schema):
        native._schema_name(schema)
    table = GENERATION_TABLES[importer]
    await _lock_generation(session, source_schema, table, required=True, read_only=read_only)
    qualified = f"{native._quoted(target_schema)}.{native._quoted(table)}"
    live = f"{native._quoted(source_schema)}.{native._quoted(table)}"
    await session.execute(text(f"CREATE TABLE {qualified} (LIKE {live} INCLUDING CONSTRAINTS INCLUDING INDEXES)"))
    columns = _COMMON_COLUMNS + _TAIL_COLUMNS[importer]
    await native.native_copy_projection(
        session,
        "SELECT " + ",".join(native._quoted(name) for name in columns) + f" FROM {live}",
        schema_name=target_schema,
        table_name=table,
        columns=columns,
        max_bytes=1024**2,
        timeout=30,
    )
    previous = await _read_retained_generation(session, importer, target_schema)
    await session.execute(text(f"ALTER TABLE {qualified} ADD COLUMN retained_family jsonb"))
    return generation_value(previous)


async def _read_retained_generation(session, importer, schema):
    columns = _COMMON_COLUMNS[1:] + _TAIL_COLUMNS[importer]
    qualified = f"{native._quoted(schema)}.{native._quoted(GENERATION_TABLES[importer])}"
    result = await session.execute(
        text("SELECT " + ",".join(native._quoted(name) for name in columns) + f" FROM {qualified} WHERE id=1")
    )
    return generation_value(dict(result.mappings().one()))


async def finish_catalog_authority(session, prepared, receipt, current):
    """Durably bind the retained inventory and new generation in the swap transaction."""
    from process.scoped_catalog_publication import _seal_candidate

    receipt["current_generation"] = generation_value(current)
    receipt["receipt_sha256"] = hashlib.sha256(canonical_metadata(receipt).encode()).hexdigest()
    encoded = canonical_metadata(receipt)
    if len(encoded.encode()) > 65536:
        raise native.ReferenceFamilyArchiveError("retained catalog authority exceeds its bound")
    schema, table = receipt["schema_name"], receipt["generation_table"]
    qualified = f"{native._quoted(schema)}.{native._quoted(table)}"
    changed = await session.scalar(
        text(
            f"UPDATE {qualified} SET retained_family=CAST(:receipt AS jsonb) WHERE id=1 AND retained_family IS NULL RETURNING id"
        ),
        {"receipt": encoded},
    )
    if changed != 1:
        raise native.ReferenceFamilyArchiveError("retained catalog authority changed")
    await session.execute(text(f"ALTER TABLE {qualified} ALTER COLUMN retained_family SET NOT NULL"))
    ownership = SimpleNamespace(
        schema_name=schema,
        schema_oid=receipt["schema_oid"],
        sequence_oids=(),
        relation_oids=tuple((entry["table"], entry["relation_oid"]) for entry in receipt["tables"])
        + ((table, receipt["generation_oid"]),),
    )
    await _seal_candidate(session, ownership, prepared.owner_oid)
    await require_retained_catalog(session, receipt)


def validate_retained_catalog(receipt, *, importer=None, live_schema=None):
    """Validate serialized identity only; native custody must be authenticated separately."""
    if type(receipt) is not dict or set(receipt) != _RECEIPT_FIELDS or receipt.get("contract") != CONTRACT:
        raise native.ReferenceFamilyArchiveError("retained catalog contract differs")
    if receipt["importer"] not in GENERATION_TABLES or importer not in (None, receipt["importer"]):
        raise native.ReferenceFamilyArchiveError("retained catalog importer differs")
    native._schema_name(receipt["live_schema"])
    if live_schema not in (None, receipt["live_schema"]):
        raise native.ReferenceFamilyArchiveError("retained catalog destination differs")
    expected_by_field = dict(receipt)
    digest = expected_by_field.pop("receipt_sha256", None)
    if (
        len(canonical_metadata(receipt).encode()) > 65536
        or digest != hashlib.sha256(canonical_metadata(expected_by_field).encode()).hexdigest()
    ):
        raise native.ReferenceFamilyArchiveError("retained catalog digest differs")
    if (
        receipt["schema_name"] != native.reference_family_predecessor_schema(UUID(receipt["dataset_id"]))
        or receipt["generation_table"] != GENERATION_TABLES[receipt["importer"]]
        or any(
            type(receipt[name]) is not int or not 0 < receipt[name] < 2**32
            for name in ("schema_oid", "owner_oid", "database_oid", "generation_oid")
        )
        or type(receipt["previous_generations"]) is not dict
        or not {receipt["importer"]} <= set(receipt["previous_generations"]) <= set(GENERATION_TABLES)
        or type(receipt["current_generation"]) is not dict
        or receipt["publication_handoff_sha256"] is not None
        and (
            not isinstance(receipt["publication_handoff_sha256"], str)
            or _HEX.fullmatch(receipt["publication_handoff_sha256"]) is None
        )
    ):
        raise native.ReferenceFamilyArchiveError("retained catalog identity differs")
    _validate_retained_tables(receipt)
    return receipt


def _validate_retained_tables(receipt):
    from process.scoped_catalog_publication import MODELS

    if type(receipt["tables"]) is not list or any(type(entry) is not dict for entry in receipt["tables"]):
        raise native.ReferenceFamilyArchiveError("retained catalog tables differ")
    table_names = tuple(entry["table"] for entry in receipt["tables"])
    if table_names not in (("code_catalog",), tuple(model.__tablename__ for model in MODELS)) or (
        receipt["importer"] == "ms-drg" and len(table_names) != 3
    ):
        raise native.ReferenceFamilyArchiveError("retained catalog family differs")
    for entry in receipt["tables"]:
        if (
            set(entry) != {"table", "relation_oid", "row_count", "row_sha256", "schema_sha256"}
            or type(entry["relation_oid"]) is not int
            or not 0 < entry["relation_oid"] < 2**32
            or type(entry["row_count"]) is not int
            or entry["row_count"] < 0
            or any(
                not isinstance(entry[key], str) or not _HEX.fullmatch(entry[key])
                for key in ("row_sha256", "schema_sha256")
            )
        ):
            raise native.ReferenceFamilyArchiveError("retained catalog table receipt differs")
    published = receipt["published_oids"]
    if (
        type(published) is not list
        or any(type(pair) is not list or len(pair) != 2 for pair in published)
        or tuple(pair[0] for pair in published) != tuple(sorted(table_names))
        or any(type(pair[1]) is not int or not 0 < pair[1] < 2**32 for pair in published)
        or len({pair[1] for pair in published}) != len(published)
        or len({entry["relation_oid"] for entry in receipt["tables"]} | {receipt["generation_oid"]})
        != len(table_names) + 1
    ):
        raise native.ReferenceFamilyArchiveError("retained catalog physical vector differs")


async def require_retained_catalog(session, receipt):
    """Recheck protected storage, complete inventory and all content before any rollback read."""
    from process.scoped_catalog_binding import require_closed_catalog_binding
    from process.scoped_catalog_publication import MODELS

    native._require_transaction(session)
    validate_retained_catalog(receipt)
    schema, table = receipt["schema_name"], receipt["generation_table"]
    models = MODELS if len(receipt["tables"]) == 3 else MODELS[:1]
    pairs = tuple((entry["table"], entry["relation_oid"]) for entry in receipt["tables"]) + (
        (table, receipt["generation_oid"]),
    )
    await native._lock_family(session, schema, tuple(sorted(name for name, _ in pairs)), "SHARE", nowait=True)
    await _require_retained_inventory(session, receipt, pairs)
    if await require_closed_catalog_binding(session, schema, pairs) != receipt["owner_oid"]:
        raise native.ReferenceFamilyArchiveError("retained catalog owner differs")
    await native.require_native_read_catalog(session, tuple(oid for _, oid in pairs))
    persisted = await session.scalar(
        text(f"SELECT retained_family FROM {native._quoted(schema)}.{native._quoted(table)} WHERE id=1")
    )
    if canonical_metadata(persisted) != canonical_metadata(receipt):
        raise native.ReferenceFamilyArchiveError("retained catalog persisted authority differs")
    if canonical_metadata(await _read_retained_generation(session, receipt["importer"], schema)) != canonical_metadata(
        receipt["previous_generations"][receipt["importer"]]
    ):
        raise native.ReferenceFamilyArchiveError("retained catalog generation changed")
    prepared = SimpleNamespace(spec=native.ReferenceFamilySpec("scoped-catalog", models))
    if await _family_receipts(session, prepared, schema) != receipt["tables"]:
        raise native.ReferenceFamilyArchiveError("retained catalog rows or schema changed")
    return receipt


async def _require_retained_inventory(session, receipt, pairs):
    schema_oid = await native._schema_oid(session, receipt["schema_name"])
    if (
        schema_oid != receipt["schema_oid"]
        or await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"))
        != receipt["database_oid"]
    ):
        raise native.ReferenceFamilyArchiveError("retained catalog location differs")
    for name, oid in pairs:
        if await native._relation_oid(session, receipt["schema_name"], name) != oid:
            raise native.ReferenceFamilyArchiveError("retained catalog heap changed")
    owned_oids = {oid for _, oid in pairs}
    for relation in await native._namespace_relations(session, schema_oid):
        if relation["relkind"] == "r" and relation["oid"] in owned_oids:
            continue
        if relation["relkind"] == "i" and relation["index_table_oid"] in owned_oids:
            continue
        raise native.ReferenceFamilyArchiveError("retained catalog inventory changed")


async def cleanup_retained_catalog(session, receipt):
    """Drop only authenticated non-serving predecessor heaps; pinned readers prevent removal."""
    await require_retained_catalog(session, receipt)
    schema = receipt["schema_name"]
    names = tuple(entry["table"] for entry in receipt["tables"]) + (receipt["generation_table"],)
    owned = {entry["relation_oid"] for entry in receipt["tables"]} | {receipt["generation_oid"]}
    for name in names:
        if await native._relation_oid(session, receipt["live_schema"], name) in owned:
            raise native.ReferenceFamilyArchiveError("retained catalog is still serving")
    await native._lock_family(session, schema, tuple(sorted(names)), "ACCESS EXCLUSIVE", nowait=True)
    await require_retained_catalog(session, receipt)
    for name in names:
        await session.execute(text(f"DROP TABLE {native._quoted(schema)}.{native._quoted(name)} RESTRICT"))
    await session.execute(text(f"DROP SCHEMA {native._quoted(schema)} RESTRICT"))
