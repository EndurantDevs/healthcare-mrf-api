# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable content and logical generations for retained shared-catalog families."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

from sqlalchemy import text

from process import reference_family_archive as native
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_storage as storage
from process.scoped_catalog_retention import canonical_metadata

CONTRACT = "scoped-catalog-retained.v2"


def logical_receipt(receipt):
    """Decode existing logical revision and content fields without trusting physical metadata."""
    if type(receipt) is not dict or receipt.get("contract") not in {CONTRACT, "scoped-catalog-retained.v1"}:
        raise RuntimeError("retained catalog contract differs")
    if len(canonical_metadata(receipt).encode()) > 65536:
        raise RuntimeError("retained catalog record exceeds its bound")
    identifier = UUID(receipt["dataset_id"])
    if not identifier.int or receipt["schema_name"] != native.reference_family_predecessor_schema(identifier):
        raise RuntimeError("retained catalog name differs")
    native._schema_name(receipt["live_schema"])
    if receipt["live_schema"] == receipt["schema_name"] or receipt["importer"] not in generations.TABLES:
        raise RuntimeError("retained catalog scope differs")
    models = storage.MODELS if len(receipt["tables"]) == 3 else storage.MODELS[:1]
    if [entry["table"] for entry in receipt["tables"]] != [model.__tablename__ for model in models]:
        raise RuntimeError("retained catalog table inventory differs")
    vector_by_field = {
        name: generations.logical_generation(field_value)
        for name, field_value in receipt["previous_generations"].items()
    }
    if not set(vector_by_field) <= generations.TABLES.keys() or receipt["importer"] not in vector_by_field:
        raise RuntimeError("retained catalog source inventory differs")
    if receipt["generation_table"] != generations.TABLES[receipt["importer"]]:
        raise RuntimeError("retained catalog generation table differs")
    result_by_field = {
        "importer": receipt["importer"],
        "dataset_id": str(identifier),
        "schema_name": receipt["schema_name"],
        "live_schema": receipt["live_schema"],
        "generation_table": generations.TABLES[receipt["importer"]],
        "tables": [
            {key: entry[key] for key in ("table", "row_count", "row_sha256", "schema_sha256")}
            for entry in receipt["tables"]
        ],
        "previous_generations": vector_by_field,
    }
    from process.scoped_catalog_named_publication import validate_tables

    validate_tables(result_by_field["tables"], tuple(model.__tablename__ for model in models))
    handoff_digest = receipt["publication_handoff_sha256"]
    if handoff_digest is not None and (
        type(handoff_digest) is not str or len(handoff_digest) != 64 or set(handoff_digest) - set("0123456789abcdef")
    ):
        raise RuntimeError("retained catalog publication digest differs")
    if receipt["contract"] == CONTRACT:
        published_by_field = {
            name: generations.logical_generation(field_value)
            for name, field_value in receipt["published_generations"].items()
        }
        if set(published_by_field) != set(vector_by_field):
            raise RuntimeError("retained catalog published source inventory differs")
    else:
        published_by_field = {
            **vector_by_field,
            receipt["importer"]: generations.logical_generation(receipt["current_generation"]),
        }
    result_by_field["published_generations"] = published_by_field
    result_by_field["publication_handoff_sha256"] = receipt["publication_handoff_sha256"]
    return result_by_field


def retained_generation_names(receipt):
    # The old writer retained only its own singleton; companion revisions remain in the logical record.
    """Return exactly the singleton tables retained by the record version."""
    return tuple(receipt["previous_generations"]) if receipt["contract"] == CONTRACT else (receipt["importer"],)


async def copy_generations(session, live_schema, retained_schema, vector):
    """Use existing native COPY for the bounded singleton controls beside retained data."""
    for importer in vector:
        table = generations.TABLES[importer]
        source, target = storage.qualified(live_schema, table), storage.qualified(retained_schema, table)
        await session.execute(text(f"CREATE TABLE {target} (LIKE {source} INCLUDING CONSTRAINTS INCLUDING INDEXES)"))
        columns = list(
            (
                await session.execute(
                    text(
                        "SELECT a.attname FROM pg_attribute a WHERE a.attrelid=to_regclass(:name) "
                        "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
                    ),
                    {"name": source},
                )
            ).scalars()
        )
        projection = "SELECT " + ",".join(native._quoted(name) for name in columns) + " FROM " + source
        await native.native_copy_projection(
            session,
            projection,
            schema_name=retained_schema,
            table_name=table,
            columns=tuple(columns),
            max_bytes=1024**2,
            timeout=30,
        )


async def record_retained(session, receipt, settings):
    """Persist one bounded rollback record and apply native read-only access."""
    logical_receipt(receipt)
    schema = receipt["schema_name"]
    table = generations.TABLES[receipt["importer"]]
    target = storage.qualified(schema, table)
    await session.execute(text(f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS retained_family jsonb"))
    changed = await session.scalar(
        text(
            f"UPDATE {target} SET retained_family=CAST(:receipt AS jsonb) WHERE id=1 AND retained_family IS NULL RETURNING id"
        ),
        {"receipt": canonical_metadata(receipt)},
    )
    if changed != 1:
        raise RuntimeError("retained catalog record already exists")
    data_models = storage.MODELS if len(receipt["tables"]) == 3 else storage.MODELS[:1]
    controls = tuple(
        SimpleNamespace(__tablename__=generations.TABLES[name]) for name in receipt["previous_generations"]
    )
    await storage.protect_tables(session, schema, (*data_models, *controls), settings, candidate=True)
    await require_retained(session, receipt)


async def require_retained(session, receipt):
    """Resolve retained names and verify content without consulting recorded physical identity."""
    logical = logical_receipt(receipt)
    schema = logical["schema_name"]
    models = storage.MODELS if len(logical["tables"]) == 3 else storage.MODELS[:1]
    persisted = await session.scalar(
        text(f"SELECT retained_family FROM {storage.qualified(schema, logical['generation_table'])} WHERE id=1")
    )
    if logical_receipt(persisted) != logical:
        raise RuntimeError("retained catalog record differs")
    if await generations.read_generations(session, schema, importer=logical["importer"]) != {
        name: logical["previous_generations"][name] for name in retained_generation_names(receipt)
    }:
        raise RuntimeError("retained catalog logical generation differs")
    if await storage.table_receipts(session, schema, models) != logical["tables"]:
        raise RuntimeError("retained catalog source data or structure differs")
    await generations.validate_published_content(
        session,
        schema,
        {name: logical["previous_generations"][name] for name in retained_generation_names(receipt)},
    )
    return logical


async def cleanup_retained(session, receipt):
    """Recheck the retained family under a lock before exact-name restrictive drops."""
    logical = await require_retained(session, receipt)
    schema = logical["schema_name"]
    names = [entry["table"] for entry in logical["tables"]]
    names += [generations.TABLES[name] for name in retained_generation_names(receipt)]
    await native._lock_family(session, schema, tuple(sorted(names)), "ACCESS EXCLUSIVE", nowait=True)
    await require_retained(session, receipt)
    for name in names:
        await session.execute(text(f"DROP TABLE {storage.qualified(schema, name)} RESTRICT"))
    await session.execute(text(f"DROP SCHEMA {native._quoted(schema)} RESTRICT"))
