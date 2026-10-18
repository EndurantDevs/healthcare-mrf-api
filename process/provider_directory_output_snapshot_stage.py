# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded COPY and finished native indexes for the fixed output candidates."""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_candidate import (
    COPY_FRAME_BYTES,
    COPY_MAX_BYTES,
    COPY_MAX_ROWS,
    copy_record_bytes,
)
from process.provider_directory_generic_source_storage import _close_grants
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    decoded,
    encoded,
    output_models,
    require_output,
    require_output_shape,
)
from process.provider_directory_output_snapshot_custody import (
    NativeOutputRelation,
    _capture,
    _digest,
    _require_capability,
)

OUTPUT_COPY_MAX_BYTES = COPY_MAX_BYTES
OUTPUT_COPY_MAX_ROWS = COPY_MAX_ROWS


def _record_bytes(record):
    """Reuse native SOURCE framing and charge the fixed output's extra scalar types."""
    basic_values = tuple(value if value is None or type(value) in (str, int, bool, bytes) else None for value in record)
    size = copy_record_bytes(basic_values)
    for value, charged in zip(record, basic_values, strict=True):
        if value is None or charged is not None:
            continue
        if type(value) in (float, datetime):
            size += 8
        elif isinstance(value, Decimal):
            parts = value.as_tuple()
            size += 8 + 2 * ((len(parts.digits) + abs(parts.exponent) + 3) // 4) if value.is_finite() else 8
        else:
            require_output(False, "copy_value_invalid")
    require_output(COPY_FRAME_BYTES + size <= OUTPUT_COPY_MAX_BYTES, "copy_record_too_large")
    return size


def _record_batches(records):
    batch, size = [], COPY_FRAME_BYTES
    for record in records:
        record_size = _record_bytes(record)
        if batch and (len(batch) == OUTPUT_COPY_MAX_ROWS or size + record_size > OUTPUT_COPY_MAX_BYTES):
            yield batch
            batch, size = [], COPY_FRAME_BYTES
        batch.append(record)
        size += record_size
    if batch:
        yield batch


@dataclass(frozen=True)
class OutputStage:
    schema: str
    target: str
    relation: str
    oid: int
    operation_digest: str
    incumbent: NativeOutputRelation

    def to_payload(self):
        """Serialize the exact private heap and original incumbent identity."""
        return {
            "schema": self.schema,
            "target": self.target,
            "relation": self.relation,
            "oid": self.oid,
            "operation_digest": self.operation_digest,
            "incumbent": self.incumbent.to_payload(),
        }

    @classmethod
    def from_payload(cls, value):
        """Decode the closed stage identity; native custody must still be verified."""
        require_output(
            type(value) is dict
            and set(value) == {"schema", "target", "relation", "oid", "operation_digest", "incumbent"}
            and type(value["oid"]) is int
            and 0 < value["oid"] < 2**32,
            "stage_receipt_invalid",
        )
        stage_by_field = dict(value)
        stage_by_field["incumbent"] = NativeOutputRelation.from_payload(stage_by_field["incumbent"])
        require_output(
            stage_by_field["schema"] == stage_by_field["incumbent"].schema
            and stage_by_field["target"] == stage_by_field["incumbent"].target
            and type(stage_by_field["operation_digest"]) is str
            and len(stage_by_field["operation_digest"]) == 64
            and set(stage_by_field["operation_digest"]) <= set("0123456789abcdef"),
            "stage_receipt_invalid",
        )
        storage_identifier(stage_by_field["relation"])
        return cls(**stage_by_field)


def _descriptor(stage, phase):
    return {
        "protocol": 1,
        "operation_digest": stage.operation_digest,
        "target": stage.target,
        "phase": phase,
        "incumbent_oid": stage.incumbent.oid,
        "incumbent_digest": _digest(stage.incumbent.to_payload()),
    }


async def _comment(capability, stage, phase):
    statement = await capability.connection.fetchval(
        "SELECT format('COMMENT ON TABLE %I.%I IS %L',$1::text,$2::text,$3::text)",
        stage.schema,
        stage.relation,
        encoded(_descriptor(stage, phase)),
    )
    await capability.connection.execute(statement)


async def _stage_row(capability, stage, *, phases):
    await _require_capability(capability)
    require_output(
        type(stage) is OutputStage
        and stage.schema == capability.policy.schema
        and stage.target in capability.targets
        and stage.operation_digest == capability.operation_digest
        and capability.reference is None
        and stage.relation not in output_models(),
        "stage_identity_changed",
    )
    storage_identifier(stage.relation)
    catalog_entries = await capability.connection.fetch(
        OUTPUT_CATALOG_SQL,
        stage.schema,
        [stage.relation],
        stage.incumbent.owner_oid,
        [],
    )
    require_output(len(catalog_entries) == 1, "stage_missing")
    catalog_entry = catalog_entries[0]
    descriptor = decoded(catalog_entry["descriptor"])
    require_output(
        catalog_entry["oid"] == stage.oid
        and catalog_entry["schema_oid"] == stage.incumbent.schema_oid
        and catalog_entry["relowner"] == stage.incumbent.owner_oid
        and catalog_entry["acl_closed"]
        and catalog_entry["readers_closed"]
        and type(descriptor) is dict
        and descriptor.get("phase") in phases
        and descriptor == _descriptor(stage, descriptor["phase"]),
        "stage_custody_changed",
    )
    require_output_shape(catalog_entry, stage.target, indexed=descriptor["phase"] != "loading")
    from process.mrf_address_publication import require_native_read_catalog

    await require_native_read_catalog(
        capability.session,
        (stage.oid,),
        executable_roles=(capability.policy.owner_role, capability.policy.publisher_role),
    )
    return catalog_entry


async def prepare_output_stage(directory, capability, target):
    """Capture the immutable incumbent before a preservation Reader may scan it."""
    await _require_capability(capability)
    require_output(
        target in capability.targets
        and directory._schema() == capability.policy.schema
        and capability.reference is None,
        "stage_identity_changed",
    )
    incumbent = (await _capture(capability, {target: target}))[target]
    name = "pdout_" + uuid4().hex
    relation = storage_relation(capability.policy.schema, name)
    await capability.connection.execute(
        f"CREATE TABLE {relation} (LIKE {storage_relation(capability.policy.schema, target)} INCLUDING STORAGE)"
    )
    await _close_grants(capability.connection, capability.policy, name)
    oid = await capability.connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
    stage = OutputStage(capability.policy.schema, target, name, oid, capability.operation_digest, incumbent)
    await _comment(capability, stage, "loading")
    await _stage_row(capability, stage, phases={"loading"})
    return stage


async def copy_output_rows(directory, capability, stage, rows):
    """Encode complete model rows and COPY bounded groups; never merge a live heap."""
    await _stage_row(capability, stage, phases={"loading"})
    require_output(directory._schema() == capability.policy.schema, "schema_changed")
    model = output_models()[stage.target]
    columns = tuple(str(column.name) for column in model.__table__.columns)
    json_columns = directory._json_columns(model.__table__)
    copied = 0
    records = (directory._copy_record(row, columns, json_columns) for row in rows)
    try:
        for batch in _record_batches(records):
            status = await capability.connection.copy_records_to_table(
                stage.relation,
                schema_name=stage.schema,
                columns=columns,
                records=batch,
            )
            require_output(status == f"COPY {len(batch)}", "copy_incomplete")
            copied += len(batch)
    except asyncio.CancelledError, TimeoutError:
        from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner

        await close_interrupted_copy(capability.connection)
        await invalidate_copy_owner(capability.session, capability.connection)
        raise
    return copied


async def finish_output_indexes(directory, capability, stage):
    """Finish all native keys/indexes after COPY, before indexed set validation."""
    await _stage_row(capability, stage, phases={"loading"})
    require_output(directory._schema() == capability.policy.schema, "schema_changed")
    model = output_models()[stage.target]
    relation = storage_relation(stage.schema, stage.relation)
    primary = ",".join(storage_identifier(str(column.name)) for column in model.__table__.primary_key.columns)
    await capability.connection.execute(
        f"ALTER TABLE {relation} ADD CONSTRAINT {storage_identifier(stage.relation + '_pk')} PRIMARY KEY ({primary})"
    )
    for number, index in enumerate(getattr(model, "__my_additional_indexes__", ())):
        method = "USING " + index["using"] if index.get("using") else ""
        predicate = " WHERE " + index["where"] if index.get("where") else ""
        elements = directory._provider_directory_index_elements_sql(model, index)
        name = storage_identifier(stage.relation + "_i" + str(number))
        unique = "UNIQUE " if index.get("unique") else ""
        await capability.connection.execute(
            f"CREATE {unique}INDEX {name} ON {relation} {method} ({elements}){predicate}"
        )
    await capability.connection.execute(f"ANALYZE {relation}")
    await _comment(capability, stage, "indexed")
    await _stage_row(capability, stage, phases={"indexed"})


async def seal_output_stage(capability, stage, *, expected_rows):
    """Seal native accounting after the owning producer's indexed semantic checks."""
    require_output(type(expected_rows) is int and expected_rows >= 0, "row_count_invalid")
    await _stage_row(capability, stage, phases={"indexed"})
    count = await capability.connection.fetchval(
        f"SELECT count(*) FROM {storage_relation(stage.schema, stage.relation)}"
    )
    require_output(count == expected_rows, "row_count_changed")
    await _comment(capability, stage, "sealed")
    await _stage_row(capability, stage, phases={"sealed"})


async def discard_output_stage(capability, stage):
    """Drop only the exact authenticated private candidate, never an installed root."""
    await _stage_row(capability, stage, phases={"loading", "indexed", "sealed"})
    await capability.connection.execute(f"DROP TABLE {storage_relation(stage.schema, stage.relation)}")
