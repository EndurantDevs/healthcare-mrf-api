# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native loading and carry-forward for isolated FHIR metadata candidates."""

import json
import os
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import asdict
from datetime import date, datetime
from uuid import UUID, uuid4

from asyncpg import InsufficientPrivilegeError, InvalidSchemaNameError, UndefinedTableError

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import table_sql
from process.registry_fhir_catalog_custody import (
    PHYSICAL_TABLES,
    FHIRCatalogTarget,
    validate_fhir_catalog_snapshot_record,
)
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_projection import BINDING_COLUMNS, NETWORK_COLUMNS, RELATIONSHIP_COLUMNS
from process.registry_fhir_catalog_validation import canonical_json, snapshot_inputs

TABLE_COLUMNS = {
    "registry_fhir_catalog_source": (
        "source_generation",
        "source_id",
        "descriptor_json",
        "profile",
        "source_rows",
        "observation_rows",
        "accepted_at",
        "created_at",
    ),
    "registry_fhir_catalog_observation": (
        "source_generation",
        "resource_type",
        "resource_id",
        "observation_kind",
        "ordinal",
        "document_json",
    ),
    "registry_fhir_catalog_network": NETWORK_COLUMNS,
    "registry_fhir_catalog_relationship": RELATIONSHIP_COLUMNS,
    "registry_fhir_catalog_binding": BINDING_COLUMNS,
}
_KEYS = {
    "registry_fhir_catalog_source": (("source_generation", "text"),),
    "registry_fhir_catalog_observation": (
        ("source_generation", "text"),
        ("resource_type", "text"),
        ("resource_id", "text"),
        ("observation_kind", "text"),
        ("ordinal", "integer"),
    ),
    "registry_fhir_catalog_network": (("metadata_generation", "text"), ("network_id", "integer")),
    "registry_fhir_catalog_relationship": (("metadata_generation", "text"), ("relationship_id", "text")),
    "registry_fhir_catalog_binding": (("metadata_generation", "text"), ("binding_id", "uuid")),
}
DEFAULT_ROWS = 1000
DEFAULT_BYTES = 8 * 1024 * 1024
MAX_BYTES = 34 * 1024 * 1024
_COPY_LANDINGS = ContextVar("fhir_catalog_copy_landings", default=None)


def named_catalog_preparation_roles(configured=None):
    """Read configured administrative role names outside portable snapshot metadata."""
    roles_by_name = configured
    if roles_by_name is None:
        from process.registry_fhir_catalog_worker import CONFIG_ENV, _validated_fhir_catalog_sync_config

        roles_by_name = _validated_fhir_catalog_sync_config(os.environ.get(CONFIG_ENV))
    if type(roles_by_name) is not dict:
        if getattr(roles_by_name, "preparation_roles", None) is not None:
            return named_catalog_preparation_roles(roles_by_name.preparation_roles)
        roles_by_name = {
            "namespace_owner_role": roles_by_name.consumer.physical_owner_role,
            "publisher_role": roles_by_name.sealer_url.username,
            "application_roles": sorted(
                {
                    roles_by_name.loader_role,
                    roles_by_name.sealer_url.username,
                    roles_by_name.lifecycle_url.username,
                    roles_by_name.execution_url.username,
                    *roles_by_name.reader_roles,
                    *roles_by_name.consumer.loader_roles,
                    *roles_by_name.consumer.reader_roles,
                }
            ),
        }
    if set(roles_by_name) != {"namespace_owner_role", "publisher_role", "application_roles"}:
        _invalid("preparation_roles_invalid")
    applications = roles_by_name["application_roles"]
    if (
        type(applications) not in {list, tuple}
        or not 1 <= len(applications) <= 128
        or any(type(role) is not str for role in applications)
        or list(applications) != sorted(set(applications))
    ):
        _invalid("preparation_roles_invalid")
    owner, publisher = roles_by_name["namespace_owner_role"], roles_by_name["publisher_role"]
    for role in (owner, publisher, *applications):
        storage_identifier(role)
    if owner in applications or owner == publisher or publisher not in applications:
        _invalid("preparation_roles_invalid")
    return {"namespace_owner_role": owner, "publisher_role": publisher, "application_roles": list(applications)}


def catalog_snapshot_preparation(snapshot):
    """Decode the bounded logical checkpoint retained with the catalog control row."""
    record = snapshot.get("custody_json")
    if type(record) is str:
        record = json.loads(record)
    if type(record) is not dict or "preparation" not in record:
        return None
    validation_fields = {"metadata_generation", "inputs", "tables", "content_sha256"}
    if set(record) not in ({"preparation"}, {"preparation"} | validation_fields):
        _invalid("preparation_invalid")
    preparation = record["preparation"]
    if type(preparation) is not dict or set(preparation) != {"target", "previous_metadata_generation", "accounting"}:
        _invalid("preparation_invalid")
    if type(preparation["target"]) is not dict or set(preparation["target"]) != set(
        FHIRCatalogTarget.__dataclass_fields__
    ):
        _invalid("preparation_invalid")
    target = FHIRCatalogTarget(**preparation["target"])
    if target.schema_name != snapshot["schema_name"]:
        _invalid("preparation_invalid")
    previous = preparation["previous_metadata_generation"]
    if previous is not None:
        _digest(previous)
    if type(preparation["accounting"]) is not dict:
        _invalid("preparation_invalid")
    if len(canonical_json(preparation).encode()) > DEFAULT_BYTES:
        _invalid("preparation_bounds")
    return preparation


def has_catalog_snapshot_validation(snapshot):
    """Distinguish complete table validation from logical inputs retained during loading."""
    record_by_field = snapshot.get("custody_json")
    if type(record_by_field) is str:
        record_by_field = json.loads(record_by_field)
    if type(record_by_field) is not dict:
        return False
    if "preparation" in record_by_field:
        catalog_snapshot_preparation(snapshot)
    return "tables" in record_by_field


def named_catalog_preparation(snapshot, control_schema):
    """Produce the stable common handoff from an actual persisted catalog checkpoint."""
    preparation = catalog_snapshot_preparation(snapshot)
    if preparation is None:
        return None
    _digest(snapshot["metadata_generation"])
    target = FHIRCatalogTarget(**preparation["target"])
    storage_identifier(control_schema)
    return {
        "kind": "catalog",
        "candidate_id": target.candidate_id,
        "catalog_schema": control_schema,
        "storage_schema": target.schema_name,
        "tables": [{"schema": target.schema_name, "name": table} for table in sorted(PHYSICAL_TABLES)],
        "snapshot": {
            "metadata_generation": snapshot["metadata_generation"],
            "target": preparation["target"],
            "previous_metadata_generation": preparation["previous_metadata_generation"],
        },
    }


async def load_named_catalog_preparation(connection, candidate_id, *, control_schema=None):
    """Load one candidate through its indexed qualified storage name."""
    from process.registry_source_observation_store import source_observation_namespace

    candidate = UUID(str(candidate_id))
    if not candidate.int or str(candidate) != str(candidate_id):
        _invalid("preparation_invalid")
    namespace = source_observation_namespace(control_schema)
    snapshot = await connection.fetchrow(
        f"SELECT metadata_generation,schema_name,custody_json FROM {namespace}.registry_fhir_catalog_snapshot "
        "WHERE schema_name=$1",
        "fhir_catalog_" + candidate.hex,
    )
    return None if snapshot is None else named_catalog_preparation(snapshot, namespace[1:-1])


async def checkpoint_fhir_catalog_snapshot(connection, target, snapshot, previous, accounting, *, control_schema):
    """Persist completed candidate loading before handing its permissions to the operator."""
    from db.registry_fhir_catalog import SNAPSHOT_COLUMNS

    _transaction(connection)
    preparation_by_field = {
        "target": asdict(target),
        "previous_metadata_generation": None if previous is None else previous["metadata_generation"],
        "accounting": json.loads(canonical_json(accounting)),
    }
    inputs_by_field = snapshot_inputs(snapshot)
    logical_accounting_by_key = (
        {"imported_source_inventory": inputs_by_field["imported_source_inventory"]}
        if "imported_source_inventory" in inputs_by_field
        else {}
    )
    snapshot.update(custody_json={"preparation": preparation_by_field}, accounting_json=logical_accounting_by_key)
    named_catalog_preparation(snapshot, control_schema)
    columns = tuple(column for column in SNAPSHOT_COLUMNS if column != "published_at")
    json_columns = {"custody_json", "accounting_json", "source_vector_json"}
    parameters = ",".join(
        f"${index}" + ("::jsonb" if column in json_columns else "") for index, column in enumerate(columns, 1)
    )
    await connection.execute(
        f"INSERT INTO {storage_identifier(control_schema)}.registry_fhir_catalog_snapshot"
        f"({','.join(columns)}) VALUES({parameters})",
        *(canonical_json(snapshot[column]) if column in json_columns else snapshot[column] for column in columns),
    )


async def admit_catalog_preparation(connection, snapshot, *, control_schema, roles):
    """Admit or resume the genuine catalog run recorded in the candidate checkpoint."""
    import hashlib

    from process.registry_named_preparation_records import NativePreparationAdmission
    from process.registry_named_preparation_store import NamedPreparationStore

    handoff = named_catalog_preparation(snapshot, control_schema)
    if handoff is None:
        _invalid("preparation_required")
    candidate, generation = handoff["candidate_id"], snapshot["metadata_generation"]
    request = NativePreparationAdmission(
        preparation_id=candidate,
        node_id=control_schema,
        importer_id="catalog",
        dataset_key=generation,
        run_id=candidate,
        generation_id=candidate,
        publication_fence=candidate,
        idempotency_key="catalog:" + generation,
        request_sha256=hashlib.sha256(canonical_json(handoff).encode()).hexdigest(),
    )
    store = NamedPreparationStore(connection, named_catalog_preparation_roles(roles), kind="catalog")
    row, claim = await store.admit_named(request, handoff, attempt_id=candidate)
    return store, row, claim


def _invalid(reason):
    raise ValueError("registry_fhir_catalog_store_" + reason)


def _target(target):
    if type(target) is not FHIRCatalogTarget:
        _invalid("target_invalid")
    return storage_identifier(target.schema_name)


def _bounds(max_rows, max_bytes):
    if (
        type(max_rows) is not int
        or not 1 <= max_rows <= 10000
        or type(max_bytes) is not int
        or not 1 <= max_bytes <= MAX_BYTES
    ):
        _invalid("bounds_invalid")


def _digest(generation):
    if type(generation) is not str or re.fullmatch(r"[0-9a-f]{64}", generation) is None:
        _invalid("generation_invalid")


def _transaction(connection):
    if not connection.is_in_transaction():
        _invalid("transaction_required")


def _table(table, columns=None):
    if table not in TABLE_COLUMNS:
        _invalid("table_invalid")
    selected_columns = TABLE_COLUMNS[table] if columns is None else tuple(columns)
    allowed_columns = (TABLE_COLUMNS[table],)
    if table == "registry_fhir_catalog_source":
        allowed_columns += (TABLE_COLUMNS[table][:-1],)
    if selected_columns not in allowed_columns:
        _invalid("columns_invalid")
    return selected_columns


async def _unregistered(connection, target, control_schema):
    if not connection.is_in_transaction():
        _invalid("transaction_required")
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {storage_identifier(control_schema)}.registry_fhir_catalog_snapshot WHERE schema_name=$1)",
        target.schema_name,
    ):
        _invalid("snapshot_immutable")


async def _mutable(connection, target, loader_role, control_schema):
    namespace = _target(target)
    storage_identifier(loader_role)
    if not connection.is_in_transaction():
        _invalid("transaction_required")
    heaps = ",".join(namespace + "." + table for table in sorted(PHYSICAL_TABLES))
    await connection.execute("LOCK TABLE " + heaps + " IN ROW EXCLUSIVE MODE")
    await _unregistered(connection, target, control_schema)


async def create_fhir_catalog_candidate(connection, target, *, loader_role, control_schema):
    """Create fresh heaps and indexes needed while assembling their projection."""
    namespace = _target(target)
    role = storage_identifier(loader_role)
    await _unregistered(connection, target, control_schema)
    async with connection.transaction():
        await connection.execute(f"CREATE SCHEMA {namespace} AUTHORIZATION {role}")
        for table in sorted(PHYSICAL_TABLES):
            await connection.execute(table_sql(namespace, table)[0])
            # The configured loader writes its own candidate; a no-op when it created the heap.
            await connection.execute(f"ALTER TABLE {namespace}.{table} OWNER TO {role}")
        await connection.execute(
            f"CREATE INDEX registry_fhir_catalog_binding_evidence_idx ON {namespace}.registry_fhir_catalog_binding "
            "((document_json->>'evidence_id') COLLATE \"C\")"
        )
        await _mutable(connection, target, loader_role, control_schema)
    return len(PHYSICAL_TABLES)


async def has_discarded_fhir_catalog_candidate(connection, target, *, loader_role, control_schema):
    """Remove only an unregistered candidate through its configured loader."""
    _transaction(connection)
    namespace = _target(target)
    await _unregistered(connection, target, control_schema)
    if not await connection.fetchval("SELECT to_regnamespace($1) IS NOT NULL", target.schema_name):
        return False
    await _mutable(connection, target, loader_role, control_schema)
    heaps = ",".join(namespace + "." + table for table in sorted(PHYSICAL_TABLES))
    await connection.execute("LOCK TABLE " + heaps + " IN ACCESS EXCLUSIVE MODE")
    await _unregistered(connection, target, control_schema)
    await connection.execute("DROP TABLE " + heaps)
    await connection.execute("DROP SCHEMA " + namespace)
    return True


discard_fhir_catalog_candidate = has_discarded_fhir_catalog_candidate


async def create_fhir_catalog_indexes(connection, target, *, loader_role, control_schema, trigram_schema="public"):
    """Prepare secondary indexes and statistics on the completed candidate."""
    namespace = _target(target)
    _transaction(connection)
    trigram = storage_identifier(trigram_schema)
    async with connection.transaction():
        await _mutable(connection, target, loader_role, control_schema)
        count = 0
        for table in sorted(PHYSICAL_TABLES):
            for statement in table_sql(namespace, table, trigram_schema=trigram)[1:]:
                await connection.execute(statement)
                count += 1
            await connection.execute(f"ANALYZE {namespace}.{table}")
    return count


def _payload_size(value):
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8")) + 1
    if isinstance(value, UUID):
        return 16
    if isinstance(value, bool):
        return 1
    if isinstance(value, int):
        return 8
    if isinstance(value, datetime | date):
        return max(8, len(value.isoformat().encode()))
    if isinstance(value, list | tuple):
        return 20 + sum(4 + _payload_size(entry) for entry in value)
    _invalid("row_value_invalid")


def _batches(rows, columns, max_rows, max_bytes):
    batch_rows, batch_bytes = [], 21
    for row in rows:
        if not isinstance(row, tuple | list) or len(row) != len(columns):
            _invalid("row_shape_invalid")
        row_bytes = 2 + sum(8 + _payload_size(value) for value in row)
        if row_bytes + 21 > max_bytes:
            _invalid("row_too_large")
        if batch_rows and (len(batch_rows) == max_rows or batch_bytes + row_bytes > max_bytes):
            yield batch_rows, batch_bytes
            batch_rows, batch_bytes = [], 21
        batch_rows.append(tuple(row))
        batch_bytes += row_bytes
    if batch_rows:
        yield batch_rows, batch_bytes


def _conflict(table, columns, mode):
    keys = tuple(name for name, _ in _KEYS[table])
    if mode == "preserve":
        return "ON CONFLICT (" + ",".join(keys) + ") DO NOTHING"
    if mode != "replace":
        _invalid("mode_invalid")
    assignments = ",".join(name + "=EXCLUDED." + name for name in columns if name not in keys)
    return "ON CONFLICT (" + ",".join(keys) + ") DO UPDATE SET " + assignments


def _write_options(options):
    default_by_name = {"mode": "replace", "columns": None, "max_rows": DEFAULT_ROWS, "max_bytes": DEFAULT_BYTES}
    if set(options) - set(default_by_name):
        _invalid("options_invalid")
    option_by_name = {**default_by_name, **options}
    return tuple(option_by_name[name] for name in ("mode", "columns", "max_rows", "max_bytes"))


def _create_landing_sql(landing, namespace, table):
    return f"CREATE TEMP TABLE {landing} (LIKE {namespace}.{table} INCLUDING DEFAULTS) ON COMMIT DROP"


@asynccontextmanager
async def fhir_catalog_copy_landings(connection, target):
    """Create this private candidate's COPY tables in its outer transaction, before any per-write savepoints."""
    _transaction(connection)
    namespace = _target(target)
    landings_by_table = {table: "fhir_landing_" + uuid4().hex for table in TABLE_COLUMNS}
    await connection.execute(
        ";".join(_create_landing_sql(name, namespace, table) for table, name in landings_by_table.items())
    )
    scope_by_field = {"writer": connection, "candidate": target, "tables": landings_by_table}
    token = _COPY_LANDINGS.set(scope_by_field)
    try:
        yield
    finally:
        _COPY_LANDINGS.reset(token)
        scope_by_field.clear()  # Child tasks can inherit this context and outlive the transaction.


def _shared_landing(connection, target, table):
    scope = _COPY_LANDINGS.get()
    if scope and scope["writer"] is connection and scope["candidate"] is target:
        return scope["tables"][table]
    return None


async def write_fhir_catalog_rows(
    connection,
    copy_target,
    table,
    copy_rows,
    *,
    loader_role,
    control_schema,
    **options,
):
    """COPY bounded landing batches and apply one set upsert per batch."""
    mode, columns, max_rows, max_bytes = _write_options(options)
    selected_columns = _table(table, columns)
    _transaction(connection)
    conflict = _conflict(table, selected_columns, mode)
    _bounds(max_rows, max_bytes)
    namespace = _target(copy_target)
    shared_landing = _shared_landing(connection, copy_target, table)
    landing = shared_landing or "fhir_landing_" + uuid4().hex
    column_sql = ",".join(selected_columns)
    keys = ",".join(name for name, _ in _KEYS[table])
    accounting_by_name = {"input_rows": 0, "affected_rows": 0, "batches": 0, "payload_bytes": 0}
    async with connection.transaction():
        await _mutable(connection, copy_target, loader_role, control_schema)
        if shared_landing is None:
            await connection.execute(_create_landing_sql(landing, namespace, table))
        for batch_rows, batch_bytes in _batches(copy_rows, selected_columns, max_rows, max_bytes):
            await connection.execute(f"TRUNCATE pg_temp.{landing}")
            await connection.copy_records_to_table(
                landing, records=batch_rows, columns=selected_columns, schema_name="pg_temp"
            )
            command_status = await connection.execute(
                f"INSERT INTO {namespace}.{table} ({column_sql}) SELECT DISTINCT ON ({keys}) {column_sql} "
                f"FROM pg_temp.{landing} ORDER BY {keys},ctid DESC {conflict}"
            )
            accounting_by_name["input_rows"] += len(batch_rows)
            accounting_by_name["affected_rows"] += int(command_status.rsplit(" ", 1)[-1])
            accounting_by_name["batches"] += 1
            accounting_by_name["payload_bytes"] += batch_bytes
        if shared_landing is None:
            await connection.execute(f"DROP TABLE pg_temp.{landing}")
    return accounting_by_name


async def replace_fhir_catalog_relationships(connection, target, source_generations, *, loader_role, control_schema):
    """Remove selected derived edges only; observations and old editions stay retained."""
    generations = tuple(source_generations)
    _transaction(connection)
    for generation in generations:
        _digest(generation)
    if len(generations) > 10000:
        _invalid("bounds_invalid")
    namespace = _target(target)
    async with connection.transaction():
        await _mutable(connection, target, loader_role, control_schema)
        result = await connection.execute(
            f"DELETE FROM {namespace}.registry_fhir_catalog_relationship WHERE source_generation=ANY($1::text[])",
            generations,
        )
    return int(result.rsplit(" ", 1)[-1])


def _copy_sql(previous_schema, candidate_schema, table):
    columns = TABLE_COLUMNS[table]
    expressions = [
        "$1::text"
        if name == "metadata_generation"
        else 'document_json||\'{"status":"last_good"}\'::jsonb'
        if table.endswith("_binding") and name == "document_json"
        else name
        for name in columns
    ]
    return (
        f"INSERT INTO {candidate_schema}.{table} ({','.join(columns)}) "
        f"SELECT {','.join(expressions)} FROM {previous_schema}.{table} "
        f"WHERE $1::text IS NOT NULL {_conflict(table, columns, 'preserve')}"
    )


def _previous_totals_sql(previous_schema, table_names):
    selects = ",".join(
        f"(SELECT jsonb_build_array(count(*),coalesce(sum(pg_column_size(stored)),0)::bigint)::text "
        f"FROM {previous_schema}.{table} stored) AS {table}"
        for table in table_names
    )
    return "SELECT " + selects


async def copy_pending_fhir_observations(
    connection, copy_target, snapshot, previous, current_by_generation, donor_reader, *, loader_role, control_schema
):
    """Copy exact observed facts from one validated, unpublished candidate before normal projection."""
    carried_generations = (
        {entry["source_generation"] for entry in previous["source_vector_json"]} if previous else set()
    )
    wanted_by_generation = {
        generation: disposition
        for generation, disposition in current_by_generation.items()
        if generation not in carried_generations
        and isinstance(disposition.descriptor, dict)
        and isinstance(admission := disposition.descriptor.get("admission"), dict)
        and admission.get("contract") == CATALOG_OBSERVATION_CONTRACT
    }
    if not wanted_by_generation:
        return {}
    transform_by_generation = {
        entry["source_generation"]: entry.get("extraction_transform")
        for entry in snapshot["source_vector_json"]
        if entry["source_generation"] in wanted_by_generation
    }
    donor = await _select_observation_donor(donor_reader, control_schema, snapshot, transform_by_generation)
    if donor is None:
        return {}
    donor_schema = storage_identifier(donor["schema_name"])
    candidate_schema = _target(copy_target)
    requested_descriptors = [
        {
            "generation": generation,
            "descriptor": wanted_by_generation[generation].descriptor,
            "accepted_at": wanted_by_generation[generation].accepted_at.isoformat(),
        }
        for generation in donor["generations"]
    ]
    try:
        async with connection.transaction():
            await _mutable(connection, copy_target, loader_role, control_schema)
            generations = await _copy_observation_rows(
                connection, candidate_schema, donor_schema, requested_descriptors
            )
    except UndefinedTableError, InvalidSchemaNameError, InsufficientPrivilegeError:
        # An unavailable optional donor rolls back the partial copy and falls back to ordinary extraction.
        return {}
    return dict.fromkeys(generations, donor["metadata_generation"])


async def _select_observation_donor(donor_reader, control_schema, snapshot, transform_by_generation):
    async with donor_reader as sealer:
        return await sealer.fetchrow(
            "SELECT s.schema_name,s.metadata_generation,overlap.generations "
            f"FROM {storage_identifier(control_schema)}.registry_fhir_catalog_snapshot s "
            "JOIN hp_snapshot_retention.preparation_relation r ON r.schema_name=s.schema_name "
            "AND r.relation_name='registry_fhir_catalog_observation' "
            "JOIN hp_snapshot_retention.preparation p ON p.preparation_id=r.preparation_id "
            "AND p.node_id=$5 AND p.importer_id='catalog' AND p.dataset_key=s.metadata_generation "
            "AND p.state='validated' AND NOT p.cancellation_requested "
            "CROSS JOIN LATERAL (SELECT array_agg(v->>'source_generation') AS generations "
            "FROM jsonb_array_elements(source_vector_json) v WHERE v->>'status'='current' "
            "AND $4::jsonb ? (v->>'source_generation') "
            "AND ($4::jsonb->>(v->>'source_generation')) IS NOT DISTINCT FROM (v->>'extraction_transform')) overlap "
            "WHERE standing_grant_sha256=$1 AND metadata_approved_revision=$2 AND projector_version=$3 "
            "AND s.published_at IS NULL "
            "AND overlap.generations IS NOT NULL "
            "ORDER BY cardinality(overlap.generations) DESC,as_of DESC,metadata_generation DESC LIMIT 1",
            snapshot["standing_grant_sha256"],
            snapshot["metadata_approved_revision"],
            snapshot["projector_version"],
            canonical_json(transform_by_generation),
            control_schema,
        )


async def _copy_observation_rows(connection, candidate_schema, donor_schema, requested_descriptors):
    source_rows = await connection.fetch(
        f"INSERT INTO {candidate_schema}.registry_fhir_catalog_source "
        "(source_generation,source_id,descriptor_json,profile,source_rows,observation_rows,accepted_at) "
        "SELECT s.source_generation,s.source_id,s.descriptor_json,s.profile,s.source_rows,s.observation_rows,r.accepted_at "
        f"FROM {donor_schema}.registry_fhir_catalog_source s JOIN jsonb_to_recordset($1::jsonb) "
        "AS r(generation text,descriptor jsonb,accepted_at timestamptz) "
        "ON s.source_generation=r.generation AND s.descriptor_json=r.descriptor "
        "RETURNING source_generation,observation_rows",
        canonical_json(requested_descriptors),
    )
    generations = [copied_source["source_generation"] for copied_source in source_rows]
    columns = ",".join(TABLE_COLUMNS["registry_fhir_catalog_observation"])
    copied = await connection.execute(
        f"INSERT INTO {candidate_schema}.registry_fhir_catalog_observation ({columns}) "
        f"SELECT {columns} FROM {donor_schema}.registry_fhir_catalog_observation WHERE source_generation=ANY($1::text[])",
        generations,
    )
    if int(copied.rsplit(" ", 1)[1]) != sum(copied_source["observation_rows"] for copied_source in source_rows):
        _invalid("observation_count_changed")
    return generations


async def copy_fhir_catalog_snapshot(
    connection,
    previous_snapshot,
    copy_target,
    metadata_generation,
    *,
    loader_role,
    control_schema,
):
    """Carry validated named metadata tables forward set-based while preserving retained generations."""
    _transaction(connection)
    _digest(metadata_generation)
    namespace = _target(copy_target)
    previous = storage_identifier(previous_snapshot["schema_name"])
    if previous == namespace:
        _invalid("snapshot_immutable")
    await validate_fhir_catalog_snapshot_record(connection, previous_snapshot)
    accounting_by_name = {"input_rows": 0, "affected_rows": 0, "batches": 0, "payload_bytes": 0}
    table_names = sorted(PHYSICAL_TABLES)
    async with connection.transaction():
        await _mutable(connection, copy_target, loader_role, control_schema)
        for table in table_names:
            command_status = await connection.execute(_copy_sql(previous, namespace, table), metadata_generation)
            accounting_by_name["affected_rows"] += int(command_status.rsplit(" ", 1)[-1])
        totals_summary = await connection.fetchrow(_previous_totals_sql(previous, table_names))
        for table in table_names:
            row_count, relation_bytes = json.loads(totals_summary[table])
            accounting_by_name["input_rows"] += row_count
            if row_count:
                accounting_by_name["batches"] += 1
                accounting_by_name["payload_bytes"] += relation_bytes
    return accounting_by_name
