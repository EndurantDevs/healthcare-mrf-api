# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native COPY with logical targets and whole-operation counters."""

import asyncio
from dataclasses import asdict, dataclass, field
from weakref import WeakKeyDictionary, WeakValueDictionary

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as source
from process import provider_directory_owned_wal_transaction as owned
from process.provider_directory_backend_wal_diagnostic import BackendWalDiagnosticResult
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    _column_type,
    decoded,
    encoded,
    output_models,
)
from process.provider_directory_output_snapshot_stage import _record_bytes

_ISSUED = WeakKeyDictionary()
_ATTEMPTED = WeakValueDictionary()
_TARGET_CATALOG_SQL = OUTPUT_CATALOG_SQL


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_profile_native_copy_custody_invalid")


@dataclass(frozen=True, eq=False)
class NativeCopyWitness:
    """Issued transport facts; indexed snapshot closure must prove semantic parity."""

    batch_json: str
    target: tuple[str, str]
    columns: tuple[str, ...]
    affected_rows: int
    command_counts: tuple[int, ...]
    source_json: str
    semantic_scope: str = field(default="transport_only", init=False)


@dataclass(frozen=True)
class NativeCopyRequest:
    """The exact existing frozen coordinate and prepared Source SELECT."""

    model: object
    relation: str
    batch: object
    table_projection: object
    source_binding: dict
    projection_sql: str
    projection_params: dict
    scope_projection: object


async def _origin(fhir, outcome):
    publisher = bound_publisher()
    _require(
        type(outcome) is owned.OwnedWalTransaction
        and owned.current_owned_wal_transaction(fhir.db) is outcome
        and outcome.session.in_transaction()
        and not outcome.session.in_nested_transaction()
        and outcome.commit_state == "not_attempted"
    )
    source_copy_session()
    return publisher, publisher.connection


async def _source(fhir, publisher, binding, batch):
    from process import provider_directory_profile_source_inputs as inputs

    binding = source._binding_json(binding, publisher.policy.schema)
    selected = inputs.dataset(fhir, batch)
    identity = binding["identity"]
    _require(
        type(selected) is fhir.ProviderDirectoryArtifactDataset
        and (selected.source_id, selected.endpoint_id, selected.dataset_id, selected.evidence_run_id)
        == (batch.source_id, identity["endpoint_id"], identity["dataset_id"], identity["acquisition_root_run_id"])
    )
    source._validate_binding(binding, identity, phases=("published",))
    _require(batch.source_id in identity["metadata"].get("source_ids", ()))
    if batch.resource_type != "source":
        _require(
            (batch.dataset_id, batch.evidence_run_id) == (identity["dataset_id"], identity["acquisition_root_run_id"])
        )
    header = await publisher.connection.fetchrow(
        "SELECT endpoint_id,acquisition_root_run_id,status,publication_metadata_json,candidate_storage_json FROM "
        + storage_relation(publisher.policy.schema, source.HEADER)
        + " WHERE dataset_id=$1 FOR SHARE",
        identity["dataset_id"],
    )
    _require(
        header is not None
        and header["status"] == selected.status
        and (header["endpoint_id"], header["acquisition_root_run_id"])
        == (identity["endpoint_id"], identity["acquisition_root_run_id"])
        and source._binding_json(header["candidate_storage_json"], publisher.policy.schema) == binding
        and all(
            source._json(header["publication_metadata_json"]).get(key) == metadata_value
            for key, metadata_value in identity["metadata"].items()
        )
    )
    names = sorted(proof["name"] for proof in binding["tables"].values())
    await publisher.connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(publisher.policy.schema, name) for name in names)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    await source._verify_tables(publisher.connection, publisher.policy, binding)


def _target_columns(model, catalog):
    """Match the exact CreateColumn layout, including order and native types."""
    columns = decoded(catalog["columns"])
    expected_columns = [
        (str(column.name), _column_type(column, {}), not column.nullable) for column in model.__table__.columns
    ]
    _require([(column["name"], column["type"].lower(), column["required"]) for column in columns] == expected_columns)
    for observed, column in zip(columns, model.__table__.columns, strict=True):
        default = None if column.server_default is None else str(column.server_default.arg)
        _require(
            _normalized_expression(observed["default"]) == _normalized_expression(default)
            and observed["identity"] == observed["generated"] == ""
            and observed["native_collation"]
        )


def _target_indexes(fhir, model, relation, catalog, phase):
    """Loading heaps have no indexes; completion requires the exact legacy set."""
    constraints, indexes = decoded(catalog["constraints"]), decoded(catalog["indexes"])
    _require(phase in {"loading", "completed"})
    if phase == "loading":
        _require(
            not indexes
            and all(
                constraint["type"] == "n"
                and constraint["validated"]
                and not constraint["deferrable"]
                and not constraint["deferred"]
                for constraint in constraints
            )
        )
        return False
    return _completed_target_indexes(fhir, model, relation, catalog)


def _completed_target_indexes(fhir, model, relation, catalog):
    """Attest only the legacy PK and optional bucket after loading is closed."""
    constraints, indexes = decoded(catalog["constraints"]), decoded(catalog["indexes"])
    key_columns = tuple(str(column.name) for column in model.__table__.primary_key.columns)
    number_by_column = {column["name"]: column["number"] for column in decoded(catalog["columns"])}
    primary_constraints = [constraint for constraint in constraints if constraint["type"] == "p"]
    _require(
        len(primary_constraints) == 1
        and tuple(primary_constraints[0]["keys"]) == tuple(number_by_column[name] for name in key_columns)
    )
    _require(
        all(
            constraint["type"] in {"p", "n"}
            and constraint["validated"]
            and not constraint["deferrable"]
            and not constraint["deferred"]
            for constraint in constraints
        )
    )
    has_bucket = model in (fhir.ProviderDirectoryPractitionerRole, fhir.ProviderDirectoryOrganizationAffiliation)
    _require(len(indexes) == 1 + int(has_bucket) and sum(index["primary"] for index in indexes) == 1)
    for index in indexes:
        _require(
            all(index[flag] is True for flag in ("valid", "ready", "live", "immediate", "native"))
            and not index["exclusion"]
            and index["keys_count"] == index["total_count"]
            and index["method"] == "btree"
            and not _normalized_expression(index["predicate"])
        )
        if index["primary"]:
            _require(
                index["primary"]
                and index["unique"]
                and tuple(index["keys"]) == key_columns
                and not _normalized_expression(index["expression"])
            )
        else:
            _require(
                not index["primary"]
                and not index["unique"]
                and index["keys"] == ["source_id", None]
                and index["keys_count"] == 2
                and bool(index["expression"])
            )
    return has_bucket


def _target_catalog(fhir, model, relation, catalog, *, phase="loading"):
    """Reject hooks, unsupported topology and fixed data-layout drift."""
    topology = decoded(catalog["topology"])
    _require(
        catalog["relname"] == relation
        and topology["kind"] == "r"
        and topology["persistence"] == "u"
        and not any(topology[key] for key in ("partition", "rls", "force_rls", "parents", "children", "hooks", "rules"))
    )
    _target_columns(model, catalog)
    return _target_indexes(fhir, model, relation, catalog, phase)


async def _target(fhir, publisher, model, relation, *, phase="loading"):
    catalog_by_root = await _target_family_catalog(fhir, publisher, ((model, relation),), phase=phase)
    return catalog_by_root[model.__tablename__]


async def _target_family_catalog(fhir, publisher, target_bindings, *, phase):
    """Read one fresh catalog for the entire locked family, retaining every shape check."""
    admission = fhir._provider_directory_profile_capacity_admission()
    _require(admission is not None and target_bindings and phase in {"loading", "completed"})
    _require(
        len({model.__tablename__ for model, _ in target_bindings})
        == len({relation for _, relation in target_bindings})
        == len(target_bindings)
    )
    for model, relation in target_bindings:
        _require(relation == fhir._provider_directory_artifact_scope_table_name(model.__tablename__, admission.run_id))
    names = sorted(relation for _, relation in target_bindings)
    await publisher.connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(publisher.policy.schema, relation) for relation in names)
        + " IN ROW EXCLUSIVE MODE NOWAIT"
    )
    catalog_rows = await publisher.connection.fetch(_TARGET_CATALOG_SQL, publisher.policy.schema, names)
    catalog_by_name = {catalog_row["relname"]: catalog_row for catalog_row in catalog_rows}
    _require(len(catalog_rows) == len(catalog_by_name) == len(target_bindings) and set(catalog_by_name) == set(names))
    catalog_by_root = {}
    for model, relation in target_bindings:
        catalog = catalog_by_name[relation]
        if _target_catalog(fhir, model, relation, catalog, phase=phase):
            index_name = fhir._provider_directory_profile_bucket_index_sql(publisher.policy.schema, relation)[0]
            index_map = await fhir._profile_bucket_index_row(
                publisher.policy.schema, relation, index_name, model.__tablename__
            )
            fhir._validate_profile_bucket_index_shape(index_map, model.__tablename__)
        catalog_by_root[model.__tablename__] = dict(catalog)
    return catalog_by_root


async def completed_target_catalog(fhir, model, relation, *, outcome):
    """Return native completion facts; this neither validates rows nor authorizes publication."""
    original = await _origin(fhir, outcome)
    catalog = await _target(fhir, original[0], model, relation, phase="completed")
    await _origin(fhir, outcome)
    return encoded(dict(schema=original[0].policy.schema, target=model.__tablename__, native_catalog=catalog))


async def _copy_pages(fhir, request, pages, outcome, origin, columns):
    """Transport bounded records without replacing the original native endpoint."""
    json_columns = fhir._json_columns(request.model.__table__)
    publisher, driver = origin
    counts, copied = [], 0
    async for page in pages:
        _require(type(page) is list and 0 < len(page) <= 512)
        records = [fhir._copy_record(row, columns, json_columns) for row in page]
        _require(21 + sum(_record_bytes(record) for record in records) <= 16 * 1024 * 1024)
        _require(copied + len(records) <= request.batch.projected_rows)
        await _origin(fhir, outcome)
        try:
            command = await driver.copy_records_to_table(
                request.relation, schema_name=publisher.policy.schema, columns=columns, records=records
            )
        except (asyncio.CancelledError, TimeoutError) as interruption:
            from process.native_copy_cleanup import cleanup_interrupted_copy

            await cleanup_interrupted_copy(outcome.session, driver, interruption)
            raise
        _require(command == "COPY " + str(len(records)))
        copied += len(records)
        counts.append(len(records))
    return copied, tuple(counts)


async def execute_profile_copy(fhir, request, pages, *, outcome):
    """COPY one bounded transport coordinate; final indexed closure proves full tuples."""
    _require(type(request) is NativeCopyRequest)
    model, relation = request.model, request.relation
    batch = request.batch
    projection_sql, projection_params = request.projection_sql, request.projection_params
    _require(
        type(request.scope_projection) is fhir._ProviderDirectoryArtifactScopeExactProjection
        and type(request.table_projection) is fhir._ProviderDirectoryArtifactScopeTableProjection
        and any(table is request.table_projection for table in request.scope_projection.tables)
        and type(batch) is fhir._ProviderDirectoryArtifactScopeBatchProjection
        and batch.projected_rows > 0
        and any(candidate is batch for candidate in request.table_projection.batches)
        and request.table_projection.table_name == model.__tablename__
    )
    _require(model is fhir.ProviderDirectorySource or output_models().get(model.__tablename__) is model)
    fhir._assert_artifact_scope_projection(request.scope_projection)
    publisher, driver = await _origin(fhir, outcome)
    source_binding = source._binding_json(request.source_binding, publisher.policy.schema)
    _require(_ATTEMPTED.get(id(outcome)) is None)
    _ATTEMPTED[id(outcome)] = outcome
    if batch.resource_type != "source":
        physical = storage_relation(publisher.policy.schema, source_binding["tables"][source.RESOURCE]["name"])
        _require(
            projection_sql.count(physical) == 1
            and storage_relation(publisher.policy.schema, source.RESOURCE) not in projection_sql
        )
    source_json, batch_json = encoded(source_binding), encoded(asdict(batch))
    params_json = encoded(projection_params)
    await _source(fhir, publisher, source_binding, batch)
    await _target(fhir, publisher, model, relation)
    columns = tuple(str(column.name) for column in model.__table__.columns)
    copied, counts = await _copy_pages(fhir, request, pages, outcome, (publisher, driver), columns)
    _require(
        copied == batch.projected_rows
        and encoded(source_binding) == source_json
        and encoded(asdict(batch)) == batch_json
        and encoded(projection_params) == params_json
    )
    await _source(fhir, publisher, source_binding, batch)
    await _target(fhir, publisher, model, relation)
    await _origin(fhir, outcome)
    _require(encoded(source_binding) == source_json and encoded(projection_params) == params_json)
    witness = NativeCopyWitness(
        batch_json, (publisher.policy.schema, relation), columns, copied, tuple(counts), source_json
    )
    _ISSUED[witness] = (
        asyncio.current_task(),
        outcome,
        encoded(asdict(witness)),
    )
    return witness


def matches(witness, outcome, task):
    """Require the same confirmed exclusive owner; incomplete work stays charged."""
    original = _ISSUED.get(witness) if type(witness) is NativeCopyWitness else None
    measurement = None if outcome is None else outcome.measurement
    return (
        original is not None
        and task is asyncio.current_task()
        and type(outcome) is owned.OwnedWalTransaction
        and original[0] is task
        and original[1] is outcome
        and encoded(asdict(witness)) == original[2]
        and outcome.is_committed
        and outcome.cleanup_complete
        and outcome.status == "committed_measured"
        and isinstance(measurement, BackendWalDiagnosticResult)
        and all(
            counter >= 0 and counter == int(counter)
            for counter in (
                measurement.wal_bytes_delta,
                measurement.wal_records_delta,
                measurement.wal_fpi_delta,
                measurement.global_insert_lsn_span,
            )
        )
    )


def transaction_capture(witness, outcome, task):
    """Report inclusive backend records; physical LSN span is diagnostic only."""
    _require(matches(witness, outcome, task))
    measurement = outcome.measurement
    return dict(
        scope="exclusive_native_copy_transaction",
        semantic_scope=witness.semantic_scope,
        units="native_record_bytes",
        wal_record_bytes=int(measurement.wal_bytes_delta),
        wal_records=measurement.wal_records_delta,
        wal_fpi=measurement.wal_fpi_delta,
        global_physical_lsn_span=measurement.global_insert_lsn_span,
        global_physical_lsn_span_authority="diagnostic_only",
        accounting_authority=False,
        reservation_refund=False,
    )
