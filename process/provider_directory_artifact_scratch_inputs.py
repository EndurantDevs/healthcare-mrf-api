# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed typed artifact inputs under an existing signed preparation owner."""

import asyncio
import hashlib
from contextlib import asynccontextmanager, contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from datetime import datetime
from uuid import uuid4
from weakref import WeakKeyDictionary

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_storage as source_storage
from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_journal as journal
from process import provider_directory_output_snapshot_stage as stages
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_dataset_rehydrate_pages import projected_pages
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session, source_transaction
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    decoded,
    encoded,
    output_models,
    require_output,
    require_output_shape,
)

SOURCE = "provider_directory_source"
TYPED_ROOTS = tuple(
    root
    for root in output_models()
    if root not in {"provider_directory_canonical_resource", "provider_directory_source_resource"}
)
TYPED_ROOTS = tuple(sorted(TYPED_ROOTS))
_SOURCE_CATALOG_SQL = OUTPUT_CATALOG_SQL
_ACTIVE = ContextVar("provider_directory_artifact_scratch_inputs", default=None)
_VERIFIED = ContextVar("provider_directory_artifact_scratch_native_transaction", default=None)
_ISSUED = WeakKeyDictionary()


@dataclass(frozen=True, eq=False)
class ScratchInputs:
    """An issued original-task descriptor; serialized fields alone grant no access."""

    directory: object
    admission: object
    task: object
    operation_json: str
    source_copy_json: str
    source_bindings_json: str
    stages: tuple
    prepared: tuple


def _scope_state(scope):
    """Compare fresh exact values without repeatedly encoding immutable JSON atoms."""

    def snapshot(value):
        """Retain JSON container identity and exact scalar types in immutable tuples."""
        if type(value) is dict:
            return (dict, tuple((key, snapshot(item)) for key, item in sorted(value.items())))
        if type(value) in (list, tuple):
            return (list, tuple(snapshot(item) for item in value))
        require_output(type(value) in (str, int, bool, type(None)), "scratch_scope_required")
        return (type(value), value)

    return snapshot(
        dict(
            operation=scope.operation_json,
            source_copy=scope.source_copy_json,
            source_bindings=scope.source_bindings_json,
            stages=[asdict(stage) for stage in scope.stages],
            prepared=[asdict(receipt) for receipt in scope.prepared],
        )
    )


def _issued_identity(scope, directory):
    require_output(type(scope) is ScratchInputs, "scratch_scope_required")
    original = _ISSUED.get(scope)
    require_output(
        original is not None
        and scope.directory is directory is original[0]
        and scope.admission is original[1]
        and scope.task is original[2]
        and _scope_state(scope) == original[4],
        "scratch_scope_required",
    )
    return original


def _require_issued(scope, directory):
    original = _issued_identity(scope, directory)
    require_output(scope.task is asyncio.current_task(), "scratch_scope_required")
    return original[3]


def _layout_scope(directory):
    """Permit readonly recapture only to the original owner or its issued bounded Source worker."""
    scope = _ACTIVE.get()
    if scope is None:
        return None
    _issued_identity(scope, directory)
    if scope.task is asyncio.current_task():
        return scope
    from process import entity_address_preparation_admission as address_admission
    from process import entity_address_source_preparation as source_preparation

    worker = address_admission._ADMISSION.get()
    require_output(worker is not None, "scratch_scope_required")
    issued = source_preparation.require_building(worker)
    require_output(
        issued is not None
        and asyncio.current_task() in worker.worker_tasks
        and issued.task is scope.task
        and issued.admission is scope.admission
        and not issued.task.done(),
        "scratch_scope_required",
    )
    return scope


def _admitted_identity(directory, admission, fence, resource_types, projection):
    from process import provider_directory_cms_preparation as preparation

    require_output(
        type(admission) is preparation.NonprofileAdmission
        and preparation._ACTIVE.get() is admission
        and admission._started,
        "scratch_admission_required",
    )
    admission._assert_lease()
    execution = directory._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get()
    plan = admission.plan
    require_output(
        bool(fence.datasets)
        and execution is not None
        and plan.selection_proof_id == execution.attestation.proof_id
        and plan.desired_profile_as_of == execution.attestation.desired_profile_as_of
        and plan.desired_fence_hash == preparation.desired_fence_hash(fence)
        and plan.artifact_scope_projection_hash == projection.projection_hash
        and plan.resource_types == tuple(sorted(resource_types)),
        "scratch_plan_changed",
    )
    publish_targets = set(plan.publish_targets)
    require_output(
        bool(publish_targets)
        and publish_targets <= set(directory.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS)
        and resource_types
        == directory._provider_directory_artifact_resource_types(
            publish_targets, publish_corroboration="corroboration" in publish_targets
        ),
        "scratch_targets_changed",
    )
    models_by_root = {model.__tablename__: model for model in directory.RESOURCE_MODELS}
    require_output(
        set(models_by_root) == set(TYPED_ROOTS)
        and all(models_by_root[root] is output_models()[root] for root in TYPED_ROOTS),
        "scratch_model_changed",
    )
    require_output(
        not ({SOURCE, *TYPED_ROOTS} & set(directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get())),
        "scratch_raw_map_refused",
    )
    return dict(
        plan=plan.payload, lease_digest=admission.lease.lease_digest, reservation_id=admission.lease.reservation_id
    )


async def _source_headers(publisher, dataset_ids):
    """Admit small indexed header weights before transporting complete native facts."""
    root = storage_relation(publisher.policy.schema, source_storage.HEADER)
    for offset in range(0, len(dataset_ids), 512):
        remaining_ids = dataset_ids[offset : offset + 512]
        while remaining_ids:
            headers = await publisher.connection.fetch(
                f"SELECT h.dataset_id,octet_length(convert_to(row_to_json(h)::text,'UTF8'))+1024 AS bytes "
                f"FROM {root} h WHERE dataset_id=ANY($1::varchar[]) ORDER BY h.dataset_id",
                remaining_ids,
            )
            selected, size = [], 21
            for header in headers:
                require_output(type(header["bytes"]) is int and header["bytes"] > 0, "scratch_header_size_invalid")
                if size + header["bytes"] > 16 * 1024 * 1024:
                    require_output(bool(selected), "scratch_header_too_large")
                    break
                selected.append(header["dataset_id"])
                size += header["bytes"]
            require_output(bool(selected), "scratch_source_missing")
            header_rows = await publisher.connection.fetch(
                f"SELECT h.dataset_id,to_jsonb(h)::text AS facts FROM {root} h "
                "WHERE dataset_id=ANY($1::varchar[]) ORDER BY dataset_id",
                selected,
            )
            require_output(
                [header_row["dataset_id"] for header_row in header_rows] == selected, "scratch_source_changed"
            )
            facts = [decoded(header_row["facts"]) for header_row in header_rows]
            require_output(
                all(
                    type(fact) is dict and fact.get("dataset_id") == header_row["dataset_id"]
                    for fact, header_row in zip(facts, header_rows, strict=True)
                ),
                "scratch_source_changed",
            )
            yield facts
            remaining_ids = [dataset_id for dataset_id in remaining_ids if dataset_id not in set(selected)]


def _source_pin(dataset, header, binding):
    metadata = binding["identity"]["metadata"]
    require_output(
        dataset.source_id in metadata.get("source_ids", ())
        and header["dataset_hash"] == dataset.dataset_hash
        and header["endpoint_id"] == dataset.endpoint_id
        and header["acquisition_root_run_id"] == dataset.evidence_run_id,
        "scratch_source_changed",
    )
    pin_by_field = dict(
        source_id=dataset.source_id,
        endpoint_id=dataset.endpoint_id,
        dataset_id=dataset.dataset_id,
        acquisition_root_run_id=dataset.evidence_run_id,
        dataset_hash=header["dataset_hash"],
        resource_hash_contract=metadata.get("resource_hash_contract"),
        semantic_projection_as_of=metadata.get("semantic_projection_as_of"),
        publication_metadata_sha256=header["publication_metadata_sha256"],
    )
    require_output(
        all(type(value) is str and value for key, value in pin_by_field.items() if key != "semantic_projection_as_of"),
        "scratch_source_pin_required",
    )
    return pin_by_field


async def _capture_mixed_sources(directory, publisher, fence):
    from process.provider_directory_generic_source_provision import verify_generic_source_family

    await verify_generic_source_family(publisher.connection, publisher.policy)
    await directory._verify_provider_directory_artifact_dataset_fence(fence)
    datasets = tuple(sorted(fence.datasets, key=lambda item: item.source_id))
    require_output(len({dataset.source_id for dataset in datasets}) == len(datasets), "scratch_sources_ambiguous")
    headers_by_id = {}
    ids = sorted({dataset.dataset_id for dataset in datasets})
    async for headers in _source_headers(publisher, ids):
        headers_by_id.update({header["dataset_id"]: header for header in headers})
    require_output(set(headers_by_id) == set(ids), "scratch_source_missing")
    bindings_by_source, facts_by_source, pins = {}, {}, []
    for dataset in datasets:
        header = headers_by_id[dataset.dataset_id]
        binding = decoded(header["candidate_storage_json"])
        require_output(type(binding) is dict, "scratch_source_binding_required")
        source_storage._validate_binding(binding, binding.get("identity"), phases=("published",))
        identity = binding["identity"]
        require_output(
            (identity["dataset_id"], identity["endpoint_id"], identity["acquisition_root_run_id"])
            == (dataset.dataset_id, dataset.endpoint_id, dataset.evidence_run_id),
            "scratch_source_changed",
        )
        metadata = decoded(header["publication_metadata_json"])
        require_output(
            type(metadata) is dict
            and all(metadata.get(key) == expected_fact for key, expected_fact in identity["metadata"].items()),
            "scratch_source_metadata_changed",
        )
        names = sorted(proof["name"] for proof in binding["tables"].values())
        await publisher.connection.execute(
            "LOCK TABLE "
            + ",".join(storage_relation(publisher.policy.schema, name) for name in names)
            + " IN ACCESS SHARE MODE NOWAIT"
        )
        await source_storage._verify_tables(publisher.connection, publisher.policy, binding)
        bindings_by_source[dataset.source_id] = binding
        facts_by_source[dataset.source_id] = dict(
            binding=binding,
            header_sha256=custody._digest(header),
            projection_time={field: header[field] for field in ("published_at", "validated_at", "created_at")},
        )
        pins.append(_source_pin(dataset, header, binding))
    return bindings_by_source, facts_by_source, pins


def typed_input_projection(directory, model, dataset, binding):
    """Keep the existing full typed encoder, selecting only an attested physical child."""
    root = model.__tablename__
    require_output(root in TYPED_ROOTS and output_models()[root] is model, "scratch_model_changed")
    identity = binding["identity"]
    require_output(
        (identity["dataset_id"], identity["endpoint_id"], identity["acquisition_root_run_id"])
        == (dataset.dataset_id, dataset.endpoint_id, dataset.evidence_run_id),
        "scratch_source_changed",
    )
    statement = directory._provider_directory_artifact_resource_select_sql(model, directory._schema())
    parent = storage_relation(directory._schema(), source_storage.RESOURCE)
    child = storage_relation(directory._schema(), binding["tables"][source_storage.RESOURCE]["name"])
    require_output(statement.count(parent) == 1, "scratch_projection_changed")
    columns = ",".join(storage_identifier(str(column.name)) for column in model.__table__.columns)
    return (
        f"WITH incoming({columns}) AS ({statement.replace(parent, child, 1)}) SELECT * FROM incoming",
        dict(
            source_ids=[dataset.source_id],
            dataset_ids=[dataset.dataset_id],
            evidence_run_ids=[dataset.evidence_run_id],
            resource_type=directory.RESOURCE_TYPES_BY_MODEL[model],
        ),
    )


async def _projection_size(directory, statement, params):
    row = await directory.db.first(directory._provider_directory_artifact_projection_count_sql(statement), **params)
    rows, logical_bytes, _cursor = directory._provider_directory_artifact_projection_values(row)
    return rows, logical_bytes


async def _verify_projection_counts(directory, fence, resource_types, projection, bindings):
    expected_by_table = {table.table_name: table for table in projection.tables}
    require_output(
        len(expected_by_table) == len(projection.tables) and set(expected_by_table) == {SOURCE, *TYPED_ROOTS},
        "scratch_projection_changed",
    )
    source_ids = sorted(dataset.source_id for dataset in fence.datasets)
    statement = directory._provider_directory_artifact_source_select_sql(
        directory.ProviderDirectorySource, directory._schema()
    )
    source_sizes = [
        await _projection_size(directory, statement, {"source_ids": source_ids[offset : offset + 512]})
        for offset in range(0, len(source_ids), 512)
    ]
    observed_by_root = {SOURCE: tuple(sum(size[index] for size in source_sizes) for index in (0, 1))}
    for root in TYPED_ROOTS:
        model = output_models()[root]
        selected = directory._artifact_scope_resource_datasets(
            fence, resource_types, directory.RESOURCE_TYPES_BY_MODEL[model]
        )
        sizes = []
        for dataset in selected:
            statement, params = typed_input_projection(directory, model, dataset, bindings[dataset.source_id])
            sizes.append(await _projection_size(directory, statement, params))
        observed_by_root[root] = tuple(sum(size[index] for size in sizes) for index in (0, 1))
    require_output(
        all(
            observed_by_root[root]
            == (expected_by_table[root].projected_rows, expected_by_table[root].projected_logical_bytes)
            for root in expected_by_table
        ),
        "scratch_projection_changed",
    )


async def _source_catalog(session, publisher, names, *, private):
    from process.mrf_address_publication import require_native_read_catalog

    owner, readers = await storage_roles(
        publisher.connection, publisher.policy.schema, **publisher.policy.role_arguments()
    )
    rows = await publisher.connection.fetch(
        _SOURCE_CATALOG_SQL, publisher.policy.schema, list(names), owner, [] if private else readers
    )
    require_output({row["relname"] for row in rows} == set(names), "scratch_source_copy_missing")
    await require_native_read_catalog(
        session,
        tuple(row["oid"] for row in rows),
        executable_roles=(publisher.policy.owner_role, publisher.policy.publisher_role),
    )
    return rows


def _source_copy_shape(directory, catalog_row, original, *, indexed):
    """Only the fixed control model is accepted; its live guards stay on the original."""
    columns = decoded(catalog_row["columns"])
    original_columns = decoded(original["columns"])
    expected_names = {str(column.name) for column in directory.ProviderDirectorySource.__table__.columns}
    require_output(
        {column["name"] for column in columns} == expected_names
        and [(column["name"], column["type"], column["required"]) for column in columns]
        == [(column["name"], column["type"], column["required"]) for column in original_columns],
        "scratch_source_copy_shape_changed",
    )
    topology = decoded(catalog_row["topology"])
    require_output(
        topology["kind"] == "r"
        and topology["persistence"] == "p"
        and not any(topology[key] for key in ("partition", "rls", "force_rls", "parents", "children", "hooks", "rules"))
        and catalog_row["acl_closed"]
        and not decoded(catalog_row["acl"])["columns"]
        and all(
            column["default"] is None and column["identity"] == column["generated"] == "" and column["native_collation"]
            for column in columns
        ),
        "scratch_source_copy_custody_changed",
    )
    constraints = decoded(catalog_row["constraints"])
    require_output(
        all(
            constraint["type"] in {"p", "n", "c"}
            and constraint["validated"]
            and not constraint["deferrable"]
            and not constraint["deferred"]
            for constraint in constraints
        ),
        "scratch_source_copy_constraints_changed",
    )
    checks = lambda catalog: sorted(
        constraint["definition"] for constraint in decoded(catalog["constraints"]) if constraint["type"] == "c"
    )
    require_output(checks(catalog_row) == checks(original), "scratch_source_copy_constraints_changed")
    indexes = decoded(catalog_row["indexes"])
    require_output(
        len(indexes) == int(indexed)
        and sum(constraint["type"] == "p" for constraint in constraints) == int(indexed)
        and all(
            index["primary"]
            and index["unique"]
            and index["keys"] == ["source_id"]
            and index["valid"]
            and index["ready"]
            and index["live"]
            and index["immediate"]
            and index["native"]
            and index["owner"] == catalog_row["relowner"]
            for index in indexes
        ),
        "scratch_source_copy_indexes_changed",
    )


def _catalog_payload(row):
    return {
        key: decoded(value) if key in {"topology", "columns", "constraints", "indexes", "acl"} else value
        for key, value in dict(row).items()
        if key != "descriptor"
    }


async def _source_pages(directory, statement, source_ids):
    """Bound the separate fixed Source-control copy without extending fixed outputs."""
    for offset in range(0, len(source_ids), 512):
        params_by_name = dict(source_ids=source_ids[offset : offset + 512], after_source_id=None)
        while True:
            base = (
                "(" + statement + ") p WHERE (CAST(:after_source_id AS varchar) IS NULL "
                "OR p.source_id>CAST(:after_source_id AS varchar))"
            )
            headers = await directory.db.all(
                "SELECT p.source_id,octet_length(convert_to(row_to_json(p)::text,'UTF8'))+1024 AS record_bytes FROM "
                + base
                + " ORDER BY p.source_id LIMIT 512",
                **params_by_name,
            )
            if not headers:
                break
            selected, size = [], 21
            for header in headers:
                charge = header._mapping["record_bytes"]
                require_output(type(charge) is int and charge > 0, "scratch_source_copy_size_invalid")
                if size + charge > 16 * 1024 * 1024:
                    require_output(bool(selected), "scratch_source_copy_too_large")
                    break
                selected.append(header)
                size += charge
            upper = selected[-1]._mapping["source_id"]
            source_rows = await directory.db.all(
                "SELECT p.* FROM " + base + " AND p.source_id<=:upper_source_id ORDER BY p.source_id",
                **params_by_name,
                upper_source_id=upper,
            )
            require_output(
                len(source_rows) == len(selected) and source_rows[-1]._mapping["source_id"] == upper,
                "scratch_source_copy_changed",
            )
            yield [dict(source_row._mapping) for source_row in source_rows]
            params_by_name["after_source_id"] = upper


async def _copy_source_rows(directory, publisher, name, rows):
    columns = tuple(str(column.name) for column in directory.ProviderDirectorySource.__table__.columns)
    json_columns = directory._json_columns(directory.ProviderDirectorySource.__table__)
    records = (directory._copy_record(row, columns, json_columns) for row in rows)
    copied = 0
    try:
        for batch in stages._record_batches(records):
            status = await publisher.connection.copy_records_to_table(
                name, schema_name=publisher.policy.schema, columns=columns, records=batch
            )
            require_output(status == f"COPY {len(batch)}", "scratch_source_copy_incomplete")
            copied += len(batch)
    except asyncio.CancelledError, TimeoutError:
        from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner

        await close_interrupted_copy(publisher.connection)
        await invalidate_copy_owner(source_copy_session(), publisher.connection)
        raise
    return copied


async def _source_body_digest(connection, relation):
    digest, cursor = hashlib.sha256(), None
    while True:
        headers = await connection.fetch(
            f"SELECT source_id,octet_length(convert_to(to_jsonb(r)::text,'UTF8'))+128 AS bytes "
            f"FROM {relation} r WHERE ($1::varchar IS NULL OR source_id>$1) ORDER BY source_id LIMIT 512",
            cursor,
        )
        if not headers:
            return digest.hexdigest()
        selected, size = [], 21
        for header in headers:
            require_output(type(header["bytes"]) is int and header["bytes"] > 0, "scratch_source_copy_size_invalid")
            if size + header["bytes"] > 16 * 1024 * 1024:
                require_output(bool(selected), "scratch_source_copy_too_large")
                break
            selected.append(header["source_id"])
            size += header["bytes"]
        source_rows = await connection.fetch(
            f"SELECT source_id,to_jsonb(r)::text AS body FROM {relation} r "
            "WHERE ($1::varchar IS NULL OR source_id>$1) AND source_id<=$2 ORDER BY source_id",
            cursor,
            selected[-1],
        )
        require_output(
            [source_row["source_id"] for source_row in source_rows] == selected, "scratch_source_copy_changed"
        )
        for source_row in source_rows:
            body = source_row["body"].encode("utf-8")
            digest.update(len(body).to_bytes(8, "big"))
            digest.update(body)
        cursor = selected[-1]


async def _prepare_source_copy(directory, admission, session, publisher, fence):
    original = (await _source_catalog(session, publisher, [SOURCE], private=False))[0]
    name = "pdsi_" + uuid4().hex
    relation = storage_relation(publisher.policy.schema, name)
    await publisher.connection.execute(
        f"CREATE TABLE {relation} (LIKE {storage_relation(publisher.policy.schema, SOURCE)} "
        "INCLUDING STORAGE INCLUDING CONSTRAINTS)"
    )
    await source_storage._close_grants(publisher.connection, publisher.policy, name)
    created = (await _source_catalog(session, publisher, [name], private=True))[0]
    _source_copy_shape(directory, created, original, indexed=False)
    require_output(
        created["relowner"]
        == await publisher.connection.fetchval(
            "SELECT oid::bigint FROM pg_roles WHERE rolname=$1", publisher.policy.owner_role
        ),
        "scratch_source_copy_owner_changed",
    )
    source_ids = sorted(dataset.source_id for dataset in fence.datasets)
    statement = directory._provider_directory_artifact_source_select_sql(
        directory.ProviderDirectorySource, publisher.policy.schema
    )
    copied = 0
    async for source_rows in _source_pages(directory, statement, source_ids):
        copied += await _copy_source_rows(directory, publisher, name, source_rows)
        await admission.measure(directory, publisher.policy.schema, (name,))
    require_output(copied == len(source_ids), "scratch_source_copy_incomplete")
    await publisher.connection.execute(
        f"ALTER TABLE {relation} ADD CONSTRAINT {storage_identifier(name + '_pk')} PRIMARY KEY(source_id)"
    )
    for offset in range(0, len(source_ids), 512):
        await _verify_set_parity(
            session,
            statement,
            relation,
            {"source_ids": source_ids[offset : offset + 512]},
            actual_filter=" WHERE a.source_id=ANY(CAST(:source_ids AS varchar[]))",
        )
    finished = (await _source_catalog(session, publisher, [name], private=True))[0]
    _source_copy_shape(directory, finished, original, indexed=True)
    return dict(
        name=name,
        oid=finished["oid"],
        original=_catalog_payload(original),
        copy=_catalog_payload(finished),
        rows=copied,
        body_sha256=await _source_body_digest(publisher.connection, relation),
    )


async def _verify_set_parity(session, projection, relation, params, *, actual_filter=""):
    actual = f"SELECT to_jsonb(a) FROM {relation} a{actual_filter}"
    statement = f"WITH expected AS MATERIALIZED ({projection}), difference AS ((SELECT to_jsonb(e) FROM expected e EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL SELECT to_jsonb(e) FROM expected e)) SELECT NOT EXISTS(SELECT FROM difference)"
    require_output((await session.execute(text(statement), params)).scalar_one(), "scratch_tuple_parity_failed")


async def _loading_enrichment(directory, admission, fence, session, projection):
    from process import provider_directory_output_address_key_backfill as address

    targets = sorted(admission.plan.publish_targets)
    decision_at = await bound_publisher().connection.fetchval("SELECT localtimestamp")
    plan_by_field = dict(
        publish_targets=targets,
        selector=dict(run_id=None, source_ids=sorted(fence.source_ids), seen_table=None),
        decision_at=decision_at.isoformat(),
        address_batch_size=512,
        location_enrichment_order=["contact", "coordinate", "address"],
        geo=None,
    )
    native = None
    if "location_address_keys" in targets:
        native = address._native_module()
        plan_by_field["geo"] = await address._geo_snapshot(session, directory._schema())
    return dict(
        plan=plan_by_field,
        projection=projection,
        metrics=dict(
            resource_id_npis_backfilled={},
            location_address_keys_stamped=0,
            location_contacts_backfilled={"location_contact_rows_updated": 0},
            location_coordinates_backfilled=0,
        ),
        native=native,
    )


def _loading_location_projection(directory, statement, params_by_name, enrichment):
    from process.provider_directory_output_contact_backfill import contact_projection
    from process.provider_directory_output_coordinate_backfill import coordinate_projection

    parent = storage_relation(directory._schema(), "provider_directory_location")
    counts = []
    params_by_name = {**params_by_name, "update_timestamp": datetime.fromisoformat(enrichment["plan"]["decision_at"])}
    for kind, target in (("contact", "location_contacts"), ("coordinate", "location_coordinates")):
        if target not in enrichment["plan"]["publish_targets"]:
            continue
        if kind == "contact":
            projection, count = contact_projection(directory)
        else:
            projection, count, selection_params = coordinate_projection(directory, dict(run_id=None, source_ids=None))
            params_by_name = {**params_by_name, **selection_params}
        require_output(projection.count(parent) == count.count(parent) == 1, "scratch_projection_changed")
        counts.append((kind, count.replace(parent, "(" + statement + ")", 1)))
        statement = projection.replace(parent, "(" + statement + ")", 1)
    return statement, params_by_name, counts


async def _record_loading_location_counts(capability, counts, params, enrichment):
    for kind, statement in counts:
        count = (await capability.session.execute(text(statement), params)).scalar_one()
        require_output(type(count) is int and count >= 0, "scratch_metrics_changed")
        if kind == "contact":
            enrichment["metrics"]["location_contacts_backfilled"]["location_contact_rows_updated"] += count
        else:
            enrichment["metrics"]["location_coordinates_backfilled"] += count


def _loading_projection(directory, target_root, statement, params, enrichment):
    from process import provider_directory_output_address_key_backfill as address
    from process import provider_directory_output_npi_backfill as npi

    publish_targets = set(enrichment["plan"]["publish_targets"])
    counts = []
    if target_root == "provider_directory_location":
        statement, params, counts = _loading_location_projection(directory, statement, params, enrichment)
    selector_by_field = dict(run_id=None, source_ids=None)
    if target_root in npi.TARGETS and publish_targets & {
        "resource_id_npis",
        "location_archive",
        "address_overlay",
        "profile",
        "corroboration",
    }:
        inputs, expected, _count, repair_params = npi.npi_projection(directory, target_root, selector_by_field)
        kind = "npi"
    elif target_root == address.TARGET and "location_address_keys" in publish_targets:
        inputs, repair_params = address.address_projection(directory, selector_by_field)
        expected, kind = statement, "address"
    else:
        return statement, statement, params, None, counts
    parent = storage_relation(directory._schema(), target_root)
    require_output(inputs.count(parent) == 1, "scratch_projection_changed")
    inputs = inputs.replace(parent, "(" + statement + ")", 1)
    if kind == "npi":
        require_output(expected.count(parent) == 1, "scratch_projection_changed")
        expected = expected.replace(parent, "(" + statement + ")", 1)
    return (
        inputs,
        expected,
        {**params, **repair_params, "update_timestamp": datetime.fromisoformat(enrichment["plan"]["decision_at"])},
        kind,
        counts,
    )


async def _repair_loading_rows(directory, rows, kind, enrichment):
    from process import provider_directory_output_address_key_backfill as address
    from process import provider_directory_output_npi_backfill as npi

    decision_at = datetime.fromisoformat(enrichment["plan"]["decision_at"])
    if kind == "npi":
        repaired = npi.repair_npi_rows(rows, decision_at)
        return repaired, sum(row["repair_eligible"] for row in rows)
    if kind == "address":
        canonical = await address._canonical_page(directory, enrichment["native"], rows, 512)
        return address.repair_address_rows(directory, rows, canonical, decision_at)
    return rows, 0


async def _fill_typed_input(directory, capability, admission, stage, selected_datasets, bindings, enrichment):
    from process.provider_directory_output_address_key_backfill import _digest_rows

    model, copied, digest = output_models()[stage.target], 0, hashlib.sha256()
    changed = 0
    for dataset in selected_datasets:
        statement, params = typed_input_projection(directory, model, dataset, bindings[dataset.source_id])
        inputs, _expected, params, kind, counts = _loading_projection(
            directory, stage.target, statement, params, enrichment
        )
        await _record_loading_location_counts(capability, counts, params, enrichment)
        async for rows in projected_pages(directory.db, inputs, params, model):
            repaired, count = await _repair_loading_rows(directory, rows, kind, enrichment)
            if (
                stage.target == "provider_directory_location"
                and "location_address_keys" in enrichment["plan"]["publish_targets"]
            ):
                _digest_rows(digest, repaired)
            copied += await stages.copy_output_rows(directory, capability, stage, repaired)
            changed += count
            await journal.record_output_progress(capability, stage, copied)
            await admission.measure(directory, stage.schema)
    if stage.target in {"provider_directory_organization", "provider_directory_practitioner"}:
        resource = directory.RESOURCE_TYPES_BY_MODEL[model]
        enrichment["metrics"]["resource_id_npis_backfilled"][resource] = changed
    elif stage.target == "provider_directory_location":
        enrichment["metrics"]["location_address_keys_stamped"] = changed
    return copied, digest.hexdigest()


async def _verify_typed_parity(directory, capability, stage, selected_datasets, bindings, enrichment):
    from process.provider_directory_output_address_key_backfill import ADDRESS_FIELDS

    model = output_models()[stage.target]
    relation = storage_relation(stage.schema, stage.relation)
    for dataset in selected_datasets:
        statement, params = typed_input_projection(directory, model, dataset, bindings[dataset.source_id])
        _inputs, expected, params, kind, _counts = _loading_projection(
            directory, stage.target, statement, params, enrichment
        )
        actual = relation
        if kind == "address":
            columns = ",".join(
                storage_identifier(str(column.name))
                for column in model.__table__.columns
                if column.name not in (*ADDRESS_FIELDS, "updated_at")
            )
            expected = f"SELECT {columns} FROM ({expected}) preserved"
            actual = f"(SELECT {columns} FROM {relation})"
        await _verify_set_parity(
            capability.session,
            expected,
            actual,
            params,
            actual_filter=" WHERE a.source_id=ANY(CAST(:source_ids AS varchar[]))",
        )


async def _verify_typed_digest(directory, stage, selected_datasets, expected, copied):
    from process.provider_directory_output_address_key_backfill import _digest_rows

    digest, count = hashlib.sha256(), 0
    for dataset in selected_datasets:
        async for rows in projected_pages(
            directory.db,
            f"SELECT * FROM {storage_relation(stage.schema, stage.relation)} WHERE source_id=CAST(:source_id AS varchar)",
            {"source_id": dataset.source_id},
            output_models()[stage.target],
        ):
            _digest_rows(digest, rows)
            count += len(rows)
    require_output(count == copied and digest.hexdigest() == expected, "scratch_enrichment_parity_failed")


async def _prepare_typed_input(
    directory, capability, admission, fence, resource_types, bindings, target_root, enrichment
):
    model = output_models()[target_root]
    stage = await stages.prepare_output_stage(directory, capability, target_root)
    await admission.measure(directory, stage.schema, (stage.relation,))
    await journal.record_output_progress(capability, stage, 0)
    selected_datasets = tuple(
        sorted(
            directory._artifact_scope_resource_datasets(
                fence, resource_types, directory.RESOURCE_TYPES_BY_MODEL[model]
            ),
            key=lambda dataset: dataset.source_id,
        )
    )
    copied, digest = await _fill_typed_input(
        directory, capability, admission, stage, selected_datasets, bindings, enrichment
    )
    expected_by_table = {table.table_name: table for table in enrichment["projection"].tables}
    require_output(
        set(expected_by_table) == {SOURCE, *TYPED_ROOTS} and copied == expected_by_table[target_root].projected_rows,
        "scratch_projection_count_changed",
    )
    await stages.finish_output_indexes(directory, capability, stage)
    await _verify_typed_parity(directory, capability, stage, selected_datasets, bindings, enrichment)
    if (
        target_root == "provider_directory_location"
        and "location_address_keys" in enrichment["plan"]["publish_targets"]
    ):
        await _verify_typed_digest(directory, stage, selected_datasets, digest, copied)
    relation = storage_relation(stage.schema, stage.relation)
    require_output(
        await capability.connection.fetchval(
            f"SELECT NOT EXISTS(SELECT FROM {relation} WHERE source_id<>ALL($1::varchar[]))",
            [dataset.source_id for dataset in selected_datasets],
        ),
        "scratch_extra_source_rows",
    )
    await stages.seal_output_stage(capability, stage, expected_rows=copied)
    prepared = await custody.capture_prepared_output(
        capability, target_root, stage.relation, expected_incumbent=stage.incumbent
    )
    return stage, prepared


def _membership(source_copy, operation, stages_by_root, prepared):
    return dict(
        contract="artifact-scratch-inputs-v1",
        phase="prepared",
        operation=operation,
        source_copy=source_copy,
        stages={root: stage.to_payload() for root, stage in stages_by_root.items()},
        prepared=[receipt.to_payload() for receipt in prepared],
    )


async def _store_membership(publisher, source_copy, operation, stages_by_root, prepared):
    descriptor = _membership(source_copy, operation, stages_by_root, prepared)
    require_output(len(encoded(descriptor).encode("utf-8")) <= 16 * 1024 * 1024, "scratch_descriptor_too_large")
    statement = await publisher.connection.fetchval(
        "SELECT format('COMMENT ON TABLE %I.%I IS %L',$1::text,$2::text,$3::text)",
        publisher.policy.schema,
        source_copy["name"],
        encoded(descriptor),
    )
    await publisher.connection.execute(statement)


async def prepare_scratch_inputs(directory, admission, fence, resource_types, projection):
    """Prepare the complete family atomically; existing retries require fresh admission."""
    from process import provider_directory_cms_preparation as preparation

    identity = _admitted_identity(directory, admission, fence, resource_types, projection)
    async with source_transaction(directory.db, isolation="repeatable read") as session:
        publisher = bound_publisher()
        require_output(directory._schema() == publisher.policy.schema, "schema_changed")
        async with preparation.nonprofile_sql_transaction(directory, admission):
            await admission._check("readiness")
            bindings, source_facts, pins = await _capture_mixed_sources(directory, publisher, fence)
            require_output(len(encoded(source_facts).encode("utf-8")) <= 16 * 1024 * 1024, "scratch_bindings_too_large")
            await _verify_projection_counts(directory, fence, resource_types, projection, bindings)
            source_copy = await _prepare_source_copy(directory, admission, session, publisher, fence)
            source_copy["admission_identity"] = identity
            enrichment = await _loading_enrichment(directory, admission, fence, session, projection)
            source_copy["enrichment_plan"] = enrichment["plan"]
            operation_by_field = dict(
                kind="source_projection",
                run_id="scratch-" + uuid4().hex,
                producer="artifact-scratch-v1",
                source_pins=pins,
                resource_types=sorted(resource_types),
                source_fence_digest=custody._digest(
                    dict(identity=identity, source_facts=source_facts, source_copy=source_copy)
                ),
            )
            stages_by_root, prepared = {}, []
            async with custody.output_snapshot_transaction(
                directory, operation=operation_by_field, targets=TYPED_ROOTS
            ) as capability:
                for root in TYPED_ROOTS:
                    stage, receipt = await _prepare_typed_input(
                        directory, capability, admission, fence, resource_types, bindings, root, enrichment
                    )
                    stages_by_root[root] = stage
                    prepared.append(receipt)
                source_copy["enrichment_metrics"] = enrichment["metrics"]
                await _store_membership(publisher, source_copy, operation_by_field, stages_by_root, prepared)
                await journal.record_output_prepared(capability, prepared)
                await admission.measure(directory, publisher.policy.schema)
            scratch_inputs = ScratchInputs(
                directory,
                admission,
                asyncio.current_task(),
                encoded(operation_by_field),
                encoded(source_copy),
                encoded(source_facts),
                tuple(stages_by_root.values()),
                tuple(prepared),
            )
    _ISSUED[scratch_inputs] = (
        directory,
        admission,
        scratch_inputs.task,
        encoded(identity),
        _scope_state(scratch_inputs),
    )
    return scratch_inputs


async def _verify_membership(directory, scope, capability):
    identity_json = _require_issued(scope, directory)
    await _verify_native_membership(directory, scope, capability, identity_json)


async def _verify_native_membership(directory, scope, capability, identity_json):
    source_copy = decoded(scope.source_copy_json)
    operation = decoded(scope.operation_json)
    scope.admission._assert_lease()
    require_output(
        encoded(
            dict(
                plan=scope.admission.plan.payload,
                lease_digest=scope.admission.lease.lease_digest,
                reservation_id=scope.admission.lease.reservation_id,
            )
        )
        == identity_json,
        "scratch_plan_changed",
    )
    catalog_rows = await _source_catalog(capability.session, bound_publisher(), [source_copy["name"]], private=True)
    catalog_row = catalog_rows[0]
    _source_copy_shape(directory, catalog_row, source_copy["original"], indexed=True)
    require_output(
        _catalog_payload(catalog_row) == source_copy["copy"]
        and await _source_body_digest(
            capability.connection, storage_relation(capability.policy.schema, source_copy["name"])
        )
        == source_copy["body_sha256"],
        "scratch_source_copy_changed",
    )
    expected = _membership(source_copy, operation, {stage.target: stage for stage in scope.stages}, scope.prepared)
    require_output(decoded(catalog_row["descriptor"]) == expected, "scratch_membership_changed")
    prior = await journal._plan(capability)
    entry = journal._journal(prior)["operations"].get(capability.operation_digest)
    require_output(
        entry is not None
        and entry["phase"] == "prepared"
        and entry["prepared"] == [receipt.to_payload() for receipt in scope.prepared],
        "scratch_journal_changed",
    )
    for stage, receipt in zip(scope.stages, scope.prepared, strict=True):
        await stages._stage_row(capability, stage, phases={"sealed"})
        require_output(
            await custody.capture_prepared_output(
                capability, stage.target, stage.relation, expected_incumbent=stage.incumbent
            )
            == receipt,
            "scratch_stage_changed",
        )


async def discard_scratch_inputs(directory, scope):
    """Remove only the issued native family, on its real protected Source owner."""
    _require_issued(scope, directory)
    async with source_transaction(directory.db, isolation="repeatable read"):
        async with custody.output_snapshot_transaction(
            directory, operation=decoded(scope.operation_json), targets=TYPED_ROOTS
        ) as capability:
            source_copy = decoded(scope.source_copy_json)
            names = [source_copy["name"], *(stage.relation for stage in scope.stages)]
            await capability.connection.execute(
                "LOCK TABLE "
                + ",".join(storage_relation(capability.policy.schema, name) for name in sorted(names))
                + " IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
            catalog_rows = await _source_catalog(
                capability.session, bound_publisher(), [source_copy["name"]], private=True
            )
            require_output(
                _catalog_payload(catalog_rows[0]) == source_copy["copy"]
                and decoded(catalog_rows[0]["descriptor"])
                == _membership(
                    source_copy,
                    decoded(scope.operation_json),
                    {stage.target: stage for stage in scope.stages},
                    scope.prepared,
                ),
                "scratch_membership_changed",
            )
            for stage in scope.stages:
                await stages._stage_row(capability, stage, phases={"sealed"})
            for stage in reversed(scope.stages):
                await stages.discard_output_stage(capability, stage)
            await capability.connection.execute(
                f"DROP TABLE {storage_relation(capability.policy.schema, source_copy['name'])}"
            )
    del _ISSUED[scope]


@asynccontextmanager
async def scratch_input_transaction(directory, scope):
    """Recheck issued native membership on a genuine original Source transaction."""
    _require_issued(scope, directory)
    active_scope = _ACTIVE.get()
    require_output(active_scope is None or active_scope is scope, "scratch_scope_required")
    async with source_transaction(directory.db, isolation="repeatable read"):
        async with custody.output_snapshot_transaction(
            directory, operation=decoded(scope.operation_json), targets=TYPED_ROOTS
        ) as capability:
            await _verify_membership(directory, scope, capability)
            token = _VERIFIED.set(capability)
            scope_context = nullcontext() if active_scope is scope else scratch_input_scope(scope)
            try:
                with scope_context:
                    yield capability
            finally:
                _VERIFIED.reset(token)


async def scratch_consumer_projection(directory, kind, target, *, run_id=None, source_ids=None, seen_table=None):
    """Return only a fixed enrichment SELECT inside its verified original Source body."""
    require_output(seen_table is None, "scratch_seen_custody_required")
    scope = active_scratch_inputs(directory)
    capability = _VERIFIED.get()
    require_output(scope is not None and capability is not None, "scratch_native_scope_required")
    await custody._require_capability(capability)
    _require_consumer_target(kind, target)
    selector_by_name = dict(run_id=run_id, source_ids=sorted(set(directory._clean_source_id_list(source_ids))) or None)
    require_output(run_id is None or type(run_id) is str and bool(run_id), "scratch_run_invalid")
    stage = next(stage for stage in scope.stages if stage.target == target)
    await stages._stage_row(capability, stage, phases={"sealed"})
    statement, count, params, secondary = await _consumer_sql(directory, capability, kind, target, selector_by_name)
    parent, child = storage_relation(stage.schema, target), storage_relation(stage.schema, stage.relation)
    require_output(
        statement.count(parent) == 1 and (count is None or count.count(parent) == 1), "scratch_projection_changed"
    )
    return dict(
        statement=statement.replace(parent, child, 1),
        count=None if count is None else count.replace(parent, child, 1),
        params=params,
        secondary=secondary,
        stage=stage,
    )


def _require_consumer_target(kind, target_root):
    require_output(
        kind in {"contact", "coordinate", "npi", "address"}
        and target_root
        in (
            {"provider_directory_organization", "provider_directory_practitioner"}
            if kind == "npi"
            else {"provider_directory_location"}
        ),
        "scratch_consumer_invalid",
    )


async def _consumer_sql(directory, capability, kind, target, selector):
    if kind == "contact":
        from process.provider_directory_output_contact_backfill import contact_projection

        statement, count = contact_projection(directory)
        return statement, count, {}, None
    if kind == "coordinate":
        from process.provider_directory_output_coordinate_backfill import coordinate_projection

        statement, count, params = coordinate_projection(directory, selector)
        return statement, count, params, None
    if kind == "npi":
        from process.provider_directory_output_npi_backfill import npi_projection

        _inputs, statement, count, params = npi_projection(directory, target, selector)
        return statement, count, params, None
    from process.provider_directory_output_address_key_backfill import _geo_snapshot, _native_module, address_projection

    _native_module()
    secondary = await _geo_snapshot(capability.session, capability.policy.schema)
    statement, params = address_projection(directory, selector)
    return statement, None, params, secondary


def active_scratch_inputs(directory):
    """Return only the active issued original-task scope, never a raw override map."""
    scope = _ACTIVE.get()
    if scope is None:
        return None
    _require_issued(scope, directory)
    return scope


@contextmanager
def scratch_input_scope(scope):
    """Expose issued membership only; actual consumers must recheck native storage before IO."""
    _require_issued(scope, scope.directory)
    require_output(_ACTIVE.get() is None, "scratch_scope_required")
    token = _ACTIVE.set(scope)
    try:
        yield scope
    finally:
        _ACTIVE.reset(token)


def _prepared_enrichment_metric(source_copy, kind):
    metric_name_by_kind = dict(
        npi="resource_id_npis_backfilled",
        address="location_address_keys_stamped",
        contact="location_contacts_backfilled",
        coordinate="location_coordinates_backfilled",
    )
    prepared_metric = source_copy.get("enrichment_metrics", {}).get(metric_name_by_kind[kind])
    if kind in {"npi", "contact"}:
        fields = {"Organization", "Practitioner"} if kind == "npi" else {"location_contact_rows_updated"}
        is_valid = (
            type(prepared_metric) is dict
            and set(prepared_metric) == fields
            and all(type(count) is int and count >= 0 for count in prepared_metric.values())
        )
    else:
        is_valid = type(prepared_metric) is int and prepared_metric >= 0
    require_output(is_valid, "scratch_metrics_changed")
    return dict(prepared_metric) if type(prepared_metric) is dict else prepared_metric


async def prepared_scratch_enrichment(
    directory, kind, *, db_schema=None, run_id=None, source_ids=None, seen_table=None, batch_size=None
):
    """Read prepared metrics only after reattesting the issued original-task family."""
    scope = active_scratch_inputs(directory)
    if scope is None:
        return None
    source_copy = decoded(scope.source_copy_json)
    plan_by_field = source_copy.get("enrichment_plan")
    require_output(
        type(plan_by_field) is dict and kind in {"npi", "address", "contact", "coordinate"},
        "scratch_enrichment_required",
    )
    require_output(db_schema is None or db_schema == directory._schema(), "schema_changed")
    require_output(
        run_id is plan_by_field["selector"]["run_id"] is None and seen_table is None, "scratch_selector_changed"
    )
    selected_sources = sorted(set(directory._clean_source_id_list(source_ids)))
    require_output(
        not selected_sources or selected_sources == plan_by_field["selector"]["source_ids"], "scratch_selector_changed"
    )
    require_output(
        batch_size is None
        or kind == "address"
        and type(batch_size) is int
        and batch_size >= plan_by_field["address_batch_size"],
        "scratch_batch_changed",
    )
    publish_targets = set(plan_by_field["publish_targets"])
    require_output(
        bool(publish_targets & {"resource_id_npis", "location_archive", "address_overlay", "profile", "corroboration"})
        if kind == "npi"
        else {"address": "location_address_keys", "contact": "location_contacts", "coordinate": "location_coordinates"}[
            kind
        ]
        in publish_targets,
        "scratch_targets_changed",
    )
    async with scratch_input_transaction(directory, scope) as capability:
        if kind == "address":
            from process.provider_directory_output_address_key_backfill import _geo_snapshot

            require_output(
                plan_by_field["geo"] is not None
                and await _geo_snapshot(capability.session, capability.policy.schema) == plan_by_field["geo"],
                "address_geo_changed",
            )
        return _prepared_enrichment_metric(source_copy, kind)


async def fence_scratch_enrichment(directory):
    """Fence the recorded ZIP origin only on the existing final Source owner."""
    scope = active_scratch_inputs(directory)
    if scope is None:
        return
    binding = directory.db._transaction_binding()
    require_output(binding is not None, "scratch_original_cut_required")
    require_output(bound_publisher().transaction_isolation == "read committed", "scratch_original_cut_required")
    source_copy = decoded(scope.source_copy_json)
    geo = source_copy.get("enrichment_plan", {}).get("geo")
    if geo is None:
        return
    async with custody.output_snapshot_transaction(
        directory, operation=decoded(scope.operation_json), targets=TYPED_ROOTS, isolation="read committed"
    ) as capability:
        await _verify_membership(directory, scope, capability)
        require_output(capability.session is binding.session, "scratch_original_cut_changed")
        from process.provider_directory_output_address_key_backfill import _lock_geo_cut

        await _lock_geo_cut(capability, geo)


async def scratch_input_metadata(directory, scope):
    """Return navigation identities only after native original-task membership verification."""
    async with scratch_input_transaction(directory, scope):
        source_copy = decoded(scope.source_copy_json)
        relation_by_table = {SOURCE: source_copy["name"], **{stage.target: stage.relation for stage in scope.stages}}
        oids_by_table = {SOURCE: source_copy["copy"]["oid"], **{stage.target: stage.oid for stage in scope.stages}}
        require_output(
            set(relation_by_table) == set(oids_by_table) == {SOURCE, *TYPED_ROOTS}
            and all(type(oid) is int and oid > 0 for oid in oids_by_table.values()),
            "scratch_membership_changed",
        )
        return dict(relation_by_table=relation_by_table, relation_oids=oids_by_table)


def _scratch_relation_member(scope, relation):
    """Match the original issued stage before selecting its catalog executor."""
    matches = [
        (stage, receipt.successor)
        for stage, receipt in zip(scope.stages, scope.prepared, strict=True)
        if stage.relation == relation.relation
    ]
    if not matches:
        return None
    require_output(len(matches) == 1, "scratch_stage_changed")
    stage, expected = matches[0]
    require_output(expected.target == stage.target and stage.target in TYPED_ROOTS, "scratch_stage_changed")
    require_output(
        (relation.schema, relation.oid, relation.persistence) == (stage.schema, stage.oid, "p"), "scratch_stage_changed"
    )
    return stage, expected


def _scratch_catalog_statement():
    """Use the same complete native catalog for single and grouped recapture."""
    statement = OUTPUT_CATALOG_SQL
    for parameter, replacement in (
        ("$1", "CAST(:schema AS text)"),
        ("$2", "CAST(:names AS text[])"),
        ("$3", "CAST(:owner AS oid)"),
        ("$4", "CAST(:readers AS oid[])"),
    ):
        statement = statement.replace(parameter, replacement)
    return statement


def _assert_scratch_catalog_row(directory, expected, catalog_row):
    """Retain every original shape, ACL and exact native receipt check."""
    require_output(catalog_row is not None, "scratch_stage_changed")
    catalog_row = directory._pagination_checkpoint_row_mapping(catalog_row)
    require_output(catalog_row["acl_closed"] and catalog_row["readers_closed"], "scratch_stage_changed")
    require_output_shape(catalog_row, expected.target)
    require_output(custody._native_receipt(expected, catalog_row, expected.target) == expected, "scratch_stage_changed")


async def scratch_relation_proof(directory, relation, *, database=None):
    """Recheck only an issued sealed stage; a generated name never grants layout authority."""
    scope = _layout_scope(directory)
    member = None if scope is None else _scratch_relation_member(scope, relation)
    if member is None:
        return None
    stage, expected = member
    executor = directory.db if database is None else database
    catalog_row = await executor.first(
        _scratch_catalog_statement(), schema=stage.schema, names=[stage.relation], owner=expected.owner_oid, readers=[]
    )
    _assert_scratch_catalog_row(directory, expected, catalog_row)
    require_output(_layout_scope(directory) is scope, "scratch_scope_required")
    return expected


def _scratch_relation_groups(directory, scope, relations, database_for):
    """Group only members sharing their exact executor, schema and admitted owner."""
    groups, coordinates = {}, set()
    for relation in relations:
        member = _scratch_relation_member(scope, relation)
        if member is None:
            continue
        stage, expected = member
        coordinate = (relation.schema, relation.relation)
        require_output(coordinate not in coordinates, "scratch_stage_changed")
        coordinates.add(coordinate)
        database = None if database_for is None else database_for(relation)
        executor = directory.db if database is None else database
        key = (id(executor), stage.schema, expected.owner_oid)
        group = groups.setdefault(key, (database, executor, {}))
        group[2][stage.relation] = (relation, expected)
    return groups


async def _capture_scratch_group(directory, scope, key, group):
    """Freshly recapture every grouped member at this original proof boundary."""
    require_output(_layout_scope(directory) is scope, "scratch_scope_required")
    _database, executor, members = group
    _actor, schema, owner = key
    rows = await executor.all(_scratch_catalog_statement(), schema=schema, names=list(members), owner=owner, readers=[])
    observed_names = set()
    for row in rows:
        row = directory._pagination_checkpoint_row_mapping(row)
        name = row.get("relname")
        require_output(type(name) is str and name in members and name not in observed_names, "scratch_stage_changed")
        observed_names.add(name)
        _assert_scratch_catalog_row(directory, members[name][1], row)
    require_output(observed_names == set(members), "scratch_stage_changed")
    require_output(_layout_scope(directory) is scope, "scratch_scope_required")


@asynccontextmanager
async def scratch_relation_batch(directory, relations, *, database_for=None):
    """Fence the full pass with fresh same-actor proofs, including negative membership."""
    scope = _layout_scope(directory)
    groups = {} if scope is None else _scratch_relation_groups(directory, scope, relations, database_for)
    proof_by_coordinate = {}
    try:
        for key, group in groups.items():
            await _capture_scratch_group(directory, scope, key, group)
            database, _executor, members = group
            proof_by_coordinate.update(
                {(relation.schema, relation.relation): (expected, database) for relation, expected in members.values()}
            )
        yield proof_by_coordinate
        for key, group in groups.items():
            await _capture_scratch_group(directory, scope, key, group)
        require_output(_layout_scope(directory) is scope, "scratch_scope_required")
    finally:
        proof_by_coordinate.clear()
