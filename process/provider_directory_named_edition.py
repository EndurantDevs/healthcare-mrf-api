# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Locate immutable Source editions through the catalog pointer."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace

from db.plan_release_storage import storage_identifier, storage_relation

POINTER_SCHEMA = "provider_directory_editions"
POINTER_TABLE = "current_edition"
HEADER = "provider_directory_endpoint_dataset"
PAYLOAD_TABLES = frozenset(
    (
        "provider_directory_dataset_resource",
        "provider_directory_dataset_insurance_plan",
        "provider_directory_dataset_network_plan",
        "provider_directory_dataset_affiliation_organization",
    )
)


@dataclass(frozen=True)
class NamedDirectoryEdition:
    catalog_schema: str
    endpoint_id: str
    dataset_id: str
    schema_name: str
    source_ids: tuple[str, ...]
    header: dict


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_named_edition_invalid")


def _text(value, maximum=128):
    return isinstance(value, str) and 0 < len(value) <= maximum and value.strip() == value and value.isprintable()


def edition_schema_name(dataset_id):
    """Derive a bounded SQL name from the logical dataset identifier."""
    return "directory_edition_" + hashlib.sha256(dataset_id.encode()).hexdigest()[:32]


async def current_edition(connection, catalog_schema, endpoint_id):
    """Resolve the edition in the caller's read transaction."""
    pointer = storage_relation(POINTER_SCHEMA, POINTER_TABLE)
    return await connection.fetchrow(
        f"SELECT dataset_id,schema_name,previous_dataset_id,previous_schema_name FROM {pointer} "
        "WHERE catalog_schema=$1 AND endpoint_id=$2",
        catalog_schema,
        endpoint_id,
    )


async def find_current_edition(connection, catalog_schema, endpoint_id):
    """Allow existing installations to read legacy catalog editions before provisioning."""
    pointer = storage_relation(POINTER_SCHEMA, POINTER_TABLE)
    if await connection.fetchval("SELECT to_regclass($1)::text", pointer) is None:
        return None
    return await current_edition(connection, catalog_schema, endpoint_id)


async def read_edition_header(connection, schema, dataset_id):
    """Read one immutable edition without consulting database-local metadata."""
    header = storage_relation(schema, HEADER)
    return await connection.fetchrow(
        f"SELECT * FROM {header} WHERE dataset_id=$1 AND status='published' AND published_at IS NOT NULL",
        dataset_id,
    )


async def named_current_editions(connection, catalog_schema, source_ids=None):
    """Resolve logical headers once in the caller's read transaction."""
    _require(
        source_ids is None or not isinstance(source_ids, str) and all(_text(source_id, 96) for source_id in source_ids)
    )
    pointer_table = storage_relation(POINTER_SCHEMA, POINTER_TABLE)
    if await connection.fetchval("SELECT to_regclass($1)::text", pointer_table) is None:
        return ()
    source_table = storage_relation(catalog_schema, "provider_directory_source")
    pointer_records = await connection.fetch(
        f"SELECT p.endpoint_id,p.dataset_id,p.schema_name,p.previous_dataset_id,p.previous_schema_name,"
        f"array_agg(s.source_id ORDER BY s.source_id) AS aliases FROM {pointer_table} p JOIN {source_table} s "
        "ON s.endpoint_id=p.endpoint_id WHERE p.catalog_schema=$1 AND ($2::text[] IS NULL OR EXISTS("
        f"SELECT FROM {source_table} requested WHERE requested.endpoint_id=p.endpoint_id "
        "AND requested.source_id=ANY($2::text[]))) GROUP BY p.endpoint_id,p.dataset_id,p.schema_name,"
        "p.previous_dataset_id,p.previous_schema_name ORDER BY p.endpoint_id",
        catalog_schema,
        None if source_ids is None else sorted(set(source_ids)),
    )
    editions = []
    for pointer_record in pointer_records:
        editions.append(await _read_named_edition(connection, catalog_schema, pointer_record))
    _require(len({edition.endpoint_id for edition in editions}) == len(editions))
    return tuple(editions)


async def _read_named_edition(connection, catalog_schema, pointer_record):
    """Validate one logical pointer and its immutable published header."""
    endpoint_id = pointer_record["endpoint_id"]
    dataset_id = pointer_record["dataset_id"]
    schema_name = pointer_record["schema_name"]
    _require(_text(endpoint_id) and _text(dataset_id, 96) and _text(schema_name, 63))
    storage_identifier(schema_name)
    _require(schema_name not in {"public", "information_schema"} and not schema_name.startswith("pg_"))
    _require((pointer_record["previous_dataset_id"] is None) == (pointer_record["previous_schema_name"] is None))
    if pointer_record["previous_schema_name"] is not None:
        _require(_text(pointer_record["previous_dataset_id"], 96))
        storage_identifier(pointer_record["previous_schema_name"])
    header_record = await read_edition_header(connection, schema_name, dataset_id)
    _require(header_record is not None)
    header_by_field = dict(header_record)
    publication_metadata = header_by_field.get("publication_metadata_json")
    if isinstance(publication_metadata, str):
        try:
            publication_metadata = json.loads(publication_metadata)
        except ValueError, RecursionError:
            raise RuntimeError("provider_directory_named_edition_invalid") from None
    header_source_ids = publication_metadata.get("source_ids") if isinstance(publication_metadata, dict) else None
    _require(
        header_by_field.get("endpoint_id") == endpoint_id
        and header_by_field.get("dataset_id") == dataset_id
        and header_by_field.get("status") == "published"
        and header_by_field.get("is_current") is True
        and header_by_field.get("published_at") is not None
        and header_by_field.get("superseded_at") is None
        and _text(header_by_field.get("acquisition_root_run_id"), 64)
        and isinstance(header_by_field.get("dataset_hash"), str)
        and len(header_by_field["dataset_hash"]) == 64
        and all(character in "0123456789abcdef" for character in header_by_field["dataset_hash"])
        and type(header_by_field.get("resource_count")) is int
        and header_by_field["resource_count"] >= 0
        and isinstance(header_source_ids, list)
        and bool(header_source_ids)
        and all(_text(source_id, 96) for source_id in header_source_ids)
        and len(header_source_ids) == len(set(header_source_ids))
        and set(header_source_ids).issubset(pointer_record["aliases"])
    )
    header_by_field["publication_metadata_json"] = publication_metadata
    return NamedDirectoryEdition(
        catalog_schema, endpoint_id, dataset_id, schema_name, tuple(sorted(header_source_ids)), header_by_field
    )


async def named_editions_for_session(session, catalog_schema, source_ids=None):
    """Use the existing caller's read transaction for standalone payload loaders."""
    from process.provider_directory_projection_stage import _copy_driver

    return await named_current_editions(await _copy_driver(session), catalog_schema, source_ids)


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def source_registration_sql(catalog_schema, editions):
    """Keep registrations catalog-bound and named data on its exact source aliases."""
    relation = storage_relation(catalog_schema, "provider_directory_source")
    if not editions:
        return relation
    endpoints = ",".join(_literal(edition.endpoint_id) for edition in editions)
    predicates = [f"endpoint_id IS NULL OR endpoint_id NOT IN ({endpoints})"]
    predicates.extend(
        "(endpoint_id="
        + _literal(edition.endpoint_id)
        + " AND source_id IN ("
        + ",".join(_literal(source_id) for source_id in edition.source_ids)
        + "))"
        for edition in editions
    )
    return "(SELECT * FROM " + relation + " WHERE " + " OR ".join(predicates) + ")"


def source_relation_sql(catalog_schema, table_name, editions=(), *, columns):
    """Project a closed published relation set using portable schema names."""
    _require(table_name in {HEADER, *PAYLOAD_TABLES} and bool(columns))
    relation = storage_relation(catalog_schema, table_name)
    editions = tuple(editions)
    if not editions:
        return relation
    _require(all(edition.catalog_schema == catalog_schema for edition in editions))
    _require(all(_text(edition.dataset_id, 96) for edition in editions))
    projection = ",".join(storage_identifier(column) for column in columns)
    datasets = ",".join(_literal(edition.dataset_id) for edition in editions)
    predicate = f"dataset_id NOT IN ({datasets})"
    if table_name == HEADER:
        endpoints = ",".join(_literal(edition.endpoint_id) for edition in editions)
        predicate += f" AND NOT (is_current AND endpoint_id IN ({endpoints}))"
    selects = [f"SELECT {projection} FROM {relation} WHERE {predicate}"]
    selects.extend(
        f"SELECT {projection} FROM {storage_relation(edition.schema_name, table_name)} "
        f"WHERE dataset_id={_literal(edition.dataset_id)}"
        for edition in editions
    )
    return "(" + " UNION ALL ".join(selects) + ")"


def artifact_dataset_relation(directory, schema, editions=()):
    """Select current immutable headers with the model's explicit column shape."""
    model = directory.ProviderDirectoryEndpointDataset
    if not editions:
        return directory._qt(schema, model.__tablename__)
    return source_relation_sql(
        schema,
        model.__tablename__,
        editions,
        columns=tuple(column.name for column in model.__table__.columns),
    )


def artifact_payload_relation(directory, schema, model, datasets=()):
    """Read selected immutable editions independently of a later pointer switch."""
    coordinates = sorted(
        {(dataset.storage_schema, dataset.dataset_id) for dataset in datasets if dataset.storage_schema is not None}
    )
    editions = tuple(
        SimpleNamespace(catalog_schema=schema, schema_name=storage_schema, dataset_id=dataset_id)
        for storage_schema, dataset_id in coordinates
    )
    if not editions:
        return directory._qt(schema, model.__tablename__)
    return source_relation_sql(
        schema,
        model.__tablename__,
        editions,
        columns=tuple(column.name for column in model.__table__.columns),
    )


async def has_named_artifact_inputs(directory, source_ids=None):
    """Detect logical named enrollment without changing legacy header selection."""
    database = directory.db
    pointer = storage_relation(POINTER_SCHEMA, POINTER_TABLE)
    if not await database.scalar("SELECT to_regclass(:pointer_name) IS NOT NULL", pointer_name=pointer):
        return False
    if not await database.scalar(
        f"SELECT EXISTS(SELECT FROM {pointer} WHERE catalog_schema=:catalog_schema)",
        catalog_schema=directory._schema(),
    ):
        return False
    if source_ids is None:
        return True
    sources = storage_relation(directory._schema(), directory.ProviderDirectorySource.__tablename__)
    return bool(
        await database.scalar(
            f"SELECT EXISTS(SELECT FROM {pointer} edition JOIN {sources} source ON source.endpoint_id=edition.endpoint_id "
            "WHERE edition.catalog_schema=:catalog_schema AND source.source_id=ANY(CAST(:source_ids AS varchar[])))",
            catalog_schema=directory._schema(),
            source_ids=list(source_ids),
        )
    )


@asynccontextmanager
async def artifact_selection_snapshot(directory, source_ids=None):
    """Resolve named pointers and selected headers in one repeatable snapshot."""
    from process.provider_directory_generic_source_policy import is_source_storage_required

    if is_source_storage_required():
        yield ()
        return
    database = directory.db
    has_bound_snapshot = database.has_reader_session() or database._transaction_binding() is not None
    async with database.transaction() as session:
        if not has_bound_snapshot:
            await database.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        elif await database.scalar("SELECT current_setting('transaction_isolation')") not in {
            "repeatable read",
            "serializable",
        }:
            if not await has_named_artifact_inputs(directory, source_ids):
                yield ()
                return
            raise RuntimeError("provider_directory_artifact_selection_snapshot_required")
        yield await named_editions_for_session(session, directory._schema(), source_ids)


def artifact_resource_input_relation(directory, schema, dataset):
    """Preserve SOURCE routing and prefer an explicitly selected immutable edition."""
    from process.provider_directory_generic_source_policy import is_source_storage_required
    from process.provider_directory_profile_source_inputs import resource_relation

    if dataset is not None and (storage_schema := getattr(dataset, "storage_schema", None)) is not None:
        return directory._unscoped_qt(storage_schema, directory.ProviderDirectoryDatasetResource.__tablename__)
    if dataset is not None and is_source_storage_required():
        return directory._unscoped_qt(schema, resource_relation(directory, dataset))
    return directory._qt(schema, directory.ProviderDirectoryDatasetResource.__tablename__)


def artifact_resource_header_relation(directory, schema, dataset):
    """Read headers from the same explicit edition or existing SOURCE route."""
    storage_schema = getattr(dataset, "storage_schema", None) if dataset is not None else None
    model = directory.ProviderDirectoryEndpointDataset
    if storage_schema is None:
        return directory._qt(schema, model.__tablename__)
    return directory._unscoped_qt(storage_schema, model.__tablename__)


async def artifact_fence_dataset_rows(directory, executor, datasets, query_params_by_name, *, for_update):
    """Recheck exact named headers without requesting writes on published tables."""
    named_datasets = tuple(dataset for dataset in datasets if dataset.storage_schema is not None)
    named_endpoints = {dataset.endpoint_id for dataset in named_datasets}
    dataset_rows = []
    if any(dataset.storage_schema is None for dataset in datasets):
        dataset_rows = [
            dataset_row
            for dataset_row in await executor.all(
                directory._artifact_fence_dataset_rows_sql(for_update=for_update),
                **query_params_by_name,
            )
            if directory._pagination_checkpoint_row_mapping(dataset_row)["endpoint_id"] not in named_endpoints
        ]
    for dataset in named_datasets:
        parameters_by_name = {**query_params_by_name, "dataset_ids": [dataset.dataset_id]}
        parameters_by_name.pop("endpoint_ids", None)
        selected_rows = await executor.all(
            directory._artifact_fence_dataset_rows_sql(
                for_update=True,
                dataset_schema=dataset.storage_schema,
                lock_rows=False,
            ),
            **parameters_by_name,
        )
        pointer = await executor.first(
            f"SELECT dataset_id,schema_name FROM {storage_relation(POINTER_SCHEMA, POINTER_TABLE)} "
            "WHERE catalog_schema=:catalog_schema AND endpoint_id=:endpoint_id",
            catalog_schema=directory._schema(),
            endpoint_id=dataset.endpoint_id,
        )
        pointer = directory._pagination_checkpoint_row_mapping(pointer) if pointer is not None else {}
        current_ids = [pointer["dataset_id"]] if pointer.get("schema_name") == dataset.storage_schema else []
        dataset_rows.extend(
            {**directory._pagination_checkpoint_row_mapping(selected_row), "locked_current_dataset_ids": current_ids}
            for selected_row in selected_rows
        )
    return dataset_rows


async def current_profile_dataset_map(directory, source_id):
    """Read one current source header and its metadata in a repeatable snapshot."""
    async with artifact_selection_snapshot(directory, [source_id]) as named_editions:
        database_row = await directory.db.first(
            f"""
            SELECT dataset.dataset_id,
                   dataset.endpoint_id,
                   dataset.acquisition_root_run_id,
                   dataset.dataset_hash,
                   dataset.status,
                   dataset.is_current,
                   dataset.superseded_at,
                   {directory._artifact_safe_publication_metadata_sql("dataset")}
                       AS publication_metadata_json
              FROM {source_registration_sql(directory._schema(), named_editions)} AS source
              JOIN {artifact_dataset_relation(directory, directory._schema(), named_editions)} AS dataset
                ON dataset.endpoint_id = source.endpoint_id
             WHERE source.source_id = :source_id
               AND dataset.status = :published_status
               AND dataset.is_current = true
               AND dataset.superseded_at IS NULL
             ORDER BY dataset.published_at DESC, dataset.dataset_id DESC
             LIMIT 2;
            """,
            source_id=source_id,
            published_status=directory.ENDPOINT_DATASET_PUBLISHED,
        )
    if database_row is None:
        return {}
    return dict(directory._pagination_checkpoint_row_mapping(database_row))


def artifact_source_summary_candidate_sql(directory, dataset):
    """Read source-summary metadata from the selected edition or legacy catalog."""
    dataset_ref = directory._unscoped_qt(
        dataset.storage_schema if dataset is not None and dataset.storage_schema is not None else directory._schema(),
        directory.ProviderDirectoryEndpointDataset.__tablename__,
    )
    return f"""
        SELECT dataset.acquisition_root_run_id,
               {directory._artifact_safe_publication_metadata_sql("dataset")}
                   AS publication_metadata_json
          FROM {dataset_ref} AS dataset
         WHERE dataset.dataset_id = :dataset_id
           AND dataset.endpoint_id = :endpoint_id
           AND COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)
               IS NOT DISTINCT FROM :evidence_run_id
           AND dataset.previous_dataset_id
               IS NOT DISTINCT FROM :previous_dataset_id
           AND dataset.dataset_hash IS NOT DISTINCT FROM :dataset_hash
           AND dataset.is_current = :is_current
           AND dataset.status = :expected_status
           AND dataset.superseded_at IS NULL;
    """


def _published_relation_proof(directory, metadata, dataset, key):
    """Reuse a completed relation proof only for the selected immutable dataset."""
    proof = metadata.get(key)
    if not (
        isinstance(proof, dict)
        and proof.get("complete") is True
        and type(proof.get("version")) is int
        and proof["version"] == 1
        and proof.get("dataset_id") == dataset.dataset_id
        and proof.get("acquisition_root_run_id") == dataset.evidence_run_id
        and all(type(value) is int and value >= 0 for name, value in proof.items() if name.endswith("_count"))
    ):
        raise directory.ProviderDirectoryArtifactBuildStale("provider_directory_named_relation_proof_invalid")
    directory._source_summary_relation_edge_count({key: proof}, key)
    return dict(proof)


async def reuse_artifact_relations(
    directory, dataset, fence, *, network_plan, affiliation_organization, source_summary
):
    """Read immutable edition proof_by_name without rebuilding or writing published relations."""
    async with directory.db.transaction():
        await directory._lock_and_verify_artifact_dataset_fence(fence, directory.db)
        candidate, metadata = await directory._current_artifact_dataset_source_summary_candidate(
            directory.db, dataset, tuple(sorted(alias.source_id for alias in fence.datasets))
        )
        metadata_keys = (
            directory.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY,
            directory.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY,
        )
        proof_by_name = {key: _published_relation_proof(directory, metadata, dataset, key) for key in metadata_keys}
        summary = directory._existing_artifact_dataset_source_summary(metadata, dataset, candidate, proof_by_name)
        if summary is None:
            raise directory.ProviderDirectoryArtifactBuildStale("provider_directory_named_source_summary_invalid")
        selected_proof_by_name = {
            key: proof_by_name[key]
            for key, enabled in zip(metadata_keys, (network_plan, affiliation_organization), strict=True)
            if enabled
        }
        return selected_proof_by_name, summary if source_summary else None


async def resolve_artifact_datasets(
    directory,
    source_ids: list[str] | tuple[str, ...] | None,
    *,
    should_select_validated_candidates: bool = False,
    allow_published_replacement: bool = False,
):
    """Resolve exact artifact inputs while preserving existing candidate and alias validation."""
    is_explicit_selection = source_ids is not None
    cleaned_source_ids = directory._clean_source_id_list(source_ids)
    if is_explicit_selection and not cleaned_source_ids:
        raise RuntimeError("provider_directory_artifact_source_ids_empty")
    async with artifact_selection_snapshot(
        directory, cleaned_source_ids if is_explicit_selection else None
    ) as named_editions:
        dataset_rows = await directory.db.all(
            directory._provider_directory_artifact_dataset_selection_sql(
                cleaned_source_ids if is_explicit_selection else None,
                should_select_validated_candidates=should_select_validated_candidates,
                allow_published_replacement=allow_published_replacement,
                named_editions=named_editions,
            ),
            published_status=directory.ENDPOINT_DATASET_PUBLISHED,
            validated_status=directory.ENDPOINT_DATASET_VALIDATED,
            select_validated_candidates=should_select_validated_candidates,
            **({"source_ids": cleaned_source_ids} if is_explicit_selection else {}),
        )
        dataset_rows = await directory._hydrate_legacy_artifact_dataset_rows(dataset_rows)
        schema_by_dataset = {edition.dataset_id: edition.schema_name for edition in named_editions}
        for dataset_row in dataset_rows:
            if (storage_schema := schema_by_dataset.get(dataset_row["dataset_id"])) is not None:
                dataset_row["storage_schema"] = storage_schema
    dataset_rows = await directory.annotate_uhc_flex_profile_dataset_readiness(
        dataset_rows,
        database=directory.db,
        row_mapping=directory._pagination_checkpoint_row_mapping,
    )
    fence = directory._validate_provider_directory_artifact_datasets(
        dataset_rows,
        cleaned_source_ids,
        should_select_validated_candidates=should_select_validated_candidates,
    )
    fence = await directory._expand_reviewed_artifact_promotion_aliases(fence)
    if allow_published_replacement:
        from process.provider_directory_published_source_replacement import mark_for_preparation

        return mark_for_preparation(directory, fence)
    return fence
