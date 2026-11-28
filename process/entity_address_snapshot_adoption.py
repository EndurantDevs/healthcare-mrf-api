# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adopt a restored completed EntityAddressUnified result without rebuilding it.

The archive/restore coordinator owns archive contents, restore isolation, and
generic receipt storage.  This module owns only the fixed seven-table native
publication seam.  It deliberately does not invoke the regular worker
shutdown path: that path derives coordinates, backfills fields, projects geo
assurance, and builds indexes for a fresh import.
"""

from __future__ import annotations

import hashlib
import importlib
import re
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import uuid4

from process import entity_address_result_generation as result_generation
from process.entity_address_retained_names import retained_address_name

entity_address_unified = importlib.import_module("process.entity_address_unified")


_SCHEMA_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_STAGE_SUFFIX_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
_RESULT_STAGE_TABLE_COUNT = 7


def _validated_snapshot_destination(
    *,
    db_schema: str,
    import_date: str,
) -> tuple[str, str]:
    """Normalize only safe, non-truncating identifiers for restored stage tables."""

    if not isinstance(db_schema, str):
        raise ValueError("entity-address snapshot adoption requires a schema name")
    normalized_schema = entity_address_unified._validate_schema_name(db_schema)
    if (
        not _SCHEMA_IDENTIFIER_PATTERN.fullmatch(normalized_schema)
        or len(normalized_schema.encode("utf-8")) > entity_address_unified.POSTGRES_IDENTIFIER_MAX_LENGTH
    ):
        raise ValueError("entity-address snapshot adoption requires a safe schema name")

    if not isinstance(import_date, str):
        raise ValueError("entity-address snapshot adoption requires import_date")
    normalized_import_date = import_date.strip()
    if not _STAGE_SUFFIX_PATTERN.fullmatch(normalized_import_date):
        raise ValueError("entity-address snapshot adoption requires a safe import_date")

    stage_table_names = tuple(
        f"{model.__tablename__}_{normalized_import_date}"
        for model in (
            entity_address_unified.EntityAddressUnified,
            *entity_address_unified.SUPPORT_TABLE_MODELS,
        )
    )
    if (
        len(stage_table_names) != _RESULT_STAGE_TABLE_COUNT
        or len(set(stage_table_names)) != _RESULT_STAGE_TABLE_COUNT
        or any(
            not _SCHEMA_IDENTIFIER_PATTERN.fullmatch(table_name)
            or len(table_name.encode("utf-8")) > entity_address_unified.POSTGRES_IDENTIFIER_MAX_LENGTH
            for table_name in stage_table_names
        )
    ):
        raise ValueError("entity-address snapshot adoption has invalid destination stage tables")
    return normalized_schema, normalized_import_date


@dataclass(frozen=True)
class EntityAddressSnapshotAdoptionCallbacks:
    """Local checks and receipt storage that share the final cutover transaction.

    ``verify_local_state`` must prove the restored stage's local relation OIDs,
    ownership, source alias and dependency semantics, predecessor fence, and
    geo-assurance preparation.  ``record_adoption`` may write only an
    address-native receipt after geo assurance activates.  The coordinator's
    generic installation receipt and scheduler state are written later by its
    own same-transaction ``after_record`` hook, once the canonical installation
    exists.  Neither callback may commit or start independent work.
    """

    verify_local_state: Callable[[], Awaitable[None]]
    record_adoption: Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class PreparedEntityAddressSnapshotAdoption:
    """Validated native stage state ready for one caller-owned cutover."""

    db_schema: str
    stage_cls: type
    support_stage_class_map: dict[type, type]
    swaps: list
    patch_statements: list[tuple[str, str]]
    relation_names: list[str]
    required_names: list[str]
    context: dict
    publish_validation: dict[str, int | dict[str, int]]


def _prepared_full_result_stage(
    *,
    db_schema: str,
    import_date: str,
) -> tuple[type, dict[type, type], list, list[tuple[str, str]], list[str], list[str]]:
    """Return the exact main-plus-six-support stage set for a completed result."""

    normalized_schema, normalized_import_date = _validated_snapshot_destination(
        db_schema=db_schema,
        import_date=import_date,
    )
    stage_cls = entity_address_unified.make_class(
        entity_address_unified.EntityAddressUnified,
        normalized_import_date,
    )
    support_stage_class_map = entity_address_unified._support_stage_classes(normalized_import_date)
    swaps, patch_statements, relation_names, required_names = entity_address_unified._entity_address_cutover_plan(
        normalized_schema,
        stage_cls,
        support_stage_class_map,
        partial_support_patch=False,
        affected_group_table="",
        context={},
    )
    return (
        stage_cls,
        support_stage_class_map,
        swaps,
        patch_statements,
        relation_names,
        required_names,
    )


def _validated_source_generation(
    source_generation: Mapping[str, Any] | result_generation.EntityAddressServingGeneration | None,
) -> result_generation.EntityAddressServingGeneration | None:
    """Normalize a portable source identity while preserving legacy absence."""

    if source_generation is None:
        return None
    return result_generation.validate_entity_address_serving_generation(source_generation)


async def _prepare_adoption_context(
    *,
    db_schema: str,
    stage_cls: type,
    swaps: list,
    source_generation: result_generation.EntityAddressServingGeneration | None,
) -> dict[str, Any]:
    """Make stages durable and bind analysis to the adoption identity."""

    for swap in swaps:
        await entity_address_unified._ensure_promoted_stage_logged(
            db_schema,
            swap.stage_cls.__tablename__,
        )
    context_map = {
        "address_alias_generation": await entity_address_unified._address_alias_generation(db_schema),
        "stage_persistence": "p",
        "result_generation_mode": "adoption",
        "source_serving_generation": (None if source_generation is None else source_generation.as_dict()),
    }
    await entity_address_unified._run_sql_phase(
        f"ANALYZE {db_schema}.{stage_cls.__tablename__};",
        context=context_map,
        phase="entity-address snapshot analyzing restored main table",
    )
    return context_map


async def prepare_completed_entity_address_snapshot_adoption(
    *,
    db_schema: str,
    import_date: str,
    preserve_unversioned_base_rows: bool = False,
    source_serving_generation: (Mapping[str, Any] | result_generation.EntityAddressServingGeneration | None) = None,
    archive_relation: tuple[str, str] | None = None,
) -> PreparedEntityAddressSnapshotAdoption:
    """Prepare one result, preserving its origin or explicitly adopting legacy input.

    A missing ``source_serving_generation`` denotes a generation-less manual
    archive. Activation clears the source serving tuple and advances the local
    generation counter to distinguish that adopted edition from its predecessor.
    """

    normalized_schema, normalized_import_date = _validated_snapshot_destination(
        db_schema=db_schema,
        import_date=import_date,
    )
    validated_source_generation = _validated_source_generation(source_serving_generation)
    (
        stage_cls,
        support_stage_class_map,
        swaps,
        patch_statements,
        relation_names,
        required_names,
    ) = _prepared_full_result_stage(
        db_schema=normalized_schema,
        import_date=normalized_import_date,
    )
    cutover_context_map = await _prepare_adoption_context(
        db_schema=normalized_schema,
        stage_cls=stage_cls,
        swaps=swaps,
        source_generation=validated_source_generation,
    )
    publish_validation = await _validate_adoption_stage(
        normalized_schema,
        stage_cls.__tablename__,
        support_stage_class_map,
        preserve_unversioned_base_rows=preserve_unversioned_base_rows,
        **({} if archive_relation is None else {"archive_relation": archive_relation}),
    )
    return PreparedEntityAddressSnapshotAdoption(
        db_schema=normalized_schema,
        stage_cls=stage_cls,
        support_stage_class_map=support_stage_class_map,
        swaps=swaps,
        patch_statements=patch_statements,
        relation_names=relation_names,
        required_names=required_names,
        context=cutover_context_map,
        publish_validation=publish_validation,
    )


async def _validate_adoption_stage(
    db_schema: str,
    stage_table: str,
    support_stage_class_map: dict[type, type],
    *,
    preserve_unversioned_base_rows: bool,
    archive_relation: tuple[str, str] | None = None,
) -> dict[str, int | dict[str, int]]:
    """Choose the native validation projection for the restored contract."""

    if preserve_unversioned_base_rows:
        return await _validate_preserved_base_version_stage(
            db_schema,
            stage_table,
            support_stage_class_map,
            **({} if archive_relation is None else {"archive_relation": archive_relation}),
        )
    return await entity_address_unified._validate_publish_integrity(
        db_schema,
        stage_table,
        support_stage_class_map,
        test_mode=False,
        **({} if archive_relation is None else {"archive_relation": archive_relation}),
    )


def _base_version_validation_view_sql(
    db_schema: str,
    stage_table: str,
    validation_table: str,
    destination_alias_version: str,
) -> str:
    """Project allowed unversioned rows as current only during native validation."""

    selected_columns = []
    for column in entity_address_unified.EntityAddressUnified.__table__.columns:
        if column.name == "base_address_version":
            selected_columns.append(
                "CASE WHEN base_address_version IS NULL "
                f"OR base_address_version = '{entity_address_unified.BASE_ADDRESS_VERSION}' "
                f"THEN '{destination_alias_version}' ELSE base_address_version END "
                "AS base_address_version"
            )
        else:
            selected_columns.append(column.name)
    return (
        f"CREATE VIEW {db_schema}.{validation_table} AS SELECT "
        f"{', '.join(selected_columns)} FROM {db_schema}.{stage_table}"
    )


async def _validate_preserved_base_version_stage(
    db_schema: str,
    stage_table: str,
    support_stage_class_map: dict[type, type],
    *,
    archive_relation: tuple[str, str] | None = None,
) -> dict[str, int | dict[str, int]]:
    """Run native integrity checks without rewriting allowed unversioned rows."""

    alias_generation = await entity_address_unified._address_alias_generation(db_schema)
    destination_alias_version = f"{entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX}{alias_generation}"
    table_digest = hashlib.sha256(stage_table.encode("ascii")).hexdigest()[:16]
    validation_table = f"entity_address_snapshot_validation_{table_digest}"
    view_sql = _base_version_validation_view_sql(
        db_schema,
        stage_table,
        validation_table,
        destination_alias_version,
    )
    async with entity_address_unified.db.transaction():
        await entity_address_unified.db.status(view_sql)
        validation = await entity_address_unified._validate_publish_integrity(
            db_schema,
            validation_table,
            support_stage_class_map,
            test_mode=False,
            **({} if archive_relation is None else {"archive_relation": archive_relation}),
        )
        await entity_address_unified.db.status(f"DROP VIEW {db_schema}.{validation_table}")
    return validation


async def adopt_prepared_entity_address_snapshot(
    prepared: PreparedEntityAddressSnapshotAdoption,
    *,
    callbacks: EntityAddressSnapshotAdoptionCallbacks,
) -> dict[str, int | dict[str, int]]:
    """Cut over a prepared result under the caller's already-open transaction.

    The native cutover opens only a nested savepoint under that transaction;
    this function never commits.  An optional address-native receipt follows
    geo-assurance activation; the coordinator records its generic installation
    receipt only in its later ``after_record`` hook, after the canonical
    installation exists and before that caller commits.
    """

    native_callbacks = entity_address_unified.EntityAddressCutoverCallbacks(
        before_cutover=callbacks.verify_local_state,
        after_publish=callbacks.record_adoption,
    )
    await entity_address_unified._run_entity_address_cutover(
        prepared.db_schema,
        prepared.swaps,
        prepared.patch_statements,
        prepared.relation_names,
        prepared.required_names,
        prepared.context,
        callbacks=native_callbacks,
        require_caller_owned_transaction=True,
    )
    return prepared.publish_validation


async def prepare_entity_address_publisher_reimport(session, *, db_schema, import_date, dependency_bindings=None):
    """Finish projection, close native writes, and validate the complete named stage."""
    from process import entity_address_snapshot_preparation as protected
    from process import reference_family_archive as archive
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

    await protected._publisher_authority(session)
    schema, date = _validated_snapshot_destination(db_schema=db_schema, import_date=import_date)
    await protected._require_alias_authority(session, schema)
    stage, _support, swaps, _patches, _relations, _required = _prepared_full_result_stage(
        db_schema=schema, import_date=date
    )
    stage_names = tuple(swap.stage_cls.__tablename__ for swap in swaps)
    tables = ",".join(f'"{schema}"."{name}"' for name in stage_names)
    await session.execute(protected.text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    binding = entity_address_unified.db._transaction_binding()
    if binding is not None and binding.session is not session:
        raise RuntimeError("address publisher reimport session differs")
    geo_context_by_field = {}
    async with entity_address_unified.db.bind_existing_session(session) if binding is None else nullcontext():
        if dependency_bindings is not None:
            await protected._lock_publication_state(session, schema)
            geo_context_by_field[
                "geo_assurance_projected_rows"
            ] = await entity_address_unified._materialize_geo_assurance(
                schema,
                stage.__tablename__,
                force=True,
                context=geo_context_by_field,
                run_id="",
                stage_rows=0,
                dependency_bindings=dependency_bindings,
            )
            if geo_context_by_field.get("geo_assurance_candidate_relation") != {
                "schema_name": schema,
                "relation_name": stage.__tablename__,
            }:
                raise RuntimeError("address publisher projection stage differs")
        owner, readers = archive.model_family_roles()
        driver = await archive._native_model_copy_driver(session, ("execute", "fetch", "is_in_transaction"))
        await freeze_fhir_source_snapshot(
            driver,
            schema,
            namespace_owner=owner,
            reader_roles=readers,
            application_roles=(),
            relation_names=stage_names,
            freeze_schema=False,
        )
        prepared = await prepare_completed_entity_address_snapshot_adoption(db_schema=schema, import_date=date)
    prepared.context.update(geo_context_by_field)
    prepared.context.update(
        snapshot_contract="entity_address_unified.postgres.v2",
        stage_relations=[{"schema_name": schema, "relation_name": name} for name in stage_names],
        result_generation_mode="ordinary",
        source_serving_generation=None,
    )
    return prepared


async def _is_registered_snapshot(db, db_schema, names):
    """A protected snapshot is registered in the retention claims; table ownership does not decide it."""
    from process.nucc_retention_read_view import _CLAIM_SOURCE_SQL, NuccRetentionReadError, claim_relations_source

    if await db.scalar("SELECT pg_catalog.to_regnamespace('hp_snapshot_retention') IS NULL") is not False:
        return False
    try:
        claims = claim_relations_source(await db.scalar(_CLAIM_SOURCE_SQL))
    except NuccRetentionReadError:
        # An incomplete catalogue cannot show that the table is unprotected.
        return True
    return (
        await db.scalar(
            f"SELECT EXISTS(SELECT 1 FROM {claims} claims "
            "WHERE claims.schema_name=:schema AND claims.relation_name=ANY(CAST(:names AS text[])))",
            schema=db_schema,
            names=list(names),
        )
        is True
    )


async def _retained_serving_generation(session, db_schema):
    """Read only the portable incumbent once while the publisher holds its cutover fence."""
    from sqlalchemy import text

    schema = result_generation._schema_name(db_schema)
    state = await session.scalar(
        text("SELECT to_regclass(:relation)"),
        {
            "relation": f'"{schema}"."{result_generation.TABLE_NAME}"',
        },
    )
    origin_row = (
        None
        if state is None
        else (
            (
                await session.execute(
                    text(
                        f'SELECT origin_lineage_id,origin_generation,published_at FROM "{schema}".entity_address_result_generation '
                        "WHERE singleton IS TRUE"
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
    )
    if state is not None and origin_row is None:
        raise RuntimeError("retained address generation is unavailable")
    generation_by_field = {} if origin_row is None else dict(origin_row)
    if not generation_by_field or all(field_value is None for field_value in generation_by_field.values()):
        return None
    return result_generation.validate_entity_address_serving_generation(generation_by_field)


async def _prepare_protected_ordinary_rotation(db_schema, swaps, partial_support_patch, context):
    """Capture the locked incumbent once without invoking full-family preparation."""
    from process.network_compact_layout import CompactAddressRelation
    from process.network_compact_retention import require_network_compact_artifacts_unreferenced
    from process.provider_directory_serving_recovery import GENERATION_PARAM
    from process.reference_family_archive import _native_model_copy_driver

    binding = entity_address_unified.db._transaction_binding()
    if binding is None:
        raise RuntimeError("address rotation requires a publisher transaction")
    generation = await _retained_serving_generation(binding.session, db_schema)
    if "stage_relations" in context or context.get("snapshot_contract") == "entity_address_unified.postgres.v2":
        return generation
    if partial_support_patch:
        raise RuntimeError("retained address reimport requires a full isolated replacement")
    if generation is None or context.get(GENERATION_PARAM) is not None:
        driver = await _native_model_copy_driver(binding.session, ("execute", "fetch", "is_in_transaction"))
        names = [
            CompactAddressRelation(db_schema, name)
            for swap in swaps
            for name in (swap.live_cls.__main_table__, swap.live_cls.__main_table__ + "_old")
        ]
        projection = await require_network_compact_artifacts_unreferenced(driver, names)
        await require_network_compact_artifacts_unreferenced(driver, names, expected_projection=projection)
    return generation


async def _require_protected_cutover(db_schema, swaps, context):
    """Bind the exact declared named stage before replacing a retained generation."""
    db = entity_address_unified.db
    if "stage_relations" in context or context.get("snapshot_contract") == "entity_address_unified.postgres.v2":
        expected_names = {swap.stage_cls.__tablename__ for swap in swaps}
        entries = context.get("stage_relations")
        if entries is None and isinstance(context.get("protected_stage_oids"), Mapping):
            # Historical addresses remain opaque; only their declared names select tables.
            entries = [{"schema_name": db_schema, "relation_name": name} for name in context["protected_stage_oids"]]
        if (
            not isinstance(entries, list)
            or len(entries) != len(expected_names)
            or any(
                not isinstance(entry, Mapping)
                or set(entry) != {"schema_name", "relation_name"}
                or entry["schema_name"] != db_schema
                for entry in entries
            )
            or {entry["relation_name"] for entry in entries} != expected_names
        ):
            raise RuntimeError("protected address stage inventory differs")
        for name in expected_names:
            await db.scalar(f'SELECT count(*) FROM (SELECT 1 FROM "{db_schema}"."{name}" LIMIT 0) AS stage')
        context["retained_relations"] = []
        return
    if await _is_registered_snapshot(
        db,
        db_schema,
        [name for swap in swaps for name in (swap.live_cls.__main_table__, swap.live_cls.__main_table__ + "_old")],
    ):
        raise RuntimeError("legacy address rotation cannot replace a protected snapshot")


async def _swap_sealed_stage_table(db_schema, live_cls, stage_cls, *, serving_generation=...):
    """Rotate immutable named tables without overwriting a retained generation."""
    from process import entity_address_publication_permissions as permissions
    from process.native_publication_read_access import preserve_publication_reads
    from process.reference_family_archive import _native_model_copy_driver

    db = entity_address_unified.db
    binding = db._transaction_binding()
    if binding is None:
        raise RuntimeError("protected address rotation requires a publisher transaction")
    table = live_cls.__main_table__
    stage = stage_cls.__tablename__
    oid = await db.scalar("SELECT to_regclass(:relation)::oid", relation=f'"{db_schema}"."{table}"')
    stage_oid = await db.scalar("SELECT to_regclass(:relation)::oid", relation=f'"{db_schema}"."{stage}"')
    if stage_oid is None:
        raise RuntimeError("protected address stage is missing")
    if serving_generation is ...:
        serving_generation = await _retained_serving_generation(binding.session, db_schema)
    if serving_generation is not None and oid is None:
        raise RuntimeError("serving address relation is missing")
    driver = await _native_model_copy_driver(binding.session, ("execute", "fetch", "is_in_transaction"))
    await preserve_publication_reads(driver, schema=db_schema, live_table=table, stage_table=stage)
    await permissions.make_relation_read_only(binding.session, db_schema, stage)
    retained_by_field = None
    if oid is not None:
        await permissions.make_relation_read_only(binding.session, db_schema, table)
        name = (
            retained_address_name(table, serving_generation)
            if serving_generation is not None
            else entity_address_unified._archived_identifier(f"{table}_retained_{uuid4().hex}", suffix="")
        )
        if await db.scalar("SELECT to_regclass(:relation)", relation=f'"{db_schema}"."{name}"') is not None:
            raise RuntimeError("protected address retained name already exists")
        index_entries = await db.all(
            "SELECT c.oid,c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=:oid",
            oid=oid,
        )
        for _index_handle, index_name in index_entries:
            await db.status(f'ALTER INDEX "{db_schema}"."{index_name}" RENAME TO "address_retained_idx_{uuid4().hex}"')
        await db.status(f'ALTER TABLE "{db_schema}"."{table}" RENAME TO "{name}"')
        retained_by_field = {"schema_name": db_schema, "relation_name": name}
    await db.status(f'ALTER TABLE "{db_schema}"."{stage}" RENAME TO "{table}"')
    index_names = [("primary", f"{table}_idx_primary")]
    index_names.extend(
        (
            index_definition.get("name", "_".join(index_definition.get("index_elements"))),
            f"{table}_idx_{index_definition.get('name', '_'.join(index_definition.get('index_elements')))}",
        )
        for index_definition in getattr(stage_cls, "__my_additional_indexes__", []) or []
    )
    for index_name, live_index_name in index_names:
        await db.status(
            f'ALTER INDEX IF EXISTS "{db_schema}"."{entity_address_unified._stage_index_name(stage, index_name)}" RENAME TO "{live_index_name}"'
        )
    return retained_by_field


async def _publish_result_swaps(db_schema, swaps, context, *, serving_generation=...):
    """Retain ordinary replacements without changing the marked recovery protocol."""
    from process.provider_directory_serving_recovery import GENERATION_PARAM

    if serving_generation is ...:
        binding = entity_address_unified.db._transaction_binding()
        if binding is None:
            raise RuntimeError("address rotation requires a publisher transaction")
        serving_generation = await _retained_serving_generation(binding.session, db_schema)
    is_controlled = (
        "stage_relations" in context or context.get("snapshot_contract") == "entity_address_unified.postgres.v2"
    )
    should_retain = is_controlled or context.get(GENERATION_PARAM) is None
    retained_entries = []
    context["retained_relations"] = []
    for swap in swaps:
        if should_retain:
            retained = await _swap_sealed_stage_table(
                db_schema, swap.live_cls, swap.stage_cls, serving_generation=serving_generation
            )
            if retained is not None:
                retained_entries.append(retained)
        else:
            await entity_address_unified._swap_stage_table(db_schema, swap.live_cls, swap.stage_cls)
    context["retained_relations"] = retained_entries


def _entity_address_cutover_plan(
    db_schema: str,
    stage_cls,
    support_stage_class_map: dict[type, type],
    *,
    partial_support_patch: bool,
    affected_group_table: str,
    context: dict,
) -> tuple[list[entity_address_unified._StageTableSwap], list[tuple[str, str]], list[str], list[str]]:
    swaps = [entity_address_unified._StageTableSwap(entity_address_unified.EntityAddressUnified, stage_cls)]
    patch_statements: list[tuple[str, str]] = []
    if partial_support_patch:
        patch_statements = entity_address_unified._partial_support_patch_sql(
            db_schema,
            support_stage_class_map,
            old_entity_table=f"{entity_address_unified.EntityAddressUnified.__main_table__}_old",
            affected_group_table=affected_group_table,
            build_network_bridge=bool(
                context.get("build_network_bridge", entity_address_unified.DEFAULT_BUILD_NETWORK_BRIDGE)
            ),
        )
    else:
        swaps.extend(
            entity_address_unified._StageTableSwap(live_cls, support_stage_cls)
            for live_cls, support_stage_cls in support_stage_class_map.items()
        )
    relation_names, required_names = entity_address_unified._cutover_relation_sets(
        swaps,
        support_stage_class_map,
        partial_support_patch=partial_support_patch,
        affected_group_table=affected_group_table,
    )
    return swaps, patch_statements, relation_names, required_names


__all__ = [
    "EntityAddressSnapshotAdoptionCallbacks",
    "PreparedEntityAddressSnapshotAdoption",
    "adopt_prepared_entity_address_snapshot",
    "prepare_completed_entity_address_snapshot_adoption",
    "prepare_entity_address_publisher_reimport",
]
