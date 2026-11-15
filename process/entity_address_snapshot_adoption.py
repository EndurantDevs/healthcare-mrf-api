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
import time
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Mapping

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
    archive and causes activation to clear the destination serving tuple while
    leaving its local generation counter unchanged.
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
    """Validate a complete locked stage and apply actual publication permissions."""
    from process import entity_address_publication_permissions as permissions
    from process import entity_address_snapshot_preparation as protected

    if not session.in_transaction():
        raise RuntimeError("address publisher reimport requires a caller transaction")
    schema, date = _validated_snapshot_destination(db_schema=db_schema, import_date=import_date)
    stage, _support, swaps, _patches, _relations, _required = _prepared_full_result_stage(
        db_schema=schema, import_date=date
    )
    tables = ",".join(f'"{schema}"."{swap.stage_cls.__tablename__}"' for swap in swaps)
    await session.execute(protected.text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    role = await permissions.retention_role(session)
    stage_oid_by_name = {}
    for swap in swaps:
        oid = await session.scalar(
            protected.text("SELECT to_regclass(:relation)::oid"),
            {"relation": f'"{schema}"."{swap.stage_cls.__tablename__}"'},
        )
        if type(oid) is not int or oid <= 0:
            raise RuntimeError("address publisher stage is missing")
        await permissions.seal_relation(session, schema, swap.stage_cls.__tablename__, role=role)
        stage_oid_by_name[swap.stage_cls.__tablename__] = oid
    binding = entity_address_unified.db._transaction_binding()
    if binding is not None and binding.session is not session:
        raise RuntimeError("address publisher reimport session differs")
    geo_context_by_field = {}
    async with entity_address_unified.db.bind_existing_session(session) if binding is None else nullcontext():
        if dependency_bindings is not None:
            await protected._lock_publication_state(session, schema, None)
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
            if geo_context_by_field["geo_assurance_candidate_table_oid"] != stage_oid_by_name[stage.__tablename__]:
                raise RuntimeError("address publisher projection stage differs")
        prepared = await prepare_completed_entity_address_snapshot_adoption(db_schema=schema, import_date=date)
    prepared.context.update(geo_context_by_field)
    prepared.context.update(
        snapshot_contract="entity_address_unified.postgres.v2",
        result_generation_mode="ordinary",
        source_serving_generation=None,
    )
    return prepared


async def _prepare_protected_ordinary_rotation(db_schema, swaps, partial_support_patch, context):
    """Plan ordinary retention from the locked incumbent and actual replacement scope."""
    from process.network_cms_registry_address_capture import native_driver
    from process.network_compact_retention import require_named_address_artifacts_unreferenced
    from process.provider_directory_serving_recovery import GENERATION_PARAM

    db = entity_address_unified.db
    binding = db._transaction_binding()
    if binding is None:
        raise RuntimeError("address rotation requires a publisher transaction")
    generation = await _retained_serving_generation(binding.session, db_schema)
    if context.get("snapshot_contract") == "entity_address_unified.postgres.v2":
        return generation
    if partial_support_patch:
        raise RuntimeError("retained address reimport requires a full isolated replacement")
    if generation is None or context.get(GENERATION_PARAM) is not None:
        connection = getattr(binding.session, "native_connection", None)
        if connection is None:
            connection = await native_driver(binding.session)
        await require_named_address_artifacts_unreferenced(
            connection,
            [
                (db_schema, name)
                for swap in swaps
                for name in (swap.live_cls.__main_table__, swap.live_cls.__main_table__ + "_old")
            ],
        )
    return generation


async def _require_protected_cutover(db_schema, swaps, context):
    """Validate named stages and preserve the current locations of retained snapshots."""
    db = entity_address_unified.db
    if context.get("snapshot_contract") == "entity_address_unified.postgres.v2":
        stage_names = {swap.stage_cls.__tablename__ for swap in swaps}
        expected = context.get("protected_stage_oids")
        if "protected_stage_oids" in context and (not isinstance(expected, Mapping) or set(expected) != stage_names):
            raise RuntimeError("protected address stage inventory differs")
        stage_count = await db.scalar(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=:schema AND c.relname=ANY(CAST(:names AS text[])) "
            "AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity",
            schema=db_schema,
            names=sorted(stage_names),
        )
        if stage_count != len(stage_names):
            raise RuntimeError("protected address stage layout differs")
        if context.get("result_generation_mode") == "adoption":
            await _require_adoptable_generation(db_schema, swaps, context.get("source_serving_generation"))
        context["retained_relations"] = []
        return
    if not await db.scalar(
        "SELECT EXISTS(SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='hp_snapshot_retention' AND c.relname='generation')"
    ):
        return
    if not await db.scalar(
        "SELECT EXISTS(SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='hp_snapshot_retention' AND c.relname='generation_relocation')"
    ):
        raise RuntimeError("retained snapshot location catalog is missing")
    protected = await db.scalar(
        "WITH current_locations AS ("
        "SELECT generation.current_location_fence IS NOT NULL AND location.fence_token IS NULL AS missing_location, "
        "CASE WHEN generation.current_location_fence IS NULL THEN generation.inventory "
        "ELSE location.after_inventory END AS inventory "
        "FROM hp_snapshot_retention.generation AS generation "
        "LEFT JOIN hp_snapshot_retention.generation_relocation AS location "
        "ON location.generation_id=generation.generation_id AND location.fence_token=generation.current_location_fence "
        "WHERE generation.state IN ('retained', 'retiring')) "
        "SELECT EXISTS(SELECT 1 FROM current_locations WHERE missing_location OR CASE "
        "WHEN jsonb_typeof(inventory->'relations') IS DISTINCT FROM 'array' THEN true "
        "ELSE jsonb_array_length(inventory->'relations')=0 OR EXISTS("
        "SELECT 1 FROM jsonb_array_elements(inventory->'relations') AS relation "
        "WHERE jsonb_typeof(relation->'schema_name') IS DISTINCT FROM 'string' "
        "OR jsonb_typeof(relation->'relation_name') IS DISTINCT FROM 'string' "
        "OR relation->>'schema_name'='' OR relation->>'relation_name'='' "
        "OR (relation->>'schema_name'=:schema AND relation->>'relation_name'=ANY(CAST(:names AS text[])))) END)",
        schema=db_schema,
        names=[
            name for swap in swaps for name in (swap.live_cls.__main_table__, swap.live_cls.__main_table__ + "_old")
        ],
    )
    if protected is True:
        raise RuntimeError("legacy address rotation cannot replace a protected snapshot")


async def _require_adoptable_generation(db_schema, swaps, source_generation):
    """Do not adopt an origin that cannot subsequently be retained without replacement."""
    if source_generation is None:
        return
    generation = result_generation.validate_entity_address_serving_generation(source_generation)
    schema = result_generation._schema_name(db_schema)
    binding = entity_address_unified.db._transaction_binding()
    if binding is None:
        raise RuntimeError("address adoption requires a publisher transaction")
    incumbent = await _retained_serving_generation(binding.session, schema)
    if incumbent is not None and generation == result_generation.validate_entity_address_serving_generation(incumbent):
        raise RuntimeError("address adoption generation is already serving")
    names = [f'"{schema}"."{retained_address_name(swap.live_cls.__main_table__, generation)}"' for swap in swaps]
    if await entity_address_unified.db.scalar(
        "SELECT EXISTS(SELECT FROM unnest(CAST(:relations AS text[])) named(relation) "
        "WHERE to_regclass(named.relation) IS NOT NULL)",
        relations=names,
    ):
        raise RuntimeError("address adoption generation is already retained")


async def _retained_serving_generation(session, db_schema):
    """Read only the portable incumbent once while the publisher holds its cutover fence."""
    from sqlalchemy import text

    from process.network_cms_registry_address_capture import native_driver
    from process.network_compact_retention import require_named_address_artifacts_unreferenced

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
        connection = getattr(session, "native_connection", None)
        if connection is None:
            connection = await native_driver(session)
        await require_named_address_artifacts_unreferenced(
            connection, [(schema, table) for table in result_generation.RELATION_NAMES]
        )
        return None
    return result_generation.validate_entity_address_serving_generation(generation_by_field)


async def _retain_address_table(db_schema, table, oid, serving_generation):
    """Keep the incumbent under its retained name without replacing an occupied name."""
    db = entity_address_unified.db
    name = (
        retained_address_name(table, serving_generation)
        if serving_generation is not None
        else entity_address_unified._archived_identifier(f"{table}_retained_{oid:x}", suffix="")
    )
    if await db.scalar("SELECT to_regclass(:relation)", relation=f'"{db_schema}"."{name}"') is not None:
        raise RuntimeError("protected address retained name already exists")
    index_entries = await db.all(
        "SELECT c.oid,c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=:oid",
        oid=oid,
    )
    for index_oid, index_name in index_entries:
        await db.status(f'ALTER INDEX "{db_schema}"."{index_name}" RENAME TO "address_retained_idx_{index_oid:x}"')
    await db.status(f'ALTER TABLE "{db_schema}"."{table}" RENAME TO "{name}"')
    return {"table_name": table, "relation_oid": oid, "retained_name": name}


async def _swap_sealed_stage_table(
    db_schema, live_cls, stage_cls, owner_oid=None, *, serving_generation=..., transfer_ownership=True
):
    """Retain the published logical generation without overwriting its predecessor."""
    from process import entity_address_publication_permissions as permissions

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
    await permissions.preserve_reader_access(binding.session, db_schema, table, stage)
    if transfer_ownership:
        role = await permissions.retention_role(binding.session)
        await permissions.seal_relation(binding.session, db_schema, stage, role=role)
    else:
        await permissions.make_relation_read_only(binding.session, db_schema, stage)
    retained_by_field = None
    if oid is not None:
        if transfer_ownership:
            await permissions.seal_relation(binding.session, db_schema, table, role=role)
        else:
            await permissions.make_relation_read_only(binding.session, db_schema, table)
        retained_by_field = await _retain_address_table(db_schema, table, oid, serving_generation)
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


async def _patch_address_support(patch_statements, context):
    """Apply the support mutation plan within the existing cutover transaction."""
    for label, statement in patch_statements:
        started = time.monotonic()
        rowcount = await entity_address_unified.db.status(statement)
        entity_address_unified._record_phase_timing(
            context,
            f"entity-address-unified patching support {label}",
            time.monotonic() - started,
            entity_address_unified._coerce_rowcount(rowcount),
        )


async def _publish_result_swaps(db_schema, swaps, context, *, serving_generation=...):
    """Retain actual replacements while preserving the marked recovery protocol."""
    from process.provider_directory_serving_recovery import GENERATION_PARAM

    is_protected = context.get("snapshot_contract") == "entity_address_unified.postgres.v2"
    if serving_generation is ...:
        binding = entity_address_unified.db._transaction_binding()
        if binding is None:
            raise RuntimeError("protected address rotation requires a publisher transaction")
        serving_generation = await _retained_serving_generation(binding.session, db_schema)
    should_retain = is_protected or context.get(GENERATION_PARAM) is None
    retained_entries = []
    context["retained_relations"] = []
    for swap in swaps:
        if should_retain:
            retained = await _swap_sealed_stage_table(
                db_schema,
                swap.live_cls,
                swap.stage_cls,
                serving_generation=serving_generation,
                transfer_ownership=is_protected,
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
