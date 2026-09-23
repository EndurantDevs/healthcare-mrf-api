# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare and atomically activate one restored unified-address destination."""

from __future__ import annotations

import importlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from api import ptg2_geo_projection as geo_projection
from db.connection import db
from process.entity_address_cutover_contract import preserve_transaction_sql_settings
from process.entity_address_snapshot_alias import (
    EntityAddressAliasSemanticReceipt,
    EntityAddressSnapshotAliasError,
    capture_entity_address_alias_semantic_receipt,
    require_matching_entity_address_alias_semantics,
    validate_entity_address_alias_semantic_receipt,
)
from process.entity_address_snapshot_ownership import (
    EntityAddressArchiveOwnershipError,
    EntityAddressArchiveStageOwnership,
    validate_entity_address_archive_stage_ownership,
    verify_entity_address_archive_stage_ownership,
)
from process.entity_address_snapshot_receipt import (
    EntityAddressArchiveReceipt,
    EntityAddressArchiveReceiptError,
    EntityAddressArchiveTableReceipt,
    EntityAddressStageIntegrityReceipt,
    capture_entity_address_archive_receipt,
    capture_entity_address_stage_integrity_receipt,
    validate_entity_address_archive_receipt,
    validate_entity_address_stage_integrity_receipt,
)
from process.entity_address_result_generation import (
    EntityAddressServingGeneration,
    validate_entity_address_serving_generation,
)

adoption = importlib.import_module("process.entity_address_snapshot_adoption")
restore = importlib.import_module("process.entity_address_snapshot_restore")
entity_address_unified = importlib.import_module("process.entity_address_unified")

CONTRACT = "entity_address_snapshot_destination.postgres.v1"
BASE_VERSION_REMAP_CONTRACT = "entity_address_base_version_remap.postgres.v1"
GEO_PREPARATION_CONTRACT = "entity_address_geo_assurance_preparation.postgres.v1"
_LOCAL_GEO_DEPENDENCIES = (
    "npi_address",
    "mrf_address",
    "doctor_clinician_address",
    "geo_zip_lookup",
)
_SHARED_GEO_DEPENDENCIES = ("tiger.zip_state", "tiger.zcta5")
_RECEIPT_SETTING_NAMES = (
    "TimeZone",
    "DateStyle",
    "IntervalStyle",
    "extra_float_digits",
    "bytea_output",
    "work_mem",
    "search_path",
)


class EntityAddressSnapshotDestinationError(RuntimeError):
    """A restored address result cannot be prepared or activated locally."""


@dataclass(frozen=True)
class EntityAddressBaseVersionRemapEvidence:
    """Bounded proof of the only supported source-to-destination remap."""

    source_alias_generation: int
    destination_alias_generation: int
    alias_rows_bound: int
    alias_rows_rewritten: int
    null_rows_preserved: int
    plain_base_rows_preserved: int
    pre_remap_content_sha256: str
    post_remap_content_sha256: str

    def as_dict(self) -> dict[str, Any]:
        """Return the durable local remap receipt."""

        return {"contract": BASE_VERSION_REMAP_CONTRACT, **self.__dict__}


@dataclass(frozen=True)
class EntityAddressGeoAssurancePreparation:
    """Destination-local identity of one fully projected stage candidate."""

    stage_table_oid: int
    projected_rows: int
    relation_signature: tuple[tuple[str, int, int], ...]

    def as_dict(self) -> dict[str, Any]:
        """Return the bounded local geo-assurance receipt."""

        return {
            "contract": GEO_PREPARATION_CONTRACT,
            "geo_assurance_version": geo_projection.GEO_ASSURANCE_VERSION,
            "stage_table_oid": self.stage_table_oid,
            "projected_rows": self.projected_rows,
            "relation_signature": {
                name: [relation_oid, relation_filenode]
                for name, relation_oid, relation_filenode in self.relation_signature
            },
        }


@dataclass(frozen=True)
class PreparedEntityAddressSnapshotDestination:
    """Serializable local preparation required for conditional activation."""

    restored: restore.PreparedEntityAddressSnapshotRestore
    source_semantic_receipt: EntityAddressArchiveReceipt
    source_alias_receipt: EntityAddressAliasSemanticReceipt
    destination_alias_receipt: EntityAddressAliasSemanticReceipt
    base_version_remap: EntityAddressBaseVersionRemapEvidence
    geo_assurance: EntityAddressGeoAssurancePreparation

    def as_dict(self) -> dict[str, Any]:
        """Return the complete destination-local activation contract."""

        return {
            "contract": CONTRACT,
            "restored": self.restored.as_dict(),
            "source_semantic_receipt": self.source_semantic_receipt.as_dict(),
            "source_alias_receipt": self.source_alias_receipt.as_dict(),
            "destination_alias_receipt": self.destination_alias_receipt.as_dict(),
            "base_version_remap": self.base_version_remap.as_dict(),
            "geo_assurance": self.geo_assurance.as_dict(),
        }


def _require_caller_transaction(session: Any) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise EntityAddressSnapshotDestinationError("entity-address destination requires a caller transaction")


@asynccontextmanager
async def _preserve_receipt_settings() -> AsyncIterator[None]:
    """Strictly restore receipt GUCs, including the list-valued search path."""

    previous_settings = []
    for setting_name in _RECEIPT_SETTING_NAMES:
        setting_value = await db.scalar(
            "SELECT pg_catalog.current_setting(:setting_name)",
            setting_name=setting_name,
        )
        previous_settings.append((setting_name, str(setting_value)))
    async with db.transaction():
        yield
        for setting_name, setting_value in previous_settings:
            await db.scalar(
                "SELECT pg_catalog.set_config(:setting_name, :setting_value, true)",
                setting_name=setting_name,
                setting_value=setting_value,
            )


async def _destination_alias_binding(
    session: Any,
    *,
    db_schema: str,
    source_alias_receipt: Mapping[str, Any] | EntityAddressAliasSemanticReceipt,
) -> tuple[EntityAddressAliasSemanticReceipt, EntityAddressAliasSemanticReceipt]:
    """Lock aliases first and require portable equality without counter equality."""

    try:
        source_alias = validate_entity_address_alias_semantic_receipt(source_alias_receipt)
        async with preserve_transaction_sql_settings(
            db,
            ("work_mem",),
            entity_address_unified._sql_literal,
        ):
            destination_alias = await capture_entity_address_alias_semantic_receipt(
                session,
                schema_name=db_schema,
            )
        require_matching_entity_address_alias_semantics(source_alias, destination_alias)
    except (EntityAddressSnapshotAliasError, ValueError) as error:
        raise EntityAddressSnapshotDestinationError(str(error)) from error
    return source_alias, destination_alias


async def _base_version_counts(
    session: Any,
    *,
    schema_name: str,
    source_alias_version: str,
    table_name: str | None = None,
) -> tuple[int, int, int, int]:
    """Classify every source base version into one closed supported set."""

    version_table = table_name or entity_address_unified.EntityAddressUnified.__tablename__
    counts = (
        (
            await session.execute(
                text(
                    f"SELECT COUNT(*) FILTER (WHERE base_address_version IS NULL) AS null_rows, "
                    "COUNT(*) FILTER (WHERE base_address_version = :plain_base) AS plain_rows, "
                    "COUNT(*) FILTER (WHERE base_address_version = :source_alias) AS alias_rows, "
                    "COUNT(*) FILTER (WHERE base_address_version IS NOT NULL "
                    "AND base_address_version <> :plain_base "
                    "AND base_address_version <> :source_alias) AS unsupported_rows "
                    f'FROM "{schema_name}"."{version_table}"'
                ),
                {
                    "plain_base": entity_address_unified.BASE_ADDRESS_VERSION,
                    "source_alias": source_alias_version,
                },
            )
        )
        .mappings()
        .one()
    )
    return tuple(
        int(counts[field_name]) for field_name in ("null_rows", "plain_rows", "alias_rows", "unsupported_rows")
    )


async def _remap_base_versions(
    session: Any,
    *,
    schema_name: str,
    source_alias: EntityAddressAliasSemanticReceipt,
    destination_alias: EntityAddressAliasSemanticReceipt,
    pre_remap_receipt: EntityAddressArchiveReceipt,
) -> tuple[EntityAddressBaseVersionRemapEvidence, EntityAddressArchiveReceipt]:
    """Validate the closed version set and remap only source-bound alias rows."""

    prefix = entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX
    source_version = f"{prefix}{source_alias.local_generation}"
    destination_version = f"{prefix}{destination_alias.local_generation}"
    null_rows, plain_rows, alias_rows, unsupported_rows = await _base_version_counts(
        session,
        schema_name=schema_name,
        source_alias_version=source_version,
    )
    if unsupported_rows:
        raise EntityAddressSnapshotDestinationError(
            "entity-address source base_address_version is unsupported or inconsistent"
        )
    rewritten_rows = 0
    if source_version != destination_version:
        table_name = entity_address_unified.EntityAddressUnified.__tablename__
        update_result = await session.execute(
            text(
                f'UPDATE "{schema_name}"."{table_name}" '
                "SET base_address_version = :destination_alias "
                "WHERE base_address_version = :source_alias"
            ),
            {"source_alias": source_version, "destination_alias": destination_version},
        )
        rewritten_rows = int(update_result.rowcount or 0)
    if rewritten_rows not in {0, alias_rows} or (
        source_version != destination_version and rewritten_rows != alias_rows
    ):
        raise EntityAddressSnapshotDestinationError("entity-address base_address_version remap row count differs")
    async with _preserve_receipt_settings():
        post_remap_receipt = await capture_entity_address_archive_receipt(
            session,
            schema_name=schema_name,
        )
    evidence = EntityAddressBaseVersionRemapEvidence(
        source_alias_generation=source_alias.local_generation,
        destination_alias_generation=destination_alias.local_generation,
        alias_rows_bound=alias_rows,
        alias_rows_rewritten=rewritten_rows,
        null_rows_preserved=null_rows,
        plain_base_rows_preserved=plain_rows,
        pre_remap_content_sha256=pre_remap_receipt.content_sha256,
        post_remap_content_sha256=post_remap_receipt.content_sha256,
    )
    return evidence, post_remap_receipt


def _geo_signature(
    value: object,
    *,
    db_schema: str,
) -> tuple[tuple[str, int, int], ...]:
    """Validate one local dependency signature without assigning portability."""

    expected_names = {
        *(f"{db_schema}.{name}" for name in _LOCAL_GEO_DEPENDENCIES),
        *_SHARED_GEO_DEPENDENCIES,
    }
    if not isinstance(value, Mapping) or set(value) != expected_names:
        raise EntityAddressSnapshotDestinationError("entity-address geo-assurance dependency signature is invalid")
    signature_entries = []
    for relation_name, identity in value.items():
        if (
            not isinstance(identity, (list, tuple))
            or len(identity) != 2
            or any(type(number) is not int or number <= 0 for number in identity)
        ):
            raise EntityAddressSnapshotDestinationError("entity-address geo-assurance dependency signature is invalid")
        signature_entries.append((str(relation_name), identity[0], identity[1]))
    return tuple(sorted(signature_entries))


async def _capture_geo_preparation(
    session: Any,
    *,
    db_schema: str,
    stage_table_oid: int,
    projected_rows: int,
    dependency_bindings=None,
) -> EntityAddressGeoAssurancePreparation:
    """Capture the candidate only when it matches the current local dependencies."""

    state_table = geo_projection.GEO_ASSURANCE_STATE_TABLE
    signature_sql = geo_projection.projection_relation_signature_sql(
        db_schema, **({} if dependency_bindings is None else {"dependency_bindings": dependency_bindings})
    )
    state = (
        (
            await session.execute(
                text(
                    "SELECT candidate_geo_assurance_version, candidate_table_oid::bigint, "
                    "candidate_relation_signature, candidate_projected_rows, "
                    f"{signature_sql} AS current_signature "
                    f'FROM "{db_schema}"."{state_table}" WHERE singleton IS TRUE'
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        state is None
        or state["candidate_geo_assurance_version"] != geo_projection.GEO_ASSURANCE_VERSION
        or state["candidate_table_oid"] != stage_table_oid
        or state["candidate_projected_rows"] != projected_rows
        or state["candidate_relation_signature"] != state["current_signature"]
    ):
        raise EntityAddressSnapshotDestinationError("entity-address geo-assurance candidate is stale")
    return EntityAddressGeoAssurancePreparation(
        stage_table_oid=stage_table_oid,
        projected_rows=projected_rows,
        relation_signature=_geo_signature(
            state["candidate_relation_signature"],
            db_schema=db_schema,
        ),
    )


async def _prepare_moved_destination(
    session: Any,
    *,
    db_schema: str,
    import_date: str,
    stage_names: Mapping[str, str],
    stage_oids: tuple[tuple[str, int], ...],
    source_serving_generation: EntityAddressServingGeneration | None,
    dependency_bindings=None,
) -> tuple[
    adoption.PreparedEntityAddressSnapshotAdoption,
    EntityAddressGeoAssurancePreparation,
    EntityAddressStageIntegrityReceipt,
]:
    """Project real local geo assurance and run native stage validation."""

    main_stage = stage_names[entity_address_unified.EntityAddressUnified.__tablename__]
    stage_oid = dict(stage_oids)[main_stage]
    stage_rows = int(await session.scalar(text(f'SELECT COUNT(*) FROM "{db_schema}"."{main_stage}"')) or 0)
    geo_context_map: dict[str, Any] = {}
    projected_rows = await entity_address_unified._materialize_geo_assurance(
        db_schema,
        main_stage,
        force=True,
        context=geo_context_map,
        run_id="",
        stage_rows=stage_rows,
        **({} if dependency_bindings is None else {"dependency_bindings": dependency_bindings}),
    )
    prepared = await adoption.prepare_completed_entity_address_snapshot_adoption(
        db_schema=db_schema,
        import_date=import_date,
        preserve_unversioned_base_rows=True,
        source_serving_generation=source_serving_generation,
    )
    geo_preparation = await _capture_geo_preparation(
        session,
        db_schema=db_schema,
        stage_table_oid=stage_oid,
        projected_rows=projected_rows,
        **({} if dependency_bindings is None else {"dependency_bindings": dependency_bindings}),
    )
    async with _preserve_receipt_settings():
        stage_integrity = await capture_entity_address_stage_integrity_receipt(
            session,
            schema_name=db_schema,
            stage_table_names=stage_names,
        )
    return prepared, geo_preparation, stage_integrity


async def _validate_owned_source(
    session: Any,
    *,
    owner: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
    semantic_receipt: Mapping[str, Any] | EntityAddressArchiveReceipt,
    db_schema: str,
    import_date: str,
) -> tuple[
    EntityAddressArchiveStageOwnership,
    EntityAddressArchiveReceipt,
    str,
    str,
    dict[str, str],
]:
    """Validate and pin the exact restored source family."""

    try:
        validated_owner = validate_entity_address_archive_stage_ownership(owner)
        source_receipt = validate_entity_address_archive_receipt(semantic_receipt)
        await restore._lock_owned_restore_relations(session, validated_owner)
        validated_owner = await verify_entity_address_archive_stage_ownership(
            session,
            owner=validated_owner,
        )
        normalized_schema, normalized_date, stage_names = restore._stage_plan(
            db_schema=db_schema,
            import_date=import_date,
        )
        async with _preserve_receipt_settings():
            await restore._actual_receipt(
                session,
                schema_name=validated_owner.schema_name,
                expected=source_receipt,
            )
    except (
        EntityAddressArchiveOwnershipError,
        EntityAddressArchiveReceiptError,
        restore.EntityAddressSnapshotRestoreError,
        ValueError,
    ) as error:
        raise EntityAddressSnapshotDestinationError(str(error)) from error
    return validated_owner, source_receipt, normalized_schema, normalized_date, stage_names


async def _move_remapped_destination(
    session: Any,
    *,
    validated_owner: EntityAddressArchiveStageOwnership,
    normalized_schema: str,
    stage_names: Mapping[str, str],
) -> tuple[tuple[str, int], ...]:
    """Move the validated remapped family into its native stage names."""

    await restore._reset_restored_evidence_sequence(session, validated_owner)
    stage_oids = await restore._move_owned_relations(
        session,
        owner=validated_owner,
        db_schema=normalized_schema,
        stage_names=stage_names,
    )
    await restore._verify_moved_stage_oids(
        session,
        db_schema=normalized_schema,
        stage_oids=stage_oids,
    )
    await restore._drop_empty_owned_schema(session, validated_owner)
    return stage_oids


async def prepare_entity_address_archive_destination(
    session: Any,
    *,
    owner: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
    semantic_receipt: Mapping[str, Any] | EntityAddressArchiveReceipt,
    source_alias_receipt: Mapping[str, Any] | EntityAddressAliasSemanticReceipt,
    db_schema: str,
    import_date: str,
    source_serving_generation: Mapping[str, Any] | EntityAddressServingGeneration | None = None,
    dependency_bindings: Mapping[str, Mapping[str, Any]] | None = None,
) -> PreparedEntityAddressSnapshotDestination:
    """Validate, remap, project, and prepare a restored local result.

    ``source_serving_generation`` is the source capture's portable origin
    tuple. ``None`` explicitly selects generation-less manual compatibility.
    ``dependency_bindings`` is trusted local held-relation authority, never
    peer metadata. Its exact physical identity is checked under locks; only
    canonical-key signatures survive preparation for the final live recheck.
    """
    _require_caller_transaction(session)
    if dependency_bindings is not None:
        dependency_bindings = geo_projection.validate_projection_dependency_bindings(db_schema, dependency_bindings)
    async with db.bind_existing_session(session):
        return await _prepare_bound_destination(
            session,
            owner=owner,
            semantic_receipt=semantic_receipt,
            source_alias_receipt=source_alias_receipt,
            db_schema=db_schema,
            import_date=import_date,
            source_serving_generation=source_serving_generation,
            **({} if dependency_bindings is None else {"dependency_bindings": dependency_bindings}),
        )


def _validated_source_generation(
    source_generation: Mapping[str, Any] | EntityAddressServingGeneration | None,
) -> EntityAddressServingGeneration | None:
    """Normalize the source identity into the destination error contract."""

    try:
        if source_generation is None:
            return None
        return validate_entity_address_serving_generation(source_generation)
    except ValueError as error:
        raise EntityAddressSnapshotDestinationError("entity-address source serving generation is invalid") from error


async def _prepare_bound_destination(
    session: Any,
    *,
    owner: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
    semantic_receipt: Mapping[str, Any] | EntityAddressArchiveReceipt,
    source_alias_receipt: Mapping[str, Any] | EntityAddressAliasSemanticReceipt,
    db_schema: str,
    import_date: str,
    source_serving_generation: Mapping[str, Any] | EntityAddressServingGeneration | None,
    dependency_bindings=None,
) -> PreparedEntityAddressSnapshotDestination:
    """Prepare while the module database uses the caller-owned session."""
    validated_source_generation = _validated_source_generation(source_serving_generation)
    source_alias, destination_alias = await _destination_alias_binding(
        session, db_schema=db_schema, source_alias_receipt=source_alias_receipt
    )
    validated_owner, source_receipt, normalized_schema, normalized_date, stage_names = await _validate_owned_source(
        session,
        owner=owner,
        semantic_receipt=semantic_receipt,
        db_schema=db_schema,
        import_date=import_date,
    )
    remap_evidence, post_remap_receipt = await _remap_base_versions(
        session,
        schema_name=validated_owner.schema_name,
        source_alias=source_alias,
        destination_alias=destination_alias,
        pre_remap_receipt=source_receipt,
    )
    stage_oids = await _move_remapped_destination(
        session,
        validated_owner=validated_owner,
        normalized_schema=normalized_schema,
        stage_names=stage_names,
    )
    prepared, geo_preparation, stage_integrity = await _prepare_moved_destination(
        session,
        db_schema=normalized_schema,
        import_date=normalized_date,
        stage_names=stage_names,
        stage_oids=stage_oids,
        source_serving_generation=validated_source_generation,
        **({} if dependency_bindings is None else {"dependency_bindings": dependency_bindings}),
    )
    return PreparedEntityAddressSnapshotDestination(
        restored=_prepared_restore_receipt(
            validated_owner=validated_owner,
            normalized_date=normalized_date,
            stage_oids=stage_oids,
            post_remap_receipt=post_remap_receipt,
            stage_integrity=stage_integrity,
            prepared=prepared,
        ),
        source_semantic_receipt=source_receipt,
        source_alias_receipt=source_alias,
        destination_alias_receipt=destination_alias,
        base_version_remap=remap_evidence,
        geo_assurance=geo_preparation,
    )


def _prepared_restore_receipt(
    *,
    validated_owner: EntityAddressArchiveStageOwnership,
    normalized_date: str,
    stage_oids: tuple[tuple[str, int], ...],
    post_remap_receipt: EntityAddressArchiveReceipt,
    stage_integrity: EntityAddressStageIntegrityReceipt,
    prepared: adoption.PreparedEntityAddressSnapshotAdoption,
) -> restore.PreparedEntityAddressSnapshotRestore:
    """Build the existing native restore contract from destination evidence."""

    return restore.PreparedEntityAddressSnapshotRestore(
        ownership=validated_owner,
        db_schema=prepared.db_schema,
        import_date=normalized_date,
        stage_relation_oids=stage_oids,
        semantic_receipt=post_remap_receipt,
        stage_integrity=stage_integrity,
        prepared=prepared,
        context=restore._json_object(prepared.context, "context"),
        native_validation=restore._json_object(
            prepared.publish_validation,
            "native_validation",
        ),
    )


def _main_table_receipt(
    receipt: EntityAddressArchiveReceipt,
) -> EntityAddressArchiveTableReceipt:
    """Return the main-table member from one validated archive receipt."""

    table_name = entity_address_unified.EntityAddressUnified.__tablename__
    return next(table for table in receipt.tables if table.table_name == table_name)


def _validated_remap_evidence(
    remap_value: object,
    *,
    source_receipt: EntityAddressArchiveReceipt,
    restored_receipt: EntityAddressArchiveReceipt,
    source_alias: EntityAddressAliasSemanticReceipt,
    destination_alias: EntityAddressAliasSemanticReceipt,
) -> EntityAddressBaseVersionRemapEvidence:
    """Bind persisted remap counts and digests to both retained receipts."""

    fields = {
        "contract",
        "source_alias_generation",
        "destination_alias_generation",
        "alias_rows_bound",
        "alias_rows_rewritten",
        "null_rows_preserved",
        "plain_base_rows_preserved",
        "pre_remap_content_sha256",
        "post_remap_content_sha256",
    }
    if not isinstance(remap_value, Mapping) or set(remap_value) != fields:
        raise EntityAddressSnapshotDestinationError("entity-address base_address_version remap receipt is invalid")
    count_fields = (
        "alias_rows_bound",
        "alias_rows_rewritten",
        "null_rows_preserved",
        "plain_base_rows_preserved",
    )
    source_generation = remap_value["source_alias_generation"]
    destination_generation = remap_value["destination_alias_generation"]
    counts_valid = all(type(remap_value[field]) is int and remap_value[field] >= 0 for field in count_fields)
    expected_rewritten = remap_value["alias_rows_bound"] if source_generation != destination_generation else 0
    if (
        remap_value["contract"] != BASE_VERSION_REMAP_CONTRACT
        or source_generation != source_alias.local_generation
        or destination_generation != destination_alias.local_generation
        or not counts_valid
        or remap_value["alias_rows_rewritten"] != expected_rewritten
        or remap_value["pre_remap_content_sha256"] != source_receipt.content_sha256
        or remap_value["post_remap_content_sha256"] != restored_receipt.content_sha256
        or source_receipt.schema_sha256 != restored_receipt.schema_sha256
        or sum(remap_value[field] for field in count_fields if field != "alias_rows_rewritten")
        != _main_table_receipt(source_receipt).row_count
    ):
        raise EntityAddressSnapshotDestinationError("entity-address base_address_version remap receipt is invalid")
    return EntityAddressBaseVersionRemapEvidence(
        source_alias_generation=source_generation,
        destination_alias_generation=destination_generation,
        alias_rows_bound=remap_value["alias_rows_bound"],
        alias_rows_rewritten=remap_value["alias_rows_rewritten"],
        null_rows_preserved=remap_value["null_rows_preserved"],
        plain_base_rows_preserved=remap_value["plain_base_rows_preserved"],
        pre_remap_content_sha256=remap_value["pre_remap_content_sha256"],
        post_remap_content_sha256=remap_value["post_remap_content_sha256"],
    )


def _validated_geo_preparation(
    geo_value: object,
    *,
    db_schema: str,
) -> EntityAddressGeoAssurancePreparation:
    """Validate persisted destination-local geo candidate evidence."""

    fields = {
        "contract",
        "geo_assurance_version",
        "stage_table_oid",
        "projected_rows",
        "relation_signature",
    }
    if (
        not isinstance(geo_value, Mapping)
        or set(geo_value) != fields
        or geo_value["contract"] != GEO_PREPARATION_CONTRACT
        or geo_value["geo_assurance_version"] != geo_projection.GEO_ASSURANCE_VERSION
        or type(geo_value["stage_table_oid"]) is not int
        or geo_value["stage_table_oid"] <= 0
        or type(geo_value["projected_rows"]) is not int
        or geo_value["projected_rows"] < 0
    ):
        raise EntityAddressSnapshotDestinationError("entity-address geo-assurance preparation receipt is invalid")
    return EntityAddressGeoAssurancePreparation(
        stage_table_oid=geo_value["stage_table_oid"],
        projected_rows=geo_value["projected_rows"],
        relation_signature=_geo_signature(
            geo_value["relation_signature"],
            db_schema=db_schema,
        ),
    )


def _require_receipt_lineage(
    source_receipt: EntityAddressArchiveReceipt,
    restored_receipt: EntityAddressArchiveReceipt,
    stage_integrity: EntityAddressStageIntegrityReceipt,
) -> None:
    """Bind unchanged support content and main shape across all three receipts."""

    source_by_model = {table.model_name: table for table in source_receipt.tables}
    restored_by_model = {table.model_name: table for table in restored_receipt.tables}
    stage_by_model = {table.model_name: table for table in stage_integrity.tables}
    main_model = entity_address_unified.EntityAddressUnified.__name__
    if not (
        source_receipt.main_input_sha256 == restored_receipt.main_input_sha256 == stage_integrity.main_input_sha256
    ):
        raise EntityAddressSnapshotDestinationError("entity-address destination main input lineage differs")
    for model_name, source_table in source_by_model.items():
        restored_table = restored_by_model[model_name]
        stage_table = stage_by_model[model_name]
        source_shape = (source_table.schema_sha256, source_table.row_count)
        restored_shape = (restored_table.schema_sha256, restored_table.row_count)
        stage_shape = (stage_table.schema_sha256, stage_table.row_count)
        if source_shape != restored_shape or restored_shape != stage_shape:
            raise EntityAddressSnapshotDestinationError(
                f"entity-address destination receipt lineage differs for {model_name}"
            )
        if model_name != main_model and not (
            source_table.row_sha256 == restored_table.row_sha256 == stage_table.row_sha256
        ):
            raise EntityAddressSnapshotDestinationError("entity-address destination support receipt lineage differs")


def _validated_destination_metadata(
    stored: Mapping[str, Any],
) -> tuple[
    EntityAddressAliasSemanticReceipt,
    EntityAddressAliasSemanticReceipt,
    EntityAddressBaseVersionRemapEvidence,
]:
    """Validate portable and local destination metadata before database work."""

    fields = {
        "contract",
        "restored",
        "source_semantic_receipt",
        "source_alias_receipt",
        "destination_alias_receipt",
        "base_version_remap",
        "geo_assurance",
    }
    if not isinstance(stored, Mapping) or set(stored) != fields or stored.get("contract") != CONTRACT:
        raise EntityAddressSnapshotDestinationError("entity-address destination preparation is invalid")
    try:
        source_receipt = validate_entity_address_archive_receipt(stored["source_semantic_receipt"])
        restored_receipt = validate_entity_address_archive_receipt(stored["restored"]["semantic_receipt"])
        source_alias = validate_entity_address_alias_semantic_receipt(stored["source_alias_receipt"])
        destination_alias = validate_entity_address_alias_semantic_receipt(stored["destination_alias_receipt"])
        require_matching_entity_address_alias_semantics(source_alias, destination_alias)
        _normalized_schema, _normalized_date, stage_names = restore._stage_plan(
            db_schema=stored["restored"]["db_schema"],
            import_date=stored["restored"]["import_date"],
        )
        stage_integrity = validate_entity_address_stage_integrity_receipt(
            stored["restored"]["stage_integrity"],
            stage_table_names=stage_names,
        )
        _require_receipt_lineage(source_receipt, restored_receipt, stage_integrity)
    except (
        EntityAddressArchiveReceiptError,
        EntityAddressSnapshotAliasError,
        EntityAddressSnapshotDestinationError,
        restore.EntityAddressSnapshotRestoreError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        raise EntityAddressSnapshotDestinationError("entity-address destination preparation is invalid") from error
    remap = _validated_remap_evidence(
        stored["base_version_remap"],
        source_receipt=source_receipt,
        restored_receipt=restored_receipt,
        source_alias=source_alias,
        destination_alias=destination_alias,
    )
    return source_alias, destination_alias, remap


def _prepared_main_stage_oid(
    stored: Mapping[str, Any],
    *,
    prepared: adoption.PreparedEntityAddressSnapshotAdoption,
) -> int:
    """Return the stored OID for the native prepared main stage."""

    try:
        stage_oid_entries = stored["restored"]["stage_relation_oids"]
        relation_oid = next(
            entry["oid"] for entry in stage_oid_entries if entry["table_name"] == prepared.stage_cls.__tablename__
        )
    except (KeyError, StopIteration, TypeError) as error:
        raise EntityAddressSnapshotDestinationError("entity-address destination stage identity is invalid") from error
    if type(relation_oid) is not int or relation_oid <= 0:
        raise EntityAddressSnapshotDestinationError("entity-address destination stage identity is invalid")
    return relation_oid


async def _require_prepared_base_versions(
    session: Any,
    *,
    prepared: adoption.PreparedEntityAddressSnapshotAdoption,
    remap_evidence: EntityAddressBaseVersionRemapEvidence,
    destination_alias: EntityAddressAliasSemanticReceipt,
) -> None:
    """Recount the closed post-remap version set on the pinned stage."""

    destination_version = (
        f"{entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX}{destination_alias.local_generation}"
    )
    null_rows, plain_rows, alias_rows, unsupported_rows = await _base_version_counts(
        session,
        schema_name=prepared.db_schema,
        source_alias_version=destination_version,
        table_name=prepared.stage_cls.__tablename__,
    )
    if (
        unsupported_rows
        or null_rows != remap_evidence.null_rows_preserved
        or plain_rows != remap_evidence.plain_base_rows_preserved
        or alias_rows != remap_evidence.alias_rows_bound
    ):
        raise EntityAddressSnapshotDestinationError("entity-address prepared base_address_version evidence differs")


async def activate_entity_address_archive_destination(
    session: Any,
    *,
    stored: Mapping[str, Any],
    callbacks: adoption.EntityAddressSnapshotAdoptionCallbacks,
) -> dict[str, int | dict[str, int]]:
    """Revalidate and conditionally cut over inside the caller-owned transaction."""

    _require_caller_transaction(session)
    async with db.bind_existing_session(session):
        return await _activate_bound_destination(
            session,
            stored=stored,
            callbacks=callbacks,
        )


async def _activate_bound_destination(
    session: Any,
    *,
    stored: Mapping[str, Any],
    callbacks: adoption.EntityAddressSnapshotAdoptionCallbacks,
) -> dict[str, int | dict[str, int]]:
    """Activate while the module database uses the caller-owned session."""

    source_alias, destination_alias, remap_evidence = _validated_destination_metadata(stored)
    current_alias_source, current_alias = await _destination_alias_binding(
        session,
        db_schema=stored["restored"]["db_schema"],
        source_alias_receipt=source_alias,
    )
    if current_alias_source != source_alias or current_alias.as_dict() != destination_alias.as_dict():
        raise EntityAddressSnapshotDestinationError("entity-address destination alias generation changed")
    async with _preserve_receipt_settings():
        prepared = await restore.rehydrate_entity_address_archive_restore(
            session,
            stored=stored["restored"],
        )
    await _require_prepared_base_versions(
        session,
        prepared=prepared,
        remap_evidence=remap_evidence,
        destination_alias=destination_alias,
    )
    await session.execute(text(geo_projection.projection_dependency_lock_sql(prepared.db_schema)))
    expected_geo = _validated_geo_preparation(
        stored["geo_assurance"],
        db_schema=prepared.db_schema,
    )
    if expected_geo.stage_table_oid != _prepared_main_stage_oid(
        stored,
        prepared=prepared,
    ):
        raise EntityAddressSnapshotDestinationError("entity-address geo-assurance stage identity differs")
    actual_geo = await _capture_geo_preparation(
        session,
        db_schema=prepared.db_schema,
        stage_table_oid=expected_geo.stage_table_oid,
        projected_rows=expected_geo.projected_rows,
    )
    if actual_geo != expected_geo:
        raise EntityAddressSnapshotDestinationError("entity-address geo-assurance preparation changed")
    return await adoption.adopt_prepared_entity_address_snapshot(
        prepared,
        callbacks=callbacks,
    )


__all__ = [
    "BASE_VERSION_REMAP_CONTRACT",
    "CONTRACT",
    "GEO_PREPARATION_CONTRACT",
    "EntityAddressBaseVersionRemapEvidence",
    "EntityAddressGeoAssurancePreparation",
    "EntityAddressSnapshotDestinationError",
    "PreparedEntityAddressSnapshotDestination",
    "activate_entity_address_archive_destination",
    "prepare_entity_address_archive_destination",
]
