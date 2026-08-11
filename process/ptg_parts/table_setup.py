# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG table creation and schema compatibility helpers."""

from __future__ import annotations

import logging

from db.connection import db
from db.models import (
    ImportLog,
    PTGAllowedItem,
    PTGAllowedPayment,
    PTGAllowedProviderPayment,
    PTGBillingCode,
    PTGProviderGroup,
    PTG2ArtifactBlobChunk,
    PTG2ArtifactManifest,
    PTG2AllowedAmountItem,
    PTG2AllowedAmountPayment,
    PTG2AllowedAmountPlan,
    PTG2AllowedAmountProviderPayment,
    PTG2Capability,
    PTG2Confidence,
    PTG2ContentIdentity,
    PTG2CurrentPlanSource,
    PTG2CurrentSnapshot,
    PTG2CurrentSourceSnapshot,
    PTG2FactChunk,
    PTG2GCCandidate,
    PTG2ImportRun,
    PTG2LocationSet,
    PTG2LocationSetMember,
    PTG2Plan,
    PTG2PlanAlias,
    PTG2PlanMonth,
    PTG2PlanRateSet,
    PTG2PriceAtom,
    PTG2PriceCodeSet,
    PTG2PriceSet,
    PTG2PriceSetEntry,
    PTG2Procedure,
    PTG2ProviderEntryComponent,
    PTG2ProviderGroup,
    PTG2ProviderGroupMember,
    PTG2ProviderLocation,
    PTG2ProviderSet,
    PTG2ProviderSetComponent,
    PTG2ProviderSetEntry,
    PTG2ProviderSetMember,
    PTG2RatePack,
    PTG2RateSet,
    PTG2RateSetContext,
    PTG2RelatedCodeSet,
    PTG2ServingRate,
    PTG2ServingRateCompact,
    PTG2Snapshot,
    PTG2SourceCatalog,
    PTG2SourceFileVersion,
    PTG2SourceIdentity,
    PTG2SourceTrace,
    PTG2SourceTraceSet,
    PTGFile,
    PTGInNetworkItem,
    PTGNegotiatedPrice,
    PTGNegotiatedRate,
)
from process.ext.utils import get_import_schema, make_class
from process.ptg_parts.config import (
    PTG2_COMPACT_BULK_DROP_INDEXES_ENV,
    PTG2_SKIP_BULK_INDEX_ENSURE_ENV,
    PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE_ENV,
    PTG2_STAGE_INDEXES_ENV,
    PTG2_UNLOGGED_STAGE_ENV,
    _env_bool,
)
from process.ptg_parts.copy_load import _primary_key_column_names
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_gc import (
    require_migration_owned_tables,
    resolve_ptg2_schema,
)
from process.ptg_parts.ptg2_lifecycle_lock import (
    acquire_ptg2_lifecycle_lock,
    configure_ptg2_lifecycle_transaction,
    is_retryable_lifecycle_database_error,
)
from process.ptg_parts.ptg2_layout_candidate import (
    PTG2_LAYOUT_BUILD_CANDIDATE_TABLE,
)
from process.ptg_parts.ptg2_block_build_pins import PTG2_BLOCK_BUILD_PIN_TABLE
from process.ptg_parts.ptg2_plan_catalog_outbox import (
    PTG2_PLAN_CATALOG_OUTBOX_TABLE,
)
from process.ptg_parts.ptg2_legacy_global_projection_queue import (
    PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE,
)
from db.ptg2_v4_attempt_schema import (
    ATTEMPT_FENCE_TABLE,
    ATTEMPT_IMPORT_JOB_TABLE,
    ATTEMPT_STAGE_TABLE,
)

logger = logging.getLogger(__name__)


PTG2_MODEL_CLASSES = (
    PTG2ImportRun,
    PTG2Snapshot,
    PTG2CurrentSnapshot,
    PTG2CurrentSourceSnapshot,
    PTG2CurrentPlanSource,
    PTG2SourceCatalog,
    PTG2SourceIdentity,
    PTG2SourceFileVersion,
    PTG2ContentIdentity,
    PTG2ArtifactManifest,
    PTG2ArtifactBlobChunk,
    PTG2Plan,
    PTG2PlanAlias,
    PTG2PlanMonth,
    PTG2AllowedAmountPlan,
    PTG2AllowedAmountItem,
    PTG2AllowedAmountPayment,
    PTG2AllowedAmountProviderPayment,
)

PTG2_ALLOWED_AMOUNT_MIGRATION_TABLE_NAMES = (
    PTG2AllowedAmountPlan.__tablename__,
    PTG2AllowedAmountItem.__tablename__,
    PTG2AllowedAmountPayment.__tablename__,
    PTG2AllowedAmountProviderPayment.__tablename__,
)
PTG2_V4_ATTEMPT_MIGRATION_TABLE_NAMES = (
    ATTEMPT_FENCE_TABLE,
    ATTEMPT_STAGE_TABLE,
    ATTEMPT_IMPORT_JOB_TABLE,
)
PTG2_ARTIFACT_BLOB_TABLE = "ptg2_artifact_blob_chunk"
_RUNTIME_SCHEMA_CAPABILITY_SQL = (
    "WITH missing_tables AS ("
    "SELECT 'table:' || required.table_name AS capability "
    "FROM unnest(CAST(:table_names AS text[])) AS required(table_name) "
    "WHERE to_regclass(format('%I.%I', CAST(:schema_name AS text), "
    "required.table_name)) IS NULL"
    "), expected_columns(table_name, column_name, udt_name, is_nullable) "
    "AS (VALUES "
    "('ptg2_artifact_blob_chunk', 'artifact_id', 'varchar', 'NO'),"
    "('ptg2_artifact_blob_chunk', 'chunk_no', 'int4', 'NO'),"
    "('ptg2_artifact_blob_chunk', 'compression', 'varchar', 'YES'),"
    "('ptg2_artifact_blob_chunk', 'payload', 'bytea', 'NO'),"
    "('ptg2_artifact_blob_chunk', 'raw_byte_count', 'int4', 'NO'),"
    "('ptg2_artifact_blob_chunk', 'byte_count', 'int4', 'NO'),"
    "('ptg2_artifact_blob_chunk', 'created_at', 'timestamp', 'YES')"
    "), missing_columns AS ("
    "SELECT 'column:' || expected.table_name || '.' || expected.column_name "
    "AS capability FROM expected_columns expected "
    "WHERE to_regclass(format('%I.%I', CAST(:schema_name AS text), "
    "expected.table_name)) IS NOT NULL AND NOT EXISTS ("
    "SELECT 1 FROM information_schema.columns actual "
    "WHERE actual.table_schema = CAST(:schema_name AS text) "
    "AND actual.table_name = expected.table_name "
    "AND actual.column_name = expected.column_name "
    "AND actual.udt_name = expected.udt_name "
    "AND actual.is_nullable = expected.is_nullable)"
    "), missing_indexes AS ("
    "SELECT 'index:ptg2_artifact_blob_artifact_idx' AS capability "
    "WHERE to_regclass(format('%I.%I', CAST(:schema_name AS text), "
    "'ptg2_artifact_blob_chunk')) IS NOT NULL AND NOT EXISTS ("
    "SELECT 1 FROM pg_index index_record "
    "JOIN pg_class index_relation ON index_relation.oid = index_record.indexrelid "
    "JOIN pg_class table_relation ON table_relation.oid = index_record.indrelid "
    "JOIN pg_namespace namespace_record "
    "ON namespace_record.oid = table_relation.relnamespace "
    "WHERE namespace_record.nspname = CAST(:schema_name AS text) "
    "AND table_relation.relname = 'ptg2_artifact_blob_chunk' "
    "AND index_relation.relname = 'ptg2_artifact_blob_artifact_idx' "
    "AND index_record.indisvalid AND index_record.indpred IS NULL "
    "AND index_record.indexprs IS NULL AND ARRAY("
    "SELECT attribute_record.attname "
    "FROM unnest(index_record.indkey::smallint[]) WITH ORDINALITY "
    "AS key_record(attnum, ordinal) JOIN pg_attribute attribute_record "
    "ON attribute_record.attrelid = table_relation.oid "
    "AND attribute_record.attnum = key_record.attnum "
    "WHERE key_record.attnum > 0 ORDER BY key_record.ordinal"
    ") = ARRAY['artifact_id']::name[])"
    "), missing_capabilities AS ("
    "SELECT capability FROM missing_tables UNION ALL "
    "SELECT capability FROM missing_columns UNION ALL "
    "SELECT capability FROM missing_indexes) "
    "SELECT COALESCE(array_agg(capability ORDER BY capability), "
    "ARRAY[]::text[]) FROM missing_capabilities"
)


class PTG2RuntimeSchemaUnavailable(RuntimeError):
    """The bounded hot-path schema capability check could not pass."""

    retryable = True


async def require_ptg2_runtime_schema_ready() -> None:
    """Read one bounded catalog capability snapshot without runtime DDL."""

    schema_name = resolve_ptg2_schema()
    required_table_names = sorted(
        {
            *(model.__tablename__ for model in PTG2_MODEL_CLASSES),
            *PTG2_ALLOWED_AMOUNT_MIGRATION_TABLE_NAMES,
            *PTG2_V4_ATTEMPT_MIGRATION_TABLE_NAMES,
            PTG2_LAYOUT_BUILD_CANDIDATE_TABLE,
            PTG2_BLOCK_BUILD_PIN_TABLE,
            PTG2_PLAN_CATALOG_OUTBOX_TABLE,
            PTG2_ARTIFACT_BLOB_TABLE,
            PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE,
        }
    )
    try:
        async with db.transaction() as session:
            await configure_ptg2_lifecycle_transaction(
                session,
                lock_timeout="100ms",
                statement_timeout="1s",
            )
            missing_result = await session.execute(
                db.text(_RUNTIME_SCHEMA_CAPABILITY_SQL),
                {
                    "schema_name": schema_name,
                    "table_names": required_table_names,
                },
            )
            missing_capabilities = list(missing_result.scalar_one() or ())
    except Exception as exc:
        if not is_retryable_lifecycle_database_error(exc):
            raise
        raise PTG2RuntimeSchemaUnavailable(
            "PTG runtime schema capability read timed out; retry"
        ) from exc
    if missing_capabilities:
        raise PTG2RuntimeSchemaUnavailable(
            "PTG runtime schema is missing migration-owned capabilities: "
            + ", ".join(missing_capabilities)
            + "; run alembic upgrade head"
        )


async def _require_allowed_amount_migration_tables(db_schema: str) -> None:
    table_records = await db.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
           AND table_name = ANY(CAST(:table_names AS text[]))
        """,
        schema_name=db_schema,
        table_names=list(PTG2_ALLOWED_AMOUNT_MIGRATION_TABLE_NAMES),
    )
    present_table_names: set[str] = set()
    for table_record in table_records:
        table_by_field = getattr(table_record, "_mapping", table_record)
        table_name = table_by_field.get("table_name")
        if table_name:
            present_table_names.add(str(table_name))
    missing_table_names = sorted(
        set(PTG2_ALLOWED_AMOUNT_MIGRATION_TABLE_NAMES)
        - present_table_names
    )
    if missing_table_names:
        raise RuntimeError(
            "PTG allowed-amount migration-owned tables are missing from "
            f"schema {db_schema}: {', '.join(missing_table_names)}; "
            "run alembic upgrade head"
        )


async def _require_v4_attempt_migration_tables(db_schema: str) -> None:
    table_records = await db.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
           AND table_name = ANY(CAST(:table_names AS text[]))
        """,
        schema_name=db_schema,
        table_names=list(PTG2_V4_ATTEMPT_MIGRATION_TABLE_NAMES),
    )
    present_table_names = {
        str(table_by_field["table_name"])
        for table_record in table_records
        if (
            table_by_field := getattr(
                table_record,
                "_mapping",
                table_record,
            )
        ).get("table_name")
    }
    missing_table_names = sorted(
        set(PTG2_V4_ATTEMPT_MIGRATION_TABLE_NAMES)
        - present_table_names
    )
    if missing_table_names:
        raise RuntimeError(
            "PTG V4 attempt migration-owned tables are missing from "
            f"schema {db_schema}: {', '.join(missing_table_names)}; "
            "run alembic upgrade head"
        )


async def _is_ptg_table_present(obj, db_schema: str) -> bool:
    relation_name = f"{db_schema}.{obj.__tablename__}"
    try:
        return bool(
            await db.scalar(
                "SELECT to_regclass(:relation_name) IS NOT NULL",
                relation_name=relation_name,
            )
        )
    except Exception as exc:
        logger.debug("Could not check whether PTG table %s exists before index ensure: %s", relation_name, exc)
        return True


async def _ensure_indexes(model_class, db_schema: str) -> None:
    if not await _is_ptg_table_present(model_class, db_schema):
        logger.warning("Skipping PTG index ensure for missing table %s.%s", db_schema, model_class.__tablename__)
        return
    if _env_bool(
        PTG2_SKIP_BULK_INDEX_ENSURE_ENV,
        _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, False),
    ) and model_class in {
        PTG2PriceSet,
        PTG2ProviderSet,
        PTG2Procedure,
        PTG2ServingRateCompact,
    }:
        logger.info("Skipping PTG2 bulk index ensure for %s before bulk load", model_class.__tablename__)
        return
    if (
        model_class is PTG2ServingRateCompact
        and _env_bool(
            PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE_ENV,
            _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, True),
        )
    ):
        logger.info("Skipping PTG2 compact serving index ensure before bulk load")
        return
    if hasattr(model_class, "__my_index_elements__") and model_class.__my_index_elements__:
        index_elements = [str(element) for element in model_class.__my_index_elements__]
        if index_elements == _primary_key_column_names(model_class):
            logger.debug("Skipping duplicate primary unique index ensure for %s", model_class.__tablename__)
        else:
            cols = ", ".join(index_elements)
            await _create_index_if_not_exists(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                + f"{model_class.__tablename__}_idx_primary ON {db_schema}.{model_class.__tablename__} ({cols});",
                index_name=f"{model_class.__tablename__}_idx_primary",
            )
    if hasattr(model_class, "__my_additional_indexes__") and model_class.__my_additional_indexes__:
        for idx in model_class.__my_additional_indexes__:
            elements = idx.get("index_elements")
            if not elements:
                continue
            name = idx.get("name") or f"{model_class.__tablename__}_{'_'.join(elements)}_idx"
            using = idx.get("using")
            where = idx.get("where")
            include_elements = idx.get("include") or ()
            cols = ", ".join(elements)
            statement = f"CREATE INDEX IF NOT EXISTS {name} ON {db_schema}.{model_class.__tablename__}"
            if using:
                statement += f" USING {using}"
            statement += f" ({cols})"
            if include_elements:
                statement += f" INCLUDE ({', '.join(include_elements)})"
            if where:
                statement += f" WHERE {where}"
            statement += ";"
            await _create_index_if_not_exists(statement, index_name=name)


async def _create_index_if_not_exists(statement: str, *, index_name: str) -> None:
    try:
        await db.status(statement)
    except Exception as exc:
        if _is_concurrent_index_exists_race(exc, index_name):
            logger.info("Skipping concurrent PTG index ensure race for %s", index_name)
            return
        raise


def _is_concurrent_index_exists_race(exc: Exception, index_name: str) -> bool:
    message = str(exc).lower()
    normalized_name = str(index_name or "").lower()
    if normalized_name and normalized_name not in message:
        return False
    return (
        "duplicate key value violates unique constraint" in message
        and "pg_class_relname_nsp_index" in message
    ) or "already exists" in message


async def _ensure_ptg2_serving_rate_columns(db_schema: str) -> None:
    column_types_by_name = {
        "procedure_code": "bigint",
        "reported_code_system": "varchar(64)",
        "reported_code": "varchar(64)",
        "procedure_display_name": "varchar",
        "source_trace_set_hash": "varchar(64)",
        "network_names": "varchar[]",
        "confidence_code": "varchar(64)",
    }
    for column_name, column_type in column_types_by_name.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_serving_rate "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate column %s ensure: %s", column_name, exc)


async def _ensure_rate_compact_columns(db_schema: str) -> None:
    column_types_by_name = {
        "network_names": "varchar[]",
    }
    for column_name, column_type in column_types_by_name.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_serving_rate_compact "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate_compact column %s ensure: %s", column_name, exc)


_ensure_ptg2_serving_rate_compact_columns = _ensure_rate_compact_columns


async def _ensure_ptg2_provider_set_columns(db_schema: str) -> None:
    for column_name in ("hash_prefix", "npi", "provider_group_hashes", "tin_type", "tin_value", "canonical_payload"):
        try:
            await db.status(f"ALTER TABLE {db_schema}.ptg2_provider_set DROP COLUMN IF EXISTS {column_name};")
        except Exception as exc:
            logger.debug("Skipping ptg2_provider_set column %s drop: %s", column_name, exc)


async def _ensure_ptg2_price_set_columns(db_schema: str) -> None:
    for column_name in (
        "hash_prefix",
        "price_atom_hashes",
        "negotiated_type",
        "negotiated_rate",
        "expiration_date",
        "service_code",
        "billing_class",
        "setting",
        "billing_code_modifier",
        "additional_information",
        "canonical_payload",
    ):
        try:
            await db.status(f"ALTER TABLE {db_schema}.ptg2_price_set DROP COLUMN IF EXISTS {column_name};")
        except Exception as exc:
            logger.debug("Skipping ptg2_price_set column %s drop: %s", column_name, exc)


async def _ensure_ptg2_price_atom_columns(db_schema: str) -> None:
    column_types_by_name = {
        "service_code_set_hash": "varchar(64)",
        "billing_code_modifier_set_hash": "varchar(64)",
    }
    for column_name, column_type in column_types_by_name.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_price_atom "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_price_atom column %s ensure: %s", column_name, exc)
    await _drop_ptg2_columns(
        db_schema,
        "ptg2_price_atom",
        ("hash_prefix", "canonical_payload", "service_code", "billing_code_modifier"),
    )


async def _drop_ptg2_columns(db_schema: str, table_name: str, column_names: tuple[str, ...]) -> None:
    for column_name in column_names:
        try:
            async with db.transaction():
                await db.status(
                    f"ALTER TABLE {_quote_ident(db_schema)}.{_quote_ident(table_name)} "
                    f"DROP COLUMN IF EXISTS {_quote_ident(column_name)};"
                )
        except Exception as exc:
            logger.debug("Skipping %s column %s drop: %s", table_name, column_name, exc)


async def _ensure_price_stage_table_locked(db_schema: str) -> None:
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    await db.status(
        f"""
        CREATE {storage_mode}TABLE IF NOT EXISTS {db_schema}.ptg2_price_set_stage (
            snapshot_id varchar(96) NOT NULL,
            price_set_hash varchar(64) NOT NULL,
            created_at timestamp
        );
        """
    )
    await _drop_ptg2_columns(db_schema, "ptg2_price_set_stage", ("hash_prefix", "price_atom_hashes", "canonical_payload"))
    if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True):
        try:
            async with db.transaction():
                await db.status(f"ALTER TABLE {db_schema}.ptg2_price_set_stage SET UNLOGGED;")
        except Exception as exc:
            logger.debug("Skipping ptg2_price_set_stage unlogged ensure: %s", exc)
    if not _env_bool(PTG2_STAGE_INDEXES_ENV, False):
        return
    try:
        async with db.transaction():
            await db.status(
                f"""
                CREATE INDEX IF NOT EXISTS ptg2_price_set_stage_snapshot_idx
                ON {db_schema}.ptg2_price_set_stage (snapshot_id, price_set_hash);
                """
            )
    except Exception as exc:
        logger.debug("Skipping ptg2_price_set_stage index ensure: %s", exc)


async def _ensure_price_stage_table(db_schema: str) -> None:
    """Serialize price-stage DDL with PTG lifecycle mutations."""

    async with db.transaction() as session:
        await acquire_ptg2_lifecycle_lock(session)
        await _ensure_price_stage_table_locked(db_schema)


_ensure_ptg2_price_set_stage_table = _ensure_price_stage_table


async def _ensure_rate_stage_table_locked(db_schema: str) -> None:
    """Create and configure the PTG2 staging table for serving-rate rows."""
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    await db.status(
        f"""
        CREATE {storage_mode}TABLE IF NOT EXISTS {db_schema}.ptg2_serving_rate_stage (
            snapshot_id varchar(96) NOT NULL,
            serving_rate_id varchar(64) NOT NULL,
            canonical_payload json,
            plan_id varchar(64),
            plan_name varchar,
            plan_id_type varchar(32),
            plan_market_type varchar(32),
            issuer_name varchar,
            plan_sponsor_name varchar,
            procedure_code bigint,
            reported_code_system varchar(64),
            reported_code varchar(64),
            billing_code varchar(64),
            billing_code_type varchar(64),
            procedure_name varchar,
            procedure_description varchar,
            procedure_display_name varchar,
            rate_pack_hash varchar(64),
            provider_set_hash varchar(64),
            provider_set_hashes varchar[],
            provider_count integer,
            provider_set_count integer,
            price_set_hash varchar(64),
            source_trace_set_hash varchar(64),
            network_names varchar[],
            confidence_code varchar(64),
            prices json,
            source_trace json,
            confidence json,
            created_at timestamp
        );
        """
    )
    if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True):
        try:
            async with db.transaction():
                await db.status(f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage SET UNLOGGED;")
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate_stage unlogged ensure: %s", exc)
    column_types_by_name = {
        "canonical_payload": "json",
        "plan_id": "varchar(64)",
        "plan_name": "varchar",
        "plan_id_type": "varchar(32)",
        "plan_market_type": "varchar(32)",
        "issuer_name": "varchar",
        "plan_sponsor_name": "varchar",
        "procedure_code": "bigint",
        "reported_code_system": "varchar(64)",
        "reported_code": "varchar(64)",
        "billing_code": "varchar(64)",
        "billing_code_type": "varchar(64)",
        "procedure_name": "varchar",
        "procedure_description": "varchar",
        "procedure_display_name": "varchar",
        "rate_pack_hash": "varchar(64)",
        "provider_set_hash": "varchar(64)",
        "provider_set_hashes": "varchar[]",
        "provider_count": "integer",
        "provider_set_count": "integer",
        "price_set_hash": "varchar(64)",
        "source_trace_set_hash": "varchar(64)",
        "network_names": "varchar[]",
        "confidence_code": "varchar(64)",
        "prices": "json",
        "source_trace": "json",
        "confidence": "json",
    }
    for column_name, column_type in column_types_by_name.items():
        try:
            async with db.transaction():
                await db.status(
                    f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage "
                    f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
                )
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate_stage column %s ensure: %s", column_name, exc)
    try:
        async with db.transaction():
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage "
                "ALTER COLUMN canonical_payload DROP NOT NULL;"
            )
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate_stage canonical_payload nullable ensure: %s", exc)
    if not _env_bool(PTG2_STAGE_INDEXES_ENV, False):
        return
    try:
        async with db.transaction():
            await db.status(
                f"""
                CREATE INDEX IF NOT EXISTS ptg2_serving_rate_stage_snapshot_idx
                ON {db_schema}.ptg2_serving_rate_stage (snapshot_id, serving_rate_id);
                """
            )
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate_stage index ensure: %s", exc)


async def _ensure_rate_stage_table(db_schema: str) -> None:
    """Serialize serving-stage DDL with PTG lifecycle mutations."""

    async with db.transaction() as session:
        await acquire_ptg2_lifecycle_lock(session)
        await _ensure_rate_stage_table_locked(db_schema)


_ensure_ptg2_serving_rate_stage_table = _ensure_rate_stage_table


async def ensure_ptg2_tables() -> None:
    """Create PTG2 tables and apply their required schema migrations."""
    db_schema = resolve_ptg2_schema()
    await require_migration_owned_tables(db, db_schema)
    await _require_v4_attempt_migration_tables(db_schema)
    await _require_allowed_amount_migration_tables(db_schema)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        raise RuntimeError(f"Failed to ensure PTG2 schema {db_schema}: {exc}") from exc
    for cls in PTG2_MODEL_CLASSES:
        try:
            await db.create_table(cls.__table__, checkfirst=True)
        except Exception as exc:
            raise RuntimeError(f"PTG2 create table {db_schema}.{cls.__tablename__} failed: {exc}") from exc
        if cls is PTG2ServingRate:
            await _ensure_ptg2_serving_rate_columns(db_schema)
        if cls is PTG2ServingRateCompact:
            await _ensure_ptg2_serving_rate_compact_columns(db_schema)
        if cls is PTG2PriceSet:
            await _ensure_ptg2_price_set_columns(db_schema)
        if cls is PTG2ProviderSet:
            await _ensure_ptg2_provider_set_columns(db_schema)
        if cls is PTG2ProviderSetMember:
            await _drop_ptg2_columns(db_schema, "ptg2_provider_set_member", ("ordinal",))
        if cls is PTG2Procedure:
            await _drop_ptg2_columns(db_schema, "ptg2_procedure", ("hash_prefix", "canonical_payload"))
        if cls is PTG2PriceAtom:
            await _ensure_ptg2_price_atom_columns(db_schema)
        if cls is PTG2PriceSetEntry:
            await _drop_ptg2_columns(db_schema, "ptg2_price_set_entry", ("ordinal",))
        if cls is PTG2ProviderGroupMember:
            await _drop_ptg2_columns(db_schema, "ptg2_provider_group_member", ("ordinal",))
        if cls is PTG2ProviderSetComponent:
            await _drop_ptg2_columns(db_schema, "ptg2_provider_set_component", ("ordinal",))
        if cls is PTG2ProviderSetEntry:
            await _drop_ptg2_columns(db_schema, "ptg2_provider_set_entry", ("ordinal",))
        if cls is PTG2ProviderEntryComponent:
            await _drop_ptg2_columns(db_schema, "ptg2_provider_entry_component", ("ordinal",))
        if cls is PTG2SourceTrace:
            await _drop_ptg2_columns(db_schema, "ptg2_source_trace", ("hash_prefix", "canonical_payload"))
        if cls is PTG2SourceTraceSet:
            await _drop_ptg2_columns(db_schema, "ptg2_source_trace_set", ("hash_prefix", "canonical_payload"))
        await _ensure_indexes(cls, db_schema)


PTG_DYNAMIC_TABLE_CLASSES = (
    PTGFile,
    PTGProviderGroup,
    PTGInNetworkItem,
    PTGBillingCode,
    PTGNegotiatedRate,
    PTGNegotiatedPrice,
    PTGAllowedItem,
    PTGAllowedPayment,
    PTGAllowedProviderPayment,
    ImportLog,
)

PTG_CONTROL_TABLE_CLASS_NAMES = frozenset({"PTGFile", "ImportLog"})
PTG_PROVIDER_REFERENCE_TABLE_CLASS_NAMES = frozenset({"PTGProviderGroup"})
PTG_ALLOWED_AMOUNT_TABLE_CLASS_NAMES = frozenset(
    {"PTGAllowedItem", "PTGAllowedPayment", "PTGAllowedProviderPayment"}
)
PTG_IN_NETWORK_DENSE_TABLE_CLASS_NAMES = frozenset(
    {
        "PTGProviderGroup",
        "PTGInNetworkItem",
        "PTGBillingCode",
        "PTGNegotiatedRate",
        "PTGNegotiatedPrice",
    }
)


async def _ensure_ptg_dynamic_tables(
    classes: dict[str, type],
    class_names: set[str] | frozenset[str],
    *,
    test_mode: bool,
) -> None:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        logger.warning("Failed to ensure schema %s exists (%s); falling back to public schema", db_schema, exc)
        db_schema = "public"
    requested_class_names = set(class_names)
    for cls in PTG_DYNAMIC_TABLE_CLASSES:
        if cls.__name__ not in requested_class_names:
            continue
        obj = classes[cls.__name__]
        if cls.__name__ not in PTG_CONTROL_TABLE_CLASS_NAMES:
            try:
                await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
            except Exception as exc:
                logger.debug("PTG drop table %s failed: %s", obj.__tablename__, exc)
        try:
            await db.create_table(obj.__table__, checkfirst=True)
        except Exception as exc:
            logger.warning("PTG create table %s failed: %s", obj.__tablename__, exc)
        await _ensure_indexes(obj, db_schema)


async def _prepare_ptg_tables(
    import_id: str,
    test_mode: bool,
    *,
    initial_table_class_names: set[str] | frozenset[str] | None = None,
) -> dict[str, type]:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        logger.warning("Failed to ensure schema %s exists (%s); falling back to public schema", db_schema, exc)
        db_schema = "public"
    dynamic_classes_by_name: dict[str, type] = {}
    for cls in PTG_DYNAMIC_TABLE_CLASSES:
        dynamic_classes_by_name[cls.__name__] = (
            cls
            if cls.__name__ in PTG_CONTROL_TABLE_CLASS_NAMES
            else make_class(cls, import_id, schema_override=db_schema)
        )
    requested_class_names = set(initial_table_class_names) if initial_table_class_names is not None else {
        cls.__name__ for cls in PTG_DYNAMIC_TABLE_CLASSES
    }
    if requested_class_names:
        await _ensure_ptg_dynamic_tables(
            dynamic_classes_by_name,
            requested_class_names,
            test_mode=test_mode,
        )
    return dynamic_classes_by_name
