# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG table creation and schema compatibility helpers."""

from __future__ import annotations

import logging
import os

from db.connection import db
from db.models import (
    ImportLog,
    PTGAllowedItem,
    PTGAllowedPayment,
    PTGAllowedProviderPayment,
    PTGBillingCode,
    PTGProviderGroup,
    PTG2ArtifactManifest,
    PTG2Capability,
    PTG2Confidence,
    PTG2ContentIdentity,
    PTG2CurrentPlanSource,
    PTG2CurrentSnapshot,
    PTG2CurrentSourceSnapshot,
    PTG2FactChunk,
    PTG2GCCandidate,
    PTG2ImportJob,
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
    PTG2ImportJob,
    PTG2ArtifactManifest,
    PTG2Plan,
    PTG2PlanAlias,
    PTG2PlanMonth,
)


async def _ensure_indexes(obj, db_schema: str) -> None:
    if _env_bool(
        PTG2_SKIP_BULK_INDEX_ENSURE_ENV,
        _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, False),
    ) and obj in {
        PTG2PriceSet,
        PTG2ProviderSet,
        PTG2Procedure,
        PTG2ServingRateCompact,
    }:
        logger.info("Skipping PTG2 bulk index ensure for %s before bulk load", obj.__tablename__)
        return
    if (
        obj is PTG2ServingRateCompact
        and _env_bool(
            PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE_ENV,
            _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, True),
        )
    ):
        logger.info("Skipping PTG2 compact serving index ensure before bulk load")
        return
    if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
        index_elements = [str(element) for element in obj.__my_index_elements__]
        if index_elements == _primary_key_column_names(obj):
            logger.debug("Skipping duplicate primary unique index ensure for %s", obj.__tablename__)
        else:
            cols = ", ".join(index_elements)
            await db.status(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                + f"{obj.__tablename__}_idx_primary ON {db_schema}.{obj.__tablename__} ({cols});"
            )
    if hasattr(obj, "__my_additional_indexes__") and obj.__my_additional_indexes__:
        for idx in obj.__my_additional_indexes__:
            elements = idx.get("index_elements")
            if not elements:
                continue
            name = idx.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
            using = idx.get("using")
            where = idx.get("where")
            include_elements = idx.get("include") or ()
            cols = ", ".join(elements)
            statement = f"CREATE INDEX IF NOT EXISTS {name} ON {db_schema}.{obj.__tablename__}"
            if using:
                statement += f" USING {using}"
            statement += f" ({cols})"
            if include_elements:
                statement += f" INCLUDE ({', '.join(include_elements)})"
            if where:
                statement += f" WHERE {where}"
            statement += ";"
            await db.status(statement)


async def _ensure_ptg2_serving_rate_columns(db_schema: str) -> None:
    column_specs = {
        "procedure_code": "bigint",
        "reported_code_system": "varchar(64)",
        "reported_code": "varchar(64)",
        "procedure_display_name": "varchar",
        "source_trace_set_hash": "varchar(64)",
        "network_names": "varchar[]",
        "confidence_code": "varchar(64)",
    }
    for column_name, column_type in column_specs.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_serving_rate "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate column %s ensure: %s", column_name, exc)


async def _ensure_ptg2_serving_rate_compact_columns(db_schema: str) -> None:
    column_specs = {
        "network_names": "varchar[]",
    }
    for column_name, column_type in column_specs.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_serving_rate_compact "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate_compact column %s ensure: %s", column_name, exc)


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
    column_specs = {
        "service_code_set_hash": "varchar(64)",
        "billing_code_modifier_set_hash": "varchar(64)",
    }
    for column_name, column_type in column_specs.items():
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
            await db.status(
                f"ALTER TABLE {_quote_ident(db_schema)}.{_quote_ident(table_name)} "
                f"DROP COLUMN IF EXISTS {_quote_ident(column_name)};"
            )
        except Exception as exc:
            logger.debug("Skipping %s column %s drop: %s", table_name, column_name, exc)


async def _ensure_ptg2_price_set_stage_table(db_schema: str) -> None:
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
            await db.status(f"ALTER TABLE {db_schema}.ptg2_price_set_stage SET UNLOGGED;")
        except Exception as exc:
            logger.debug("Skipping ptg2_price_set_stage unlogged ensure: %s", exc)
    if not _env_bool(PTG2_STAGE_INDEXES_ENV, False):
        return
    try:
        await db.status(
            f"""
            CREATE INDEX IF NOT EXISTS ptg2_price_set_stage_snapshot_idx
            ON {db_schema}.ptg2_price_set_stage (snapshot_id, price_set_hash);
            """
        )
    except Exception as exc:
        logger.debug("Skipping ptg2_price_set_stage index ensure: %s", exc)


async def _ensure_ptg2_serving_rate_stage_table(db_schema: str) -> None:
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
            await db.status(f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage SET UNLOGGED;")
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate_stage unlogged ensure: %s", exc)
    column_specs = {
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
    for column_name, column_type in column_specs.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate_stage column %s ensure: %s", column_name, exc)
    try:
        await db.status(
            f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage "
            "ALTER COLUMN canonical_payload DROP NOT NULL;"
        )
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate_stage canonical_payload nullable ensure: %s", exc)
    if not _env_bool(PTG2_STAGE_INDEXES_ENV, False):
        return
    try:
        await db.status(
            f"""
            CREATE INDEX IF NOT EXISTS ptg2_serving_rate_stage_snapshot_idx
            ON {db_schema}.ptg2_serving_rate_stage (snapshot_id, serving_rate_id);
            """
        )
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate_stage index ensure: %s", exc)


async def ensure_ptg2_tables() -> None:
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
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


async def _prepare_ptg_tables(import_id: str, test_mode: bool) -> dict[str, type]:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        logger.warning("Failed to ensure schema %s exists (%s); falling back to public schema", db_schema, exc)
        db_schema = "public"
    dynamic: dict[str, type] = {}
    for cls in (
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
    ):
        obj = make_class(cls, import_id, schema_override=db_schema)
        dynamic[cls.__name__] = obj
        try:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
        except Exception as exc:
            logger.debug("PTG drop table %s failed: %s", obj.__tablename__, exc)
        try:
            await db.create_table(obj.__table__, checkfirst=True)
        except Exception as exc:
            logger.warning("PTG create table %s failed: %s", obj.__tablename__, exc)
        await _ensure_indexes(obj, db_schema)
    return dynamic
