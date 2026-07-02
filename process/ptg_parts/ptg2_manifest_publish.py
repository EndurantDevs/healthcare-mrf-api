# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 source-scoped manifest-backed serving stage and publish helpers."""

from __future__ import annotations

import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.config import (
    PTG2_BINARY_IDS_ENV, PTG2_MANIFEST_PUBLISH_DB_DEDUPE_FALLBACK_ENV,
    PTG2_UNLOGGED_STAGE_ENV, _env_bool)
from process.ptg_parts.db_tables import (_exact_table_rows, _quote_ident,
                                         _table_exists, _table_has_rows)
from process.ptg_parts.ptg2_manifest_artifacts import read_global_sidecar_entries
from process.ptg_parts.snapshot_tables import (_ptg2_snapshot_index_name,
                                               _ptg2_snapshot_table_name)

PTG2_MANIFEST_SERVING_COPY_ENV = "HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH"
logger = logging.getLogger(__name__)
PTG2_MANIFEST_SERVING_COLUMNS = [
    "serving_content_hash_128",
    "plan_id",
    "reported_code_system",
    "reported_code",
    "procedure_global_id_128",
    "provider_set_global_id_128",
    "provider_count",
    "price_set_global_id_128",
    "source_trace_set_hash",
    "network_names",
]
PTG2_MANIFEST_PRICE_ATOM_COLUMNS = [
    "price_atom_global_id_128",
    "negotiated_type",
    "negotiated_rate",
    "expiration_date",
    "service_code",
    "billing_class",
    "setting",
    "billing_code_modifier",
    "additional_information",
]
PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COLUMNS = [
    "provider_group_global_id_128",
    "npi",
]
PTG2_MANIFEST_PROVIDER_SET_COMPONENT_COLUMNS = [
    "provider_set_global_id_128",
    "provider_group_global_id_128",
]


def _ptg2_id_storage() -> str:
    return "uuid" if _env_bool(PTG2_BINARY_IDS_ENV, True) else "hex"


def _ptg2_id_sql_type() -> str:
    return "uuid" if _ptg2_id_storage() == "uuid" else "char(32)"


def _ptg2_manifest_stage_table_name(token: str) -> str:
    safe_token = "".join(ch if ch.isalnum() else "_" for ch in token.lower()).strip("_")
    return f"ptg2_manifest_stage_serving_{safe_token}"[:63]


def _ptg2_manifest_stage_suffix(serving_stage_table: str) -> str:
    prefix = "ptg2_manifest_stage_serving_"
    return serving_stage_table[len(prefix):] if serving_stage_table.startswith(prefix) else serving_stage_table


def _ptg2_manifest_support_stage_table(serving_stage_table: str, kind: str) -> str:
    return f"ptg2_manifest_stage_{kind}_{_ptg2_manifest_stage_suffix(serving_stage_table)}"[:63]


async def _create_ptg2_manifest_serving_stage_table(token: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_stage_table_name(token)
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            serving_content_hash_128 {id_type} NOT NULL,
            plan_id varchar(64) NOT NULL,
            procedure_global_id_128 {id_type} NOT NULL,
            reported_code_system varchar(64),
            reported_code varchar(64),
            provider_set_global_id_128 {id_type} NOT NULL,
            provider_count integer,
            price_set_global_id_128 {id_type} NOT NULL,
            source_trace_set_hash varchar(64),
            network_names varchar[] NOT NULL DEFAULT '{{}}'
        );
        """
    )
    try:
        await db.status(
            f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} "
            "SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);"
        )
    except Exception as exc:
        logger.debug("failed to disable autovacuum on PTG2 manifest stage table %s: %s", stage_table, exc)
    await _create_ptg2_manifest_price_atom_stage_table(stage_table)
    await _create_ptg2_manifest_provider_group_member_stage_table(stage_table)
    return stage_table


async def _create_ptg2_manifest_price_atom_stage_table(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(serving_stage_table, "price_atom")
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            price_atom_global_id_128 {id_type} NOT NULL,
            negotiated_type varchar(64),
            negotiated_rate varchar(64),
            expiration_date varchar(32),
            service_code text[] NOT NULL DEFAULT '{{}}',
            billing_class varchar(64),
            setting varchar(64),
            billing_code_modifier text[] NOT NULL DEFAULT '{{}}',
            additional_information text
        );
        """
    )
    return stage_table


async def _create_ptg2_manifest_provider_group_member_stage_table(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(serving_stage_table, "provider_group_member")
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            provider_group_global_id_128 {id_type} NOT NULL,
            npi bigint NOT NULL
        );
        """
    )
    return stage_table


async def _copy_ptg2_manifest_serving_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_SERVING_COLUMNS)


async def _copy_ptg2_manifest_price_atom_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_PRICE_ATOM_COLUMNS)


async def _copy_ptg2_manifest_provider_group_member_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COLUMNS)


async def _copy_manifest_component_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(
        copy_path,
        target_table=target_table,
        columns=PTG2_MANIFEST_PROVIDER_SET_COMPONENT_COLUMNS,
    )


async def _copy_ptg2_manifest_file(copy_path: Path, *, target_table: str, columns: list[str]) -> None:
    if not copy_path.exists() or copy_path.stat().st_size <= 0:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("Active database driver does not expose copy_to_table")
        with copy_path.open("rb") as source:
            await copy_to_table(
                target_table,
                source=source,
                schema_name=schema_name,
                columns=columns,
                format="text",
                delimiter="\t",
                null="\\N",
            )


def _looks_like_unique_index_duplicate(exc: Exception) -> bool:
    parts = [str(exc)]
    orig = getattr(exc, "orig", None)
    if orig is not None:
        parts.append(str(orig))
        cause = getattr(orig, "__cause__", None)
        if cause is not None:
            parts.append(str(cause))
    message = " ".join(parts).lower()
    return (
        "uniqueviolation" in message
        or "unique violation" in message
        or (
            "could not create unique index" in message
            and ("duplicate" in message or "duplicated" in message)
        )
    )


def _manifest_provider_inverted_entry(
    sidecar_artifacts: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not sidecar_artifacts:
        return None
    direct = sidecar_artifacts.get("provider_inverted")
    if isinstance(direct, Mapping):
        return dict(direct)
    for value in sidecar_artifacts.values():
        if isinstance(value, Mapping) and value.get("name") == "provider_inverted":
            return dict(value)
    return None


def _ptg2_manifest_publish_sidecar_path(
    entry: Mapping[str, Any],
    artifacts: Mapping[str, Any] | None,
) -> Path:
    raw_path = str(entry.get("path") or "").strip()
    path = Path(raw_path)
    if path.is_absolute() or path.exists():
        return path
    manifest_uri = str((artifacts or {}).get("manifest_uri") or "").strip()
    if manifest_uri.startswith("file://"):
        manifest_path = Path(manifest_uri.removeprefix("file://"))
        candidate = manifest_path.parent / path
        if candidate.exists():
            return candidate
    return path


def _manifest_sql_id(value: bytes) -> str:
    if _ptg2_id_storage() == "uuid":
        return str(uuid.UUID(bytes=value))
    return value.hex()


def _write_manifest_component_copy(
    sidecar_path: Path,
    sidecar_entry: Mapping[str, Any],
) -> tuple[Path | None, int]:
    tmp_path: Path | None = None
    row_count = 0
    with tempfile.NamedTemporaryFile("wb", delete=False) as component_file:
        tmp_path = Path(component_file.name)
        for sidecar_owner in read_global_sidecar_entries(sidecar_path, metadata=sidecar_entry):
            provider_group_id = _manifest_sql_id(sidecar_owner.owner)
            for member in sidecar_owner.members:
                provider_set_id = _manifest_sql_id(member)
                component_file.write(f"{provider_set_id}\t{provider_group_id}\n".encode("ascii"))
                row_count += 1
    return tmp_path, row_count


async def _index_manifest_components(schema_name: str, table_name: str) -> None:
    primary_index = _ptg2_snapshot_index_name(table_name, "primary")
    group_index = _ptg2_snapshot_index_name(table_name, "group_idx")
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(primary_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(table_name)}
        (provider_set_global_id_128, provider_group_global_id_128);
        """
    )
    await db.status(
        f"""
        CREATE INDEX {_quote_ident(group_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(table_name)}
        (provider_group_global_id_128, provider_set_global_id_128);
        """
    )


async def _materialize_manifest_components(
    *,
    schema_name: str,
    table_name: str,
    artifacts: Mapping[str, Any] | None,
    sidecar_artifacts: Mapping[str, Any] | None,
) -> str | None:
    """Persist provider-inverted sidecar membership as an indexed SQL table."""

    sidecar_entry = _manifest_provider_inverted_entry(sidecar_artifacts)
    if not sidecar_entry:
        return None
    if not str(sidecar_entry.get("path") or "").strip():
        return None
    sidecar_path = _ptg2_manifest_publish_sidecar_path(sidecar_entry, artifacts)
    if not sidecar_path.exists() or sidecar_path.stat().st_size <= 0:
        return None

    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)} CASCADE;")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(table_name)} (
            provider_set_global_id_128 {id_type} NOT NULL,
            provider_group_global_id_128 {id_type} NOT NULL
        );
        """
    )

    copy_path: Path | None = None
    try:
        copy_path, row_count = _write_manifest_component_copy(sidecar_path, sidecar_entry)
        if row_count <= 0 or copy_path is None:
            await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)} CASCADE;")
            return None
        await _copy_manifest_component_file(copy_path, target_table=table_name)
    finally:
        if copy_path is not None:
            try:
                copy_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Failed to remove PTG2 provider-set component copy file: %s", copy_path)

    await _index_manifest_components(schema_name, table_name)
    return table_name


async def _dedupe_ptg2_manifest_serving_table(schema_name: str, final_table: str) -> dict[str, int]:
    serving_rows_before_dedupe = await _exact_table_rows(schema_name, final_table)
    dedup_table = _ptg2_snapshot_index_name(final_table, "dedup")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(dedup_table)};")
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(dedup_table)} AS
        SELECT DISTINCT ON (serving_content_hash_128)
            serving_content_hash_128,
            plan_id,
            procedure_global_id_128,
            reported_code_system,
            reported_code,
            provider_set_global_id_128,
            provider_count,
            price_set_global_id_128,
            source_trace_set_hash,
            network_names
        FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        ORDER BY serving_content_hash_128, source_trace_set_hash NULLS LAST;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(final_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(dedup_table)}
        RENAME TO {_quote_ident(final_table)};
        """
    )
    serving_rows_after_dedupe = await _exact_table_rows(schema_name, final_table)
    return {
        "before": serving_rows_before_dedupe,
        "after": serving_rows_after_dedupe,
        "dropped": max(serving_rows_before_dedupe - serving_rows_after_dedupe, 0),
    }


async def _dedupe_ptg2_manifest_price_atom_table(schema_name: str, price_atom_table: str) -> dict[str, int]:
    price_atom_rows_before_dedupe = await _exact_table_rows(schema_name, price_atom_table)
    price_atom_dedup_table = _ptg2_snapshot_index_name(price_atom_table, "dedup")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(price_atom_dedup_table)};")
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_dedup_table)} AS
        SELECT DISTINCT ON (price_atom_global_id_128)
            price_atom_global_id_128,
            negotiated_type,
            negotiated_rate,
            expiration_date,
            service_code,
            billing_class,
            setting,
            billing_code_modifier,
            additional_information
        FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
        ORDER BY price_atom_global_id_128;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_dedup_table)}
        RENAME TO {_quote_ident(price_atom_table)};
        """
    )
    price_atom_rows_after_dedupe = await _exact_table_rows(schema_name, price_atom_table)
    return {
        "before": price_atom_rows_before_dedupe,
        "after": price_atom_rows_after_dedupe,
        "dropped": max(price_atom_rows_before_dedupe - price_atom_rows_after_dedupe, 0),
    }


async def _dedupe_ptg2_manifest_provider_group_member_table(
    schema_name: str,
    provider_group_member_table: str,
) -> dict[str, int]:
    provider_group_member_rows_before_dedupe = await _exact_table_rows(schema_name, provider_group_member_table)
    provider_group_member_dedup_table = _ptg2_snapshot_index_name(provider_group_member_table, "dedup")
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_dedup_table)};"
    )
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_dedup_table)} AS
        SELECT DISTINCT ON (provider_group_global_id_128, npi)
            provider_group_global_id_128,
            npi
        FROM {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)}
        ORDER BY provider_group_global_id_128, npi;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_dedup_table)}
        RENAME TO {_quote_ident(provider_group_member_table)};
        """
    )
    provider_group_member_rows_after_dedupe = await _exact_table_rows(schema_name, provider_group_member_table)
    return {
        "before": provider_group_member_rows_before_dedupe,
        "after": provider_group_member_rows_after_dedupe,
        "dropped": max(
            provider_group_member_rows_before_dedupe - provider_group_member_rows_after_dedupe,
            0,
        ),
    }


async def _publish_ptg2_manifest_serving_snapshot(
    stage_table: str,
    *,
    snapshot_id: str,
    source_key: str,
    artifacts: dict[str, Any] | None = None,
    sidecar_artifacts: Mapping[str, Any] | None = None,
    db_dedupe_fallback: bool | None = None,
) -> dict[str, Any]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if not await _table_exists(schema_name, stage_table):
        raise RuntimeError(f"PTG2 serving stage table does not exist: {schema_name}.{stage_table}")
    if not await _table_has_rows(schema_name, stage_table):
        raise RuntimeError(f"PTG2 serving stage table is empty: {schema_name}.{stage_table}")

    final_table = _ptg2_snapshot_table_name("serving", source_key, snapshot_id)
    price_atom_stage = _ptg2_manifest_support_stage_table(stage_table, "price_atom")
    provider_group_member_stage = _ptg2_manifest_support_stage_table(stage_table, "provider_group_member")
    price_atom_table = _ptg2_snapshot_table_name("price_atom", source_key, snapshot_id)
    provider_group_member_table = _ptg2_snapshot_table_name("provider_group_member", source_key, snapshot_id)
    provider_set_component_table = _ptg2_snapshot_table_name("provider_set_component", source_key, snapshot_id)
    code_count_table = _ptg2_snapshot_table_name("code_count", source_key, snapshot_id)
    started_at = time.monotonic()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(final_table)} CASCADE;")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)} CASCADE;")
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)} CASCADE;"
    )
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_set_component_table)} CASCADE;"
    )
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} CASCADE;")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
        RENAME TO {_quote_ident(final_table)};
        """
    )
    if await _table_exists(schema_name, price_atom_stage):
        await db.status(
            f"""
            ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_stage)}
            RENAME TO {_quote_ident(price_atom_table)};
            """
        )
    if await _table_exists(schema_name, provider_group_member_stage):
        await db.status(
            f"""
            ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_stage)}
            RENAME TO {_quote_ident(provider_group_member_table)};
            """
        )
    use_db_dedupe = (
        _env_bool(PTG2_MANIFEST_PUBLISH_DB_DEDUPE_FALLBACK_ENV, True)
        if db_dedupe_fallback is None
        else bool(db_dedupe_fallback)
    )
    dedupe_metrics: dict[str, Any] = {"db_dedupe": use_db_dedupe}
    serving_deduped = False
    if use_db_dedupe:
        dedupe_metrics["serving"] = await _dedupe_ptg2_manifest_serving_table(schema_name, final_table)
        serving_deduped = True
    unique_index = _ptg2_snapshot_index_name(final_table, "content_uidx")
    lookup_index = _ptg2_snapshot_index_name(final_table, "plan_code_lookup_idx")
    try:
        await db.status(
            f"""
            CREATE UNIQUE INDEX {_quote_ident(unique_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
            (serving_content_hash_128);
            """
        )
    except Exception as exc:
        if serving_deduped or not _looks_like_unique_index_duplicate(exc):
            raise
        logger.warning(
            "PTG2 manifest serving unique index found duplicate rows after direct publish; "
            "running DB dedupe rescue",
            exc_info=True,
        )
        dedupe_metrics["db_dedupe"] = True
        dedupe_metrics["rescue"] = True
        dedupe_metrics["serving"] = await _dedupe_ptg2_manifest_serving_table(schema_name, final_table)
        serving_deduped = True
        await db.status(
            f"""
            CREATE UNIQUE INDEX {_quote_ident(unique_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
            (serving_content_hash_128);
            """
        )
    await db.status(
        f"""
        CREATE INDEX {_quote_ident(lookup_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        (plan_id, reported_code_system, reported_code, provider_count DESC NULLS LAST, serving_content_hash_128);
        """
    )
    # The unique index is a publish-time correctness guard. Once it builds, the
    # immutable snapshot has proven there are no duplicate manifest serving identities;
    # retaining the 128-bit-only btree adds several GB and is not on the API hot path.
    await db.status(f"DROP INDEX {_quote_ident(schema_name)}.{_quote_ident(unique_index)};")
    if await _table_exists(schema_name, price_atom_table):
        price_atom_deduped = False
        if use_db_dedupe:
            dedupe_metrics["price_atom"] = await _dedupe_ptg2_manifest_price_atom_table(
                schema_name,
                price_atom_table,
            )
            price_atom_deduped = True
        price_atom_index = _ptg2_snapshot_index_name(price_atom_table, "primary")
        try:
            await db.status(
                f"""
                CREATE UNIQUE INDEX {_quote_ident(price_atom_index)}
                ON {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
                (price_atom_global_id_128);
                """
            )
        except Exception as exc:
            if price_atom_deduped or not _looks_like_unique_index_duplicate(exc):
                raise
            logger.warning(
                "PTG2 manifest price atom unique index found duplicate rows after direct publish; "
                "running DB dedupe rescue",
                exc_info=True,
            )
            dedupe_metrics["db_dedupe"] = True
            dedupe_metrics["rescue"] = True
            dedupe_metrics["price_atom"] = await _dedupe_ptg2_manifest_price_atom_table(
                schema_name,
                price_atom_table,
            )
            price_atom_deduped = True
            await db.status(
                f"""
                CREATE UNIQUE INDEX {_quote_ident(price_atom_index)}
                ON {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
                (price_atom_global_id_128);
                """
            )
    if await _table_exists(schema_name, provider_group_member_table):
        if use_db_dedupe:
            dedupe_metrics["provider_group_member"] = await _dedupe_ptg2_manifest_provider_group_member_table(
                schema_name,
                provider_group_member_table,
            )
        provider_group_member_index = _ptg2_snapshot_index_name(provider_group_member_table, "group_npi_idx")
        provider_group_member_npi_index = _ptg2_snapshot_index_name(provider_group_member_table, "npi_idx")
        await db.status(
            f"""
            CREATE INDEX {_quote_ident(provider_group_member_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)}
            (provider_group_global_id_128, npi);
            """
        )
        await db.status(
            f"""
            CREATE INDEX {_quote_ident(provider_group_member_npi_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)}
            (npi, provider_group_global_id_128);
            """
        )
    materialized_provider_set_component_table = await _materialize_manifest_components(
        schema_name=schema_name,
        table_name=provider_set_component_table,
        artifacts=artifacts,
        sidecar_artifacts=sidecar_artifacts,
    )
    await db.status(
        f"""
        CREATE TABLE {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} AS
        SELECT
            plan_id,
            reported_code_system,
            reported_code,
            COUNT(*)::bigint AS rate_count
        FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        GROUP BY plan_id, reported_code_system, reported_code;
        """
    )
    code_count_index = _ptg2_snapshot_index_name(code_count_table, "primary")
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(code_count_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(code_count_table)}
        (plan_id, reported_code_system, reported_code);
        """
    )
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(final_table)};")
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(code_count_table)};")
    if await _table_exists(schema_name, price_atom_table):
        await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)};")
    if await _table_exists(schema_name, provider_group_member_table):
        await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)};")
    if materialized_provider_set_component_table:
        await db.status(
            f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(materialized_provider_set_component_table)};"
        )
    row_count = await _exact_table_rows(schema_name, final_table)
    elapsed_seconds = time.monotonic() - started_at
    artifact_manifest = _ptg2_manifest_artifacts_manifest(artifacts=artifacts, sidecar_artifacts=sidecar_artifacts)
    return {
        "storage": "manifest_snapshot",
        "type": "ptg2_serving",
        "id_storage": _ptg2_id_storage(),
        "snapshot_scoped": True,
        "source_key": source_key,
        "table": f"{schema_name}.{final_table}",
        "price_atom_table": f"{schema_name}.{price_atom_table}",
        "provider_group_member_table": f"{schema_name}.{provider_group_member_table}",
        "provider_set_component_table": (
            f"{schema_name}.{materialized_provider_set_component_table}"
            if materialized_provider_set_component_table
            else None
        ),
        "code_count_table": f"{schema_name}.{code_count_table}",
        "rate_count": row_count,
        "serving_rates": row_count,
        "row_count": row_count,
        "artifacts": artifact_manifest,
        "timings": {"publish_seconds": elapsed_seconds},
        "dedupe": dedupe_metrics,
    }


def _ptg2_manifest_artifacts_manifest(
    *,
    artifacts: Mapping[str, Any] | None = None,
    sidecar_artifacts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = dict(artifacts or {})
    if not sidecar_artifacts:
        return manifest
    existing_sidecars = manifest.get("sidecars")
    sidecars: list[Any] = list(existing_sidecars) if isinstance(existing_sidecars, list) else []
    for name, value in sidecar_artifacts.items():
        if value is None:
            continue
        if isinstance(value, Mapping):
            entry = dict(value)
            entry.setdefault("name", str(name))
            sidecars.append(entry)
        else:
            sidecars.append({"name": str(name), "path": str(value)})
    manifest["sidecars"] = sidecars
    return manifest
