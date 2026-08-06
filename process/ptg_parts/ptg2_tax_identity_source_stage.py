# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Load an authenticated source-local projection into an indexed temp stage."""

from __future__ import annotations

import os
import uuid
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_artifact import (
    _COPY_COLUMNS,
    _is_copy_file_unchanged,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection,
    TaxIdentitySourceProjectionError,
    _fail,
)


async def _copy_prepared_projection(
    session: Any,
    prepared: PreparedTaxIdentitySourceProjection,
    *,
    stage_table: str,
) -> None:
    open_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    open_flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(prepared.copy_path, open_flags)
    with os.fdopen(descriptor, "rb", closefd=True) as copy_file:
        if not _is_copy_file_unchanged(copy_file, prepared):
            raise _fail()
        copy_file.seek(0)
        connection = await session.connection()
        raw_connection = await connection.get_raw_connection()
        driver_connection = getattr(raw_connection, "driver_connection", raw_connection)
        copy_to_table = getattr(driver_connection, "copy_to_table", None)
        if copy_to_table is None:
            raise _fail()
        await copy_to_table(
            stage_table,
            source=copy_file,
            schema_name="pg_temp",
            columns=list(_COPY_COLUMNS),
            format="binary",
        )
        if not _is_copy_file_unchanged(copy_file, prepared):
            raise _fail()


async def stage_tax_identity_source_projection(
    session: Any,
    prepared: PreparedTaxIdentitySourceProjection,
) -> str:
    """COPY authenticated observations into one indexed backend-local stage."""

    if not isinstance(prepared, PreparedTaxIdentitySourceProjection):
        raise _fail()
    stage_table = f"ptg2_tax_source_stage_{uuid.uuid4().hex[:20]}"
    quoted_stage = f'{_quote_ident("pg_temp")}.{_quote_ident(stage_table)}'
    try:
        await session.execute(db.text(f"""
            CREATE TEMP TABLE {quoted_stage} (
                source_key integer NOT NULL,
                source_ordinal integer NOT NULL,
                source_record_ordinal bigint NOT NULL,
                provider_group_global_id_128 bytea NOT NULL,
                tax_identity_state text NOT NULL,
                tin_id_128 bytea,
                tin_hmac_sha256 bytea
            ) ON COMMIT DROP
            """))
        await _copy_prepared_projection(session, prepared, stage_table=stage_table)
        await session.execute(db.text(f"""
            CREATE UNIQUE INDEX {_quote_ident(stage_table + "_source_order_uq")}
                ON {quoted_stage} (source_key, source_record_ordinal)
            """))
        await session.execute(db.text(f"""
            CREATE INDEX {_quote_ident(stage_table + "_group_idx")}
                ON {quoted_stage} (provider_group_global_id_128)
            """))
        await session.execute(db.text(f"ANALYZE {quoted_stage}"))
        observed_count = await session.scalar(
            db.text(f"SELECT COUNT(*)::bigint FROM {quoted_stage}")
        )
        if int(observed_count or 0) != prepared.provider_group_occurrence_count:
            raise _fail()
        return stage_table
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


__all__ = ["stage_tax_identity_source_projection"]
