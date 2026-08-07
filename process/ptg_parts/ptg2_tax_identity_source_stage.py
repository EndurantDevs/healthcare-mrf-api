# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Load an authenticated source-local projection into an indexed temp stage."""

from __future__ import annotations

from dataclasses import dataclass
import uuid
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_artifact import _COPY_COLUMNS
from process.ptg_parts.ptg2_tax_identity_source_copy import (
    _authenticated_projection_copy_stream,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection,
    TaxIdentitySourceProjectionError,
    _fail,
)

_STAGE_GUARD_FUNCTION = "ptg2_tax_source_stage_guard"


@dataclass(frozen=True, slots=True, repr=False)
class StagedTaxIdentitySourceProjection:
    """Backend-local stage and immutable seal coordinates."""

    table_name: str
    seal_table_name: str
    stage_oid: int
    seal_oid: int
    seal_token: str

    def __repr__(self) -> str:
        return "<staged-tax-identity-source-projection evidence=<redacted>>"


async def _copy_prepared_projection(
    session: Any,
    prepared: PreparedTaxIdentitySourceProjection,
    *,
    stage_table: str,
) -> None:
    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    driver_connection = getattr(
        raw_connection,
        "driver_connection",
        raw_connection,
    )
    copy_to_table = getattr(driver_connection, "copy_to_table", None)
    if copy_to_table is None:
        raise _fail()
    with _authenticated_projection_copy_stream(prepared) as copy_stream:
        await copy_to_table(
            stage_table,
            source=copy_stream,
            schema_name="pg_temp",
            columns=list(_COPY_COLUMNS),
            format="binary",
        )


async def _temp_relation_oid(session: Any, relation_name: str) -> int:
    relation_oid = await session.scalar(
        db.text("""
            SELECT relation.oid::bigint
              FROM pg_class AS relation
             WHERE relation.relnamespace = pg_my_temp_schema()
               AND relation.relname = :relation_name
               AND relation.relkind = 'r'
               AND relation.relpersistence = 't'
            """),
        {"relation_name": relation_name},
    )
    normalized_oid = int(relation_oid or 0)
    if normalized_oid <= 0:
        raise _fail()
    return normalized_oid


async def _create_stage_seal_table(session: Any, *, quoted_seal: str) -> None:
    await session.execute(db.text(f"""
        CREATE TEMP TABLE {quoted_seal} (
            seal_token varchar(32) PRIMARY KEY
                CHECK (seal_token ~ '^[0-9a-f]{{32}}$'),
            stage_oid oid NOT NULL,
            seal_oid oid NOT NULL,
            stage_table text NOT NULL,
            copy_sha256 varchar(64) NOT NULL
                CHECK (copy_sha256 ~ '^[0-9a-f]{{64}}$'),
            copy_byte_count bigint NOT NULL CHECK (copy_byte_count > 0),
            content_digest bytea NOT NULL
                CHECK (octet_length(content_digest) = 32),
            source_ordinal_map_digest bytea NOT NULL
                CHECK (octet_length(source_ordinal_map_digest) = 32),
            binding_vector_digest bytea NOT NULL
                CHECK (octet_length(binding_vector_digest) = 32),
            aggregate_tax_content_digest bytea NOT NULL
                CHECK (octet_length(aggregate_tax_content_digest) = 32),
            source_count integer NOT NULL CHECK (source_count > 0),
            provider_group_occurrence_count bigint NOT NULL
                CHECK (provider_group_occurrence_count >= 0),
            provider_group_count bigint NOT NULL
                CHECK (provider_group_count >= 0)
        ) ON COMMIT DROP
        """))


async def _insert_stage_seal(
    session: Any,
    *,
    quoted_seal: str,
    seal_parameters_by_name: dict[str, object],
) -> None:
    await session.execute(
        db.text(f"""
            INSERT INTO {quoted_seal}
                (seal_token, stage_oid, seal_oid, stage_table, copy_sha256,
                 copy_byte_count, content_digest, source_ordinal_map_digest,
                 binding_vector_digest, aggregate_tax_content_digest, source_count,
                 provider_group_occurrence_count, provider_group_count)
            VALUES
                (:seal_token, :stage_oid, :seal_oid, :stage_table,
                 :copy_sha256, :copy_byte_count,
                 :content_digest, :source_ordinal_map_digest,
                 :binding_vector_digest, :aggregate_tax_content_digest,
                 :source_count,
                 :provider_group_occurrence_count, :provider_group_count)
            """),
        seal_parameters_by_name,
    )


def _stage_seal_parameters(
    prepared: PreparedTaxIdentitySourceProjection,
    *,
    stage_table: str,
    stage_oid: int,
    seal_oid: int,
    seal_token: str,
    provider_group_count: int,
) -> dict[str, object]:
    return {
        "seal_token": seal_token,
        "stage_oid": stage_oid,
        "seal_oid": seal_oid,
        "stage_table": stage_table,
        "copy_sha256": prepared.copy_sha256,
        "copy_byte_count": prepared.copy_byte_count,
        "content_digest": prepared.content_digest,
        "source_ordinal_map_digest": prepared.source_ordinal_map_digest,
        "binding_vector_digest": prepared.binding_vector_digest,
        "aggregate_tax_content_digest": prepared.aggregate_tax_content_digest,
        "source_count": prepared.source_count,
        "provider_group_occurrence_count": (prepared.provider_group_occurrence_count),
        "provider_group_count": provider_group_count,
    }


async def _seal_staged_projection(
    session: Any,
    prepared: PreparedTaxIdentitySourceProjection,
    *,
    stage_table: str,
    seal_table: str,
    seal_token: str,
    provider_group_count: int,
) -> StagedTaxIdentitySourceProjection:
    """Bind the authenticated COPY and aggregate coordinates into one seal."""

    stage_oid = await _temp_relation_oid(session, stage_table)
    quoted_seal = f'{_quote_ident("pg_temp")}.{_quote_ident(seal_table)}'
    await _create_stage_seal_table(session, quoted_seal=quoted_seal)
    seal_oid = await _temp_relation_oid(session, seal_table)
    if seal_oid == stage_oid:
        raise _fail()
    await _insert_stage_seal(
        session,
        quoted_seal=quoted_seal,
        seal_parameters_by_name=_stage_seal_parameters(
            prepared,
            stage_table=stage_table,
            stage_oid=stage_oid,
            seal_oid=seal_oid,
            seal_token=seal_token,
            provider_group_count=provider_group_count,
        ),
    )
    return StagedTaxIdentitySourceProjection(
        table_name=stage_table,
        seal_table_name=seal_table,
        stage_oid=stage_oid,
        seal_oid=seal_oid,
        seal_token=seal_token,
    )


async def _freeze_staged_projection(
    session: Any,
    *,
    relation_names: tuple[str, str],
) -> None:
    quoted_function = f'{_quote_ident("pg_temp")}.{_quote_ident(_STAGE_GUARD_FUNCTION)}'
    await session.execute(db.text(f"""
        CREATE OR REPLACE FUNCTION {quoted_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $guard$
        BEGIN
            RAISE EXCEPTION 'PTG2_TAX_SOURCE_STAGE_SEALED'
                USING ERRCODE = 'P0001';
        END
        $guard$
        """))
    for relation_name in relation_names:
        quoted_relation = f'{_quote_ident("pg_temp")}.{_quote_ident(relation_name)}'
        trigger_name = _quote_ident(f"{relation_name}_guard")
        await session.execute(db.text(f"""
            CREATE TRIGGER {trigger_name}
            BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {quoted_relation}
            FOR EACH STATEMENT EXECUTE FUNCTION {quoted_function}()
            """))
        await session.execute(db.text(f"""
            ALTER TABLE {quoted_relation}
            ENABLE ALWAYS TRIGGER {trigger_name}
            """))


async def _drop_staged_tax_identity_source_projection(
    session: Any,
    staged: StagedTaxIdentitySourceProjection,
) -> None:
    """Remove a successfully consumed stage and its backend-local guard."""

    quoted_stage = f'{_quote_ident("pg_temp")}.{_quote_ident(staged.table_name)}'
    quoted_seal = f'{_quote_ident("pg_temp")}.{_quote_ident(staged.seal_table_name)}'
    quoted_function = (
        f'{_quote_ident("pg_temp")}.' f"{_quote_ident(_STAGE_GUARD_FUNCTION)}"
    )
    await session.execute(db.text(f"DROP TABLE {quoted_stage}"))
    await session.execute(db.text(f"DROP TABLE {quoted_seal}"))
    remaining_trigger_count = await session.scalar(db.text("""
        SELECT COUNT(*)::bigint
          FROM pg_trigger
         WHERE tgfoid = to_regprocedure(
                   'pg_temp.ptg2_tax_source_stage_guard()'
               )
           AND NOT tgisinternal
        """))
    if int(remaining_trigger_count or 0) == 0:
        await session.execute(db.text(f"DROP FUNCTION IF EXISTS {quoted_function}()"))


async def stage_tax_identity_source_projection(
    session: Any,
    prepared: PreparedTaxIdentitySourceProjection,
) -> StagedTaxIdentitySourceProjection:
    """COPY authenticated observations into one indexed backend-local stage."""

    if not isinstance(prepared, PreparedTaxIdentitySourceProjection):
        raise _fail()
    seal_token = uuid.uuid4().hex
    stage_table = f"ptg2_tax_source_stage_{seal_token[:20]}"
    seal_table = f"{stage_table}_seal"
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
        observed_count, provider_group_count = (await session.execute(db.text(f"""
                SELECT COUNT(*)::bigint,
                       COUNT(DISTINCT provider_group_global_id_128)::bigint
                  FROM {quoted_stage}
                """))).one()
        if int(observed_count or 0) != prepared.provider_group_occurrence_count:
            raise _fail()
        staged = await _seal_staged_projection(
            session,
            prepared,
            stage_table=stage_table,
            seal_table=seal_table,
            seal_token=seal_token,
            provider_group_count=int(provider_group_count or 0),
        )
        await _freeze_staged_projection(
            session,
            relation_names=(stage_table, seal_table),
        )
        return staged
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


__all__ = [
    "StagedTaxIdentitySourceProjection",
    "stage_tax_identity_source_projection",
]
