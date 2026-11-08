# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cache identity policy for sealed legacy receipts and native NPI revisions."""

import re

from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api.provider_list_sql import _NPI_COUNT_READ_SCOPE
from api.provider_profile_snapshot import provider_read_savepoint
from process.custom_import.read_core import _is_statement_timeout
from process.npi_result_generation import RELATION_NAMES, validate_npi_result_generation_authority


async def npi_native_cache_identity(execute, schema, *, session=None):
    """Require one native revision and the exact six current heaps before caching."""
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("NPI cache schema is invalid")
    current_oids = ",".join(f"to_regclass(:{name}_ref)::oid::bigint" for name in RELATION_NAMES)
    async with provider_read_savepoint(session):
        result = await execute(
            text(
                f'SELECT revision.* FROM "{schema}".npi_result_generation AS revision '
                f"WHERE singleton IS TRUE AND relation_oids=ARRAY[{current_oids}]::bigint[]"
            ),
            session=session,
            params={f"{name}_ref": f'"{schema}"."{name}"' for name in RELATION_NAMES},
        )
        rows = result.mappings().all()
    if len(rows) != 1:
        return None
    authority = validate_npi_result_generation_authority(rows[0])
    if authority.serving_generation is None or authority.canonical_provenance is None:
        return None
    return f"native:{authority.local_lineage_id}:{authority.local_generation}:{authority.canonical_provenance.publication_ref}"


async def npi_publication_cache_identity(identity_rows, execute, schema, logger, *, session=None):
    """Prefer one valid legacy receipt and bypass caching when native lookup fails."""
    if not identity_rows:
        try:
            return await npi_native_cache_identity(execute, schema, session=session)
        except ServiceUnavailable, TimeoutError:
            raise
        except Exception as exc:
            if isinstance(exc, DBAPIError) and _is_statement_timeout(exc):
                raise
            logger.debug("Native NPI revision identity unavailable; bypassing cache (%s)", type(exc).__name__)
            return None
    if len(identity_rows) != 1:
        return None
    publication_generation, publication_ref = identity_rows[0]
    if (
        type(publication_generation) is not int
        or publication_generation < 1
        or type(publication_ref) is not str
        or len(publication_ref) != 50
        or not publication_ref.startswith("nppub1_")
    ):
        return None
    return f"{publication_generation}:{publication_ref}"


async def npi_canonical_publication_identity(execute, schema, logger, *, receipt_table, seal_table, session=None):
    """Read the sealed legacy receipt and fall back to the exact native revision."""
    live_table_by_oid_column = (
        ("npi_table_oid", "npi"),
        ("npi_address_table_oid", "npi_address"),
        ("npi_taxonomy_table_oid", "npi_taxonomy"),
        ("npi_taxonomy_group_table_oid", "npi_taxonomy_group"),
        ("npi_other_identifier_table_oid", "npi_other_identifier"),
        ("npi_phone_staffing_table_oid", "npi_phone_staffing"),
    )
    relation_predicates = " AND ".join(
        f"receipt.{oid_column}=to_regclass(:{table_name}_ref)::oid"
        for oid_column, table_name in live_table_by_oid_column
    )
    parameters_by_name = {f"{table_name}_ref": f"{schema}.{table_name}" for _, table_name in live_table_by_oid_column}
    try:
        identity_result = await execute(
            text(
                f"SELECT receipt.publication_generation, receipt.publication_ref "
                f"FROM {receipt_table} AS receipt "
                f"JOIN {seal_table} AS sealed USING (publication_ref) "
                f"WHERE {relation_predicates} "
                "ORDER BY receipt.publication_generation DESC LIMIT 2"
            ),
            session=session,
            params=parameters_by_name,
        )
        identity_rows = identity_result.all()
    except ServiceUnavailable:
        raise
    except Exception as exc:
        if _NPI_COUNT_READ_SCOPE.get() is not None:
            raise
        logger.debug(
            "Canonical NPI publication identity unavailable; bypassing cache (%s)",
            type(exc).__name__,
        )
        return None
    return await npi_publication_cache_identity(identity_rows, execute, schema, logger, session=session)
