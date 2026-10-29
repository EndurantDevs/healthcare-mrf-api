# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cache identity policy for sealed legacy receipts and native NPI revisions."""

import re

from sqlalchemy import text

from api.provider_profile_snapshot import provider_read_savepoint
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
        except Exception as exc:
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
