# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded company choices from the current immutable approval map."""

from process.registry_approval_store import RegistryApprovalConflict
from process.registry_source_observation_store import _namespace

MAX_APPROVED_COMPANY_PAGE_SIZE = 40


async def read_registry_approved_companies(
    connection, *, limit=MAX_APPROVED_COMPANY_PAGE_SIZE, offset=0, approved_revision=None, schema=None
):
    """Caller owns one repeatable-read transaction; draft heads never enter this read."""
    if (
        type(limit) is not int
        or not 1 <= limit <= MAX_APPROVED_COMPANY_PAGE_SIZE
        or type(offset) is not int
        or not 0 <= offset <= 1000000
        or offset > 0
        and approved_revision is None
        or approved_revision is not None
        and (type(approved_revision) is not int or not 0 <= approved_revision <= 2**53 - 1)
    ):
        raise ValueError("registry_page_invalid")
    namespace = _namespace(schema)
    current = await connection.fetchval(
        f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
    )
    if type(current) is not int or not 0 <= current <= 2**53 - 1:
        raise RuntimeError("registry_approved_company_unavailable")
    if approved_revision is not None and current != approved_revision:
        raise RegistryApprovalConflict("registry_approved_company_revision_conflict")
    company_rows = await connection.fetch(
        f"""SELECT record_key AS company_id,record_json->>'display_name' AS display_name
        FROM {namespace}.registry_approved_record
        WHERE approved_revision=$1 AND record_kind='company'
          AND record_json->>'company_id'=record_key AND record_json->'archived'='false'::jsonb
        ORDER BY record_key LIMIT $2 OFFSET $3""",
        current,
        limit + 1,
        offset,
    )
    return {
        "approved_revision": current,
        "items": [dict(company_row) for company_row in company_rows[:limit]],
        "limit": limit,
        "offset": offset,
        "has_more": len(company_rows) > limit,
    }
