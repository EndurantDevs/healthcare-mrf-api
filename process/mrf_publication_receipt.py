# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed completion authority for the multi-transaction MRF publisher.

A pending claim has no lease or automatic takeover. After a crash, an operator
must prove the old finalizer stopped before removing that exact attempt's row
and rerunning a complete import. Removing a row never makes export admissible.
"""

from __future__ import annotations

import json
import re
from uuid import uuid4

from sqlalchemy import text

from db.connection import db
from process.mrf_address_publication import capture_address_content, require_address_coverage
from process.reference_family_result_generation import (
    current_reference_family_relation_oids,
    read_reference_family_result_generation_authority,
)

TABLE = "mrf_publication_receipt"
SUMMARY_INPUTS = ("plan", "plan_attributes", "plan_benefits", "plan_prices")


def qualified(schema: str, name: str) -> str:
    """Accept only native identifiers, never caller SQL."""
    if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value) for value in (schema, name)):
        raise ValueError("invalid MRF receipt identifier")
    return f'"{schema}"."{name}"'


async def begin_publication(schema: str, import_id: str) -> str:
    """Commit exclusive durable ownership before any live publication effect."""
    if db._transaction_binding() is not None:
        raise RuntimeError("MRF pending claim requires an independent durable transaction")
    attempt = str(uuid4())
    table = qualified(schema, TABLE)
    async with db.transaction() as session:
        claimed = (await session.execute(text(f"""
            INSERT INTO {table} AS incumbent (singleton, attempt_id, import_id, state)
            VALUES (TRUE, CAST(:attempt AS uuid), :import_id, 'pending')
            ON CONFLICT (singleton) DO UPDATE SET
                attempt_id=EXCLUDED.attempt_id, import_id=EXCLUDED.import_id,
                state='pending', started_at=clock_timestamp(), completed_at=NULL,
                generation=NULL, summary_oid=NULL, summary_inputs=NULL,
                address_resolution_performed=NULL, address_content=NULL
            WHERE incumbent.state='complete'
            RETURNING attempt_id
        """), {"attempt": attempt, "import_id": str(import_id)})).scalar_one_or_none()
        if claimed is None:
            raise RuntimeError("MRF publication is pending; reconcile its finalizer before retry")
    return attempt


async def capture_summary_inputs(session, schema: str) -> dict[str, int]:
    """Capture the actual build inputs while blocking replacement and writes."""
    names = [qualified(schema, name) for name in SUMMARY_INPUTS]
    await session.execute(text(f"LOCK TABLE {', '.join(names)} IN SHARE MODE"))
    input_oid_by_table = {}
    for name in SUMMARY_INPUTS:
        input_oid_by_table[name] = int((await session.execute(
            text("SELECT to_regclass(:name)::oid"), {"name": qualified(schema, name)}
        )).scalar_one())
    return input_oid_by_table


async def complete_publication(
    session, schema: str, attempt: str, generation, inputs, address_resolution_performed: bool,
) -> None:
    """Complete only in the summary rotation/index transaction, after rechecking inputs."""
    # Execution history is separate from the independently verified source content.
    if type(address_resolution_performed) is not bool:
        raise ValueError("MRF address resolution execution must be an explicit boolean")
    observed_inputs = await capture_summary_inputs(session, schema)
    if observed_inputs != inputs:
        raise RuntimeError("MRF summary inputs changed during publication")
    current = await read_reference_family_result_generation_authority(
        session, importer_id="mrf", schema_name=schema, lock=True,
    )
    oids = await current_reference_family_relation_oids(session, importer_id="mrf", schema_name=schema)
    if current != generation or current.relation_oids != oids:
        raise RuntimeError("MRF publication generation changed")
    address_content = await capture_address_content(session, schema, qualified)
    summary_oid = (await session.execute(
        text("SELECT to_regclass(:name)::oid"), {"name": qualified(schema, "plan_search_summary")}
    )).scalar_one()
    completed = (await session.execute(text(f"""
        UPDATE {qualified(schema, TABLE)} SET state='complete', completed_at=clock_timestamp(),
            generation=CAST(:generation AS jsonb), summary_oid=:summary_oid,
            summary_inputs=CAST(:inputs AS jsonb),
            address_resolution_performed=:address_resolution_performed,
            address_content=CAST(:address_content AS jsonb)
        WHERE singleton AND attempt_id=CAST(:attempt AS uuid) AND state='pending'
        RETURNING attempt_id
    """), {"attempt": attempt, "generation": json.dumps(generation.as_dict()),
           "summary_oid": summary_oid, "inputs": json.dumps(inputs),
           "address_resolution_performed": address_resolution_performed,
           "address_content": json.dumps(address_content)})).scalar_one_or_none()
    if completed is None:
        raise RuntimeError("MRF publication claim changed")


async def require_completed_publication(session, schema: str) -> dict:
    """Admit a captured MRF family only after its ordinary finalizer completed.

The caller must hold the family locks in its capture snapshot. Source-local
address coverage does not transfer shared canonical archive rows to a destination.
"""
    if not session.in_transaction():
        raise RuntimeError("MRF completion admission requires a capture transaction")
    table = qualified(schema, TABLE)
    if not (await session.execute(text("SELECT to_regclass(:name)"), {"name": table})).scalar_one():
        raise RuntimeError("MRF publication completion is unavailable")
    publication_receipt = (await session.execute(text(f"SELECT * FROM {table} WHERE singleton"))).mappings().one_or_none()
    if (
        publication_receipt is None
        or publication_receipt["contract_version"] != 1
        or publication_receipt["state"] != "complete"
    ):
        raise RuntimeError("MRF publication completion is unavailable")
    current = await read_reference_family_result_generation_authority(
        session, importer_id="mrf", schema_name=schema,
    )
    oids = await current_reference_family_relation_oids(session, importer_id="mrf", schema_name=schema)
    if publication_receipt["generation"] != current.as_dict() or current.relation_oids != oids:
        raise RuntimeError("MRF publication completion generation differs")
    inputs = await capture_summary_inputs(session, schema)
    # Use the finalizer's input-before-summary lock order; never lock its generation row.
    summary = qualified(schema, "plan_search_summary")
    await session.execute(text(f"LOCK TABLE {summary} IN SHARE MODE"))
    summary_oid = (await session.execute(text("SELECT to_regclass(:name)::oid"), {"name": summary})).scalar_one()
    if publication_receipt["summary_inputs"] != inputs or publication_receipt["summary_oid"] != summary_oid:
        raise RuntimeError("MRF publication completion summary identity differs")
    address_content = await capture_address_content(session, schema, qualified)
    require_address_coverage(address_content)
    if publication_receipt["address_content"] != address_content:
        raise RuntimeError("MRF publication address content differs")
    return dict(publication_receipt)
