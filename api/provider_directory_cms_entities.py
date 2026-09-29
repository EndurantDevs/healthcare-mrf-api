# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public allowlists over a sealed CMS retained-resource snapshot."""

import asyncio
import unicodedata
from datetime import date, timezone

from sqlalchemy import text

from api.provider_directory_cms_generation import SOURCE, accepted_cms_generation
from api.provider_directory_cms_payers import cms_payer_organization_rows, cms_payer_rows, payer_generation
from api.provider_directory_cms_queries import (
    KINDS,
    cms_entity_rows,
    cms_network_plan_rows,
    cms_relationship_rows,
    cms_target_identity,
)
from api.provider_directory_entities_contract import (
    DirectoryReadError,
    directory_cursor_key,
    issue_directory_cursor,
    opaque_directory_key,
    read_directory_cursor,
)
from api.provider_directory_medical_groups import _schema_name
from process.provider_directory_cms_serving_coverage import require_cms_coverage


def _evidence(key, generation, resource_type, resource_id):
    observed_at = generation["observed_at"]
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return {
        "source_id": SOURCE,
        "resource_type": resource_type,
        "record_key": opaque_directory_key(key, "src_", SOURCE, resource_type, resource_id),
        "release_id": opaque_directory_key(key, "release_", SOURCE, generation["release_id"]),
        "observed_at": observed_at.isoformat(),
    }


def _safe_name(name):
    if not isinstance(name, str):
        return None
    name = name.strip()
    if not name or len(name) > 2048 or any(unicodedata.category(char).startswith("C") for char in name):
        return None
    return name


def _effective_date(raw_date):
    try:
        return date.fromisoformat(raw_date).isoformat() if len(raw_date) == 10 else None
    except TypeError, ValueError:
        return None


def _entity(key, generation, kind, record):
    status = record["status"] if record["status"] in {"active", "inactive", "conflict"} else "unknown"
    if status != "conflict" and record["active"] in {"true", "false"}:
        status = "active" if record["active"] == "true" else "inactive"
    evidence_entries = [_evidence(key, generation, record["resource_type"], record["resource_id"])]
    if record["plan_evidence"]:
        evidence_entries.append(_evidence(key, generation, "InsurancePlan", record["plan_evidence"]))
    return {
        "id": str(record["entity_id"]),
        "kind": kind,
        "source_id": SOURCE,
        "display_name": _safe_name(record["name"]),
        "status": status,
        "effective_start": _effective_date(record["period_start"]),
        "effective_end": _effective_date(record["period_end"]),
        "evidence": evidence_entries,
    }


async def _relationships(session, schema, key, query, generation, parent):
    position = read_directory_cursor(key, query, generation["generation_id"])
    reader = {"networks": cms_network_plan_rows, "payers": cms_payer_organization_rows}.get(
        query.kind, cms_relationship_rows
    )
    relationship_rows = await reader(session, schema, query, generation, parent["resource_id"], position)
    relationship_items = []
    for relation_record in relationship_rows[: query.limit]:
        target_id, status = await cms_target_identity(
            session,
            schema,
            generation,
            relation_record["target_kind"],
            relation_record["reference"],
        )
        evidence_type = "InsurancePlan" if query.kind == "networks" else parent["resource_type"]
        evidence_id = relation_record.get("evidence_id", parent["resource_id"])
        relationship_items.append(
            {
                "relationship_key": opaque_directory_key(
                    key,
                    "rel_",
                    SOURCE,
                    query.kind,
                    parent["resource_id"],
                    generation["release_id"],
                    str(relation_record["position"]),
                ),
                "relationship_type": relation_record["relationship_type"],
                "source_id": SOURCE,
                "target_kind": relation_record["target_kind"],
                "target_id": target_id,
                "status": status,
                "effective_start": _effective_date(parent["period_start"]),
                "effective_end": _effective_date(parent["period_end"]),
                "evidence": [_evidence(key, generation, evidence_type, evidence_id)],
            }
        )
    relationship_items.sort(key=lambda relation_item: relation_item["relationship_key"])
    next_position = relationship_rows[query.limit - 1]["position"] if len(relationship_rows) > query.limit else None
    if next_position is not None and query.kind in {"networks", "payers"}:
        next_position = str(next_position)
    return _page(key, query, generation, relationship_items, next_position)


def _page(key, query, generation, items, next_position):
    generation_id = generation["generation_id"]
    cursor = issue_directory_cursor(key, query, generation_id, next_position) if next_position is not None else None
    return {"generation_id": generation_id, "items": items, "next_cursor": cursor}


async def _read_snapshot(session, schema, key, query):
    generation = await accepted_cms_generation(session, key)
    if query.kind == "payers":
        generation = await payer_generation(session, schema, key, generation)
    if query.generation_id is not None and query.generation_id != generation["generation_id"]:
        raise DirectoryReadError(409)
    await require_cms_coverage(session, schema, generation)
    position = read_directory_cursor(key, query, generation["generation_id"]) if query.shape == "entities" else None
    reader = cms_payer_rows if query.kind == "payers" else cms_entity_rows
    rows = await reader(session, schema, query, generation, position)
    if query.shape != "entities" and not rows:
        raise DirectoryReadError(404)
    if query.shape == "relationships":
        return await _relationships(session, schema, key, query, generation, rows[0])
    items = [_entity(key, generation, query.kind, record) for record in rows[: query.limit]]
    if query.shape == "entity":
        return {"generation_id": generation["generation_id"], "item": items[0]}
    next_position = items[-1]["id"] if len(rows) > query.limit else None
    return _page(key, query, generation, items, next_position)


async def read_cms_entities(session, query):
    """Serve one bounded read-only snapshot; missing dependency or authority fails closed."""
    if query.source_id != SOURCE or query.kind not in {*KINDS, "payers"} or session.in_transaction():
        raise DirectoryReadError(503)
    key = directory_cursor_key()
    schema = _schema_name()
    try:
        async with asyncio.timeout(2), session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '2s'"))
            await session.execute(text("SET LOCAL lock_timeout = '250ms'"))
            await session.execute(text("SET LOCAL work_mem = '4MB'"))
            return await _read_snapshot(session, schema, key, query)
    except TimeoutError:
        raise DirectoryReadError(503) from None
