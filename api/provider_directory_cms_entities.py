# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public allowlists over a sealed CMS retained-resource snapshot."""

import asyncio
import unicodedata
from dataclasses import replace
from datetime import date, timezone

from sqlalchemy import text

from api.provider_directory_cms_control_scope import scoped_cms_control_generation
from api.provider_directory_cms_generation import SOURCE, accepted_cms_generation
from api.provider_directory_cms_network_scope import pin_cms_network_scope
from api.provider_directory_cms_payers import cms_payer_organization_rows, cms_payer_rows, payer_generation
from api.provider_directory_cms_queries import (
    KINDS,
    cms_entity_rows,
    cms_network_plan_rows,
    cms_practitioner_npi,
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
    if observed_at is not None and observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return {
        "source_id": SOURCE,
        "resource_type": resource_type,
        "record_key": opaque_directory_key(key, "src_", SOURCE, resource_type, resource_id),
        "release_id": opaque_directory_key(key, "release_", SOURCE, generation["release_id"]),
        "observed_at": observed_at.isoformat() if observed_at is not None else None,
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
        source_status = relation_record.get("resolution_status")
        if source_status in {"unresolved", "ambiguous"}:
            target_id, status = None, "conflict" if source_status == "ambiguous" else "unresolved"
        elif relation_record["target_kind"] == "providers":
            target_id = (
                await cms_practitioner_npi(session, schema, generation, relation_record["reference"])
                if source_status == "resolved"
                else None
            )
            status = "resolved" if target_id else "unresolved"
        else:
            target_id, status = await cms_target_identity(
                session,
                schema,
                generation,
                relation_record["target_kind"],
                relation_record["reference"],
            )
        evidence_type = "InsurancePlan" if query.kind == "networks" else parent["resource_type"]
        evidence_id = relation_record.get("evidence_id", parent["resource_id"])
        relationship_map = {
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
            "effective_start": _effective_date(relation_record.get("period_start", parent["period_start"])),
            "effective_end": _effective_date(relation_record.get("period_end", parent["period_end"])),
            "evidence": [_evidence(key, generation, evidence_type, evidence_id)],
        }
        if relation_record["target_kind"] == "providers" and target_id is not None:
            relationship_map["provider_npi"] = target_id
            relationship_map["provider_profile_path"] = f"/api/v1/providers/{target_id}/profile"
        relationship_items.append(relationship_map)
    relationship_items.sort(key=lambda relation_item: relation_item["relationship_key"])
    next_position = relationship_rows[query.limit - 1]["position"] if len(relationship_rows) > query.limit else None
    if next_position is not None and query.kind in {"networks", "payers"}:
        next_position = str(next_position)
    return _page(key, query, generation, relationship_items, next_position)


def _page(key, query, generation, items, next_position):
    generation_id = generation["generation_id"]
    cursor = issue_directory_cursor(key, query, generation_id, next_position) if next_position is not None else None
    return {"generation_id": generation_id, "items": items, "next_cursor": cursor}


async def _scoped_generation(session, schema, key, query, generation, response_headers):
    """Bind the source token and cursor to the exact canonical edition and access policy."""
    scope = await pin_cms_network_scope(session, schema, query, generation)
    query = replace(query, network_generation=scope.manifest.generation_id)
    generation = scope.source_generation
    pair = generation.get("retained_pair")
    source_token = generation.get("generation_id") or opaque_directory_key(
        key,
        "gen_",
        scope.source_pin.coordinates,
        str(generation["observed_at"]),
        pair.cms_receipt_payload if pair is not None else None,
        pair.proof.publication_xid if pair is not None else None,
    )
    generation_by_field = {
        **generation,
        **scope.parameters_by_name,
        "network_scope": scope,
        "control_schema": schema,
        "scope_kind": query.kind,
        "scope_shape": query.shape,
        "generation_id": opaque_directory_key(
            key,
            "gen_",
            source_token,
            scope.manifest.generation_id,
            scope.manifest.manifest_sha256,
            query.network_ids,
            query.access_scope_sha256,
        ),
    }
    if response_headers is not None:
        response_headers["X-Network-Generation"] = str(scope.manifest.generation_id)
        if generation.get("retained_pair") is not None:
            response_headers["X-CMS-Retained-Publication"] = "committed/v1"
    return query, generation_by_field


async def _read_snapshot(session, schema, key, query, response_headers=None):
    """Pin scoped source and control evidence before paging or disclosing redirects."""
    query, schema, generation_map = await _read_generation(session, schema, key, query, response_headers)
    if query.generation_id is not None and query.generation_id != generation_map["generation_id"]:
        raise DirectoryReadError(409)
    if generation_map.get("retained_pair") is None:
        await require_cms_coverage(session, schema, generation_map)
    if query.shape != "entities" and query.kind in {"organizations", "sites"}:
        resource_type = KINDS[query.kind]
        canonical_id = await session.scalar(
            text(f"""SELECT canonical_entity_id FROM {generation_map.get("control_schema", schema)}.provider_directory_entity_redirect
                WHERE source_id=:source_id AND resource_type=:resource_type
                  AND old_entity_id=CAST(:entity_id AS uuid)"""),
            {"source_id": SOURCE, "resource_type": resource_type, "entity_id": query.entity_id},
        )
        if canonical_id is not None:
            if query.shape == "relationships":
                read_directory_cursor(key, query, generation_map["generation_id"])
            canonical_query = replace(query, entity_id=str(canonical_id))
            if canonical_query.entity_id == query.entity_id or not await cms_entity_rows(
                session, schema, canonical_query, generation_map
            ):
                raise DirectoryReadError(503)
            return {
                "generation_id": generation_map["generation_id"],
                "redirect": {
                    "source_id": SOURCE,
                    "kind": query.kind,
                    "requested_id": query.entity_id,
                    "canonical_id": canonical_query.entity_id,
                },
            }
    position = read_directory_cursor(key, query, generation_map["generation_id"]) if query.shape == "entities" else None
    reader = cms_payer_rows if query.kind == "payers" else cms_entity_rows
    directory_rows = await reader(session, schema, query, generation_map, position)
    if query.shape != "entities" and not directory_rows:
        raise DirectoryReadError(404)
    if query.shape == "relationships":
        return await _relationships(session, schema, key, query, generation_map, directory_rows[0])
    directory_items = [
        _entity(key, generation_map, query.kind, directory_record) for directory_record in directory_rows[: query.limit]
    ]
    if query.shape == "entity":
        return {"generation_id": generation_map["generation_id"], "item": directory_items[0]}
    next_position = directory_items[-1]["id"] if len(directory_rows) > query.limit else None
    return _page(key, query, generation_map, directory_items, next_position)


async def read_cms_entities(session, query, *, response_headers=None):
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
            if query.is_network_scoped:
                await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
            return await _read_snapshot(session, schema, key, query, response_headers)
    except TimeoutError:
        raise DirectoryReadError(503) from None


async def read_cms_entities_snapshot(session, query, *, response_headers=None):
    """Read inside the caller's verified Reader transaction without opening another."""
    if query.source_id != SOURCE or query.kind not in {*KINDS, "payers"} or not session.in_transaction():
        raise DirectoryReadError(503)
    if query.is_network_scoped:
        await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
    return await _read_snapshot(session, _schema_name(), directory_cursor_key(), query, response_headers)


async def _read_generation(session, schema, key, query, response_headers):
    """Resolve a scoped physical source without consulting the current edition."""
    generation = None if query.is_network_scoped else await accepted_cms_generation(session, key)
    if query.is_network_scoped:
        query, generation = await _scoped_generation(session, schema, key, query, generation, response_headers)
        control_schema = schema
        schema = generation["network_scope"].source_schema
        generation_map = await scoped_cms_control_generation(session, control_schema, key, generation)
    else:
        review_count = await session.scalar(
            text(
                f"SELECT count(*) FROM {schema}.provider_directory_entity_redirect_decision WHERE source_id=:source_id"
            ),
            {"source_id": SOURCE},
        )
        generation_map = {
            **generation,
            "generation_id": opaque_directory_key(
                key, "gen_", generation["generation_id"], "entity-redirects", review_count
            ),
        }
        if query.kind == "payers":
            generation_map = await payer_generation(session, schema, key, generation_map)
    return query, schema, generation_map
